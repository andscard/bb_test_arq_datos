# ADR-001: CDC por binlog con Debezium sobre réplica dedicada

**Estado:** Aceptado

## Contexto

La información de RDS MySQL debe llegar a Azure para analítica sin afectar la autorización de pagos. Cualquier mecanismo que ejecute consultas sobre las tablas de producción reproduce el problema actual. El negocio requiere datos con latencia de minutos para monitoreo operativo.

## Alternativas evaluadas

| Opción                                                   | A favor                                                                                                                                                          | En contra                                                                                                                               |
| -------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| Extracciones batch (`SELECT` por fecha de actualización) | Simple                                                                                                                                                           | Carga directa sobre MySQL; no captura borrados; latencia de horas                                                                       |
| AWS DMS con CDC                                          | Servicio administrado                                                                                                                                            | Destinos nativos orientados a AWS (S3/Kinesis); sumar un salto adicional para llegar a Azure; menor control sobre el formato del evento |
| Federación (Databricks consulta MySQL en vivo)           | Sin pipeline                                                                                                                                                     | Vuelve a cargar la BD transaccional; descartada                                                                                         |
| **Debezium (Kafka Connect) → Event Hubs (Kafka)**        | Lee binlog sin consultar tablas; eventos con antes/después y metadatos de posición; Event Hubs expone protocolo Kafka nativo en Azure; código abierto y portable | Operar Kafka Connect (contenedores en ECS)                                                                                              |

## Origen del binlog: ¿de qué instancia lee Debezium?

| Opción                                  | A favor                                        | En contra                                                                            |
| --------------------------------------- | ---------------------------------------------- | ------------------------------------------------------------------------------------ |
| Directo de la principal                 | Sin instancia extra; mínima latencia           | Comparte conexiones y red con la autorización (impacto bajo, pero no nulo)           |
| De la Réplica A (la de los SPs, Fase 0) | Sin instancia extra                            | Los SPs la saturan y el _replica lag_ retrasa el CDC; además se elimina en la Fase 2 |
| **De una Réplica B dedicada**           | Aislamiento total de la principal y de los SPs | Costo de una instancia; segundos adicionales de latencia                             |

## Decisión

Usar **Debezium MySQL Connector** ejecutándose en Kafka Connect sobre **ECS Fargate** en AWS, leyendo el binlog de la **Réplica B**, una réplica de lectura de RDS dedicada solo al CDC, y publicando en **Azure Event Hubs** vía su endpoint Kafka.

- La Réplica B se crea en la Fase 1 con backups automáticos activados (en RDS es requisito para que una réplica escriba binlog) y `log_replica_updates` habilitado, para que registre en su propio binlog los cambios que recibe de la principal. Ver [`src/aws/01_replicas_rds.sh`](../../src/aws/01_replicas_rds.sh).
- `binlog_format=ROW`, `binlog_row_image=FULL`, retención de binlog de 48 h.
- **Carga inicial con _incremental snapshot_ de Debezium** sobre la Réplica B: lee cada tabla por bloques de 10.000 filas mientras sigue capturando cambios, sin bloqueos sobre los datos y sin escribir en la base (`read.only=true`, requiere GTID).
- El PAN y los datos sensibles de autenticación se excluyen en el conector (`column.exclude.list`), tanto en la carga inicial como en el flujo continuo. Ver [ADR-005](ADR-005-tratamiento-del-pan.md).

## Carga inicial: alternativas evaluadas

| Opción                                                          | A favor                                                                                                                             | En contra                                                                                                                                                                         |
| --------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Snapshot completo inicial de Debezium (`snapshot.mode=initial`) | Simple                                                                                                                              | Una lectura larga por tabla; si falla a la mitad, empieza de cero                                                                                                                 |
| Exportar un snapshot de RDS a S3 (Parquet) y copiarlo a Azure   | No consulta ninguna base                                                                                                            | La exportación no permite excluir columnas: el PAN saldría de AWS salvo un paso adicional de limpieza (AWS Glue); más piezas y marcas de tiempo manuales para empalmar con el CDC |
| **Incremental snapshot de Debezium**                            | Un solo mecanismo para histórico y cambios; respeta la exclusión del PAN; reanudable por bloques; Debezium resuelve el solapamiento | 83M eventos por Event Hubs (horas de carga y más capacidad temporal); requiere GTID                                                                                               |

Con 83M registros el costo de pasar el histórico por Event Hubs es de horas, y la Réplica B dedicada absorbe las lecturas por bloques sin afectar la principal ni los SPs. La exportación a S3 sería preferible con volúmenes de miles de millones.

## Consecuencias

- Impacto prácticamente nulo sobre la principal; ni siquiera el CDC lee de ella.
- Captura inserts, updates y deletes con orden por clave; reproceso desde _offsets_.
- La misma señal de _incremental snapshot_ permite re-sincronizar una tabla en el futuro sin detener el flujo.
- Latencia de segundos hasta Event Hubs.
- Se debe operar y monitorear Kafka Connect (lag, reinicios); mitigado con alarmas y despliegue como código.
- El _replica lag_ se suma a la latencia total; aceptable (segundos) para el caso de uso.
- Activar GTID en una base en uso se hace por etapas y en ventanas de mantenimiento.
- Una instancia RDS adicional y permanente (costo); se dimensiona pequeña porque solo sirve binlog.
