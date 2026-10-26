# Documento de Arquitectura

Respuesta a los puntos A–E de la prueba. Las decisiones con alternativas evaluadas están en [`adr/`](adr/).

---

## A. Desacoplamiento operacional y rendimiento

### A.1 Causas raíz

1. **Mezcla de cargas OLTP y OLAP.** El motor que autoriza pagos (escrituras cortas, latencia de milisegundos) también ejecuta consultas analíticas (lecturas masivas de segundos o minutos). Ambas compiten por CPU, I/O, conexiones y el _buffer pool_ de InnoDB; las lecturas analíticas desalojan de memoria las páginas calientes de la autorización.
2. **Modelo normalizado consultado como si fuera analítico.** Cada consulta reconstruye la transacción uniendo 6 tablas (`maestra`, `detalle`, `comercio_360`, `canal_digital`, `sucursal_geo`, `estado_workflow`). `estado_workflow` es de tipo historial (varias filas por transacción), lo que obliga a subconsultas para obtener el estado vigente.
3. **Filtros dinámicos sin índice posible.** Con 11 filtros opcionales hay más de 2.000 combinaciones. Un índice B-tree solo sirve si se usa su prefijo izquierdo; las demás combinaciones terminan en escaneos de rango amplios o completos.
4. **Efectos de concurrencia en InnoDB.** Lecturas largas mantienen _read views_ abiertas (crece el _history list length_ y el trabajo de _purge_), y operaciones como `INSERT … SELECT` o `CREATE TEMPORARY TABLE … SELECT` dentro de SPs toman _locks_ compartidos sobre filas leídas, bloqueando a las escrituras de autorización.
5. **Contrato síncrono con límite de tiempo.** El front llama a API Gateway, que espera al SP. Toda consulta que excede el tiempo máximo de integración devuelve 504, pero el SP sigue ejecutándose en MySQL: el usuario reintenta y la carga se multiplica.
6. **Sin estrategia de volumen.** No hay particionamiento, réplicas, caché ni separación entre datos recientes e históricos; el costo de cada consulta crece con cada día de operación.

### A.2 Estrategia de protección del transaccional

| Horizonte                   | Acción                                                                            | Mecanismo                                                                                                                                                                          |
| --------------------------- | --------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Inmediato** (Fase 0)      | Redirigir SPs analíticos a la **Réplica A** (réplica de lectura de RDS, temporal) | Los SPs cambian de endpoint; la principal queda exclusiva para autorización ([`src/aws/01_replicas_rds.sh`](../src/aws/01_replicas_rds.sh))                                        |
|                             | Limitar consultas descontroladas                                                  | Usuario de BD dedicado con `MAX_USER_CONNECTIONS` + _watchdog_ que termina sesiones largas (`mysql.rds_kill`); `max_execution_time` para SELECT directos (no aplica dentro de SPs) |
|                             | Índices compuestos para las 3–4 combinaciones de filtro más usadas                | Basados en `performance_schema` / slow query log                                                                                                                                   |
| **Estructural** (Fases 1–2) | Sacar la analítica de MySQL                                                       | CDC → Lakehouse en Azure Databricks (patrón **CQRS**)                                                                                                                              |
|                             | Apagar los SPs analíticos                                                         | Reemplazados por la API de consultas sobre Gold                                                                                                                                    |

La Réplica A es una **contención**, no la solución: sigue siendo un motor OLTP y sufre _replica lag_ bajo carga analítica. Se elimina en la Fase 2, cuando la API reemplaza a los SPs. Ver [`src/mysql/01_contencion_inmediata.sql`](../src/mysql/01_contencion_inmediata.sql).

### A.3 Alta concurrencia y tiempos de respuesta consistentes

- **Databricks SQL Serverless Warehouse** con escalado automático de clústeres: más usuarios concurrentes ⇒ más clústeres, sin degradar a los demás.
- **Caché de resultados** del warehouse + caché en disco: consultas repetidas responden en milisegundos.
- **Datos preparados** (tabla de consulta operativa ya unida y con estado vigente) en lugar de _joins_ en tiempo de consulta.
- **Contrato de API protegido:** rango de fechas obligatorio y acotado, paginación, timeout por sentencia y límite de filas.
- **Exportaciones pesadas asíncronas:** la API responde `202 Accepted` con un `job_id`; el resultado se genera en segundo plano y se descarga cuando está listo. Ninguna consulta larga queda colgada de un timeout de gateway.

---

## B. Integración y arquitectura de datos

### B.1 Flujo de integración

```
RDS MySQL (principal)
   ├─ replicación nativa ─► Réplica A (analítica, temporal): SPs actuales hasta la Fase 2
   └─ replicación nativa ─► Réplica B (CDC, permanente): binlog propio (ROW, log_replica_updates, backups activos)
                               └─ Debezium MySQL Connector (Kafka Connect en ECS Fargate, AWS)
                                    └─ TLS sobre VPN ─► Azure Event Hubs (endpoint Kafka, 1 tópico por tabla)
                                                          └─ Lakeflow Declarative Pipeline (streaming)
                                                               └─ Bronze ─► Silver ─► Gold (Delta Lake en ADLS Gen2)
```

**Por qué dos réplicas:** la replicación nativa de RDS mantiene ambas sincronizadas con la principal a un costo mínimo para ella (ya escribe binlog por sus backups). Si Debezium leyera de la Réplica A, la carga de los SPs la retrasaría y con ella al CDC. La alternativa de conectar Debezium directo a la principal también es válida (leer el binlog es liviano); se prefirió la Réplica B por máximo aislamiento en un sistema de pagos, a cambio de una instancia adicional y segundos de latencia. Ver [ADR-001](adr/ADR-001-cdc-debezium-binlog.md).

**Carga inicial (83M filas) sin tocar producción:** el CDC solo ve los cambios desde que arranca, así que el histórico se trae con un **_incremental snapshot_** del mismo conector. Debezium lee cada tabla de la Réplica B **por bloques** de 10.000 filas, ordenados por clave primaria, mientras sigue capturando cambios del binlog. Antes y después de cada bloque marca su posición en el flujo (dos "marcas de agua"); si una fila cambió mientras se leía su bloque, descarta la lectura y conserva el cambio, que es más reciente. Así, en el tópico, los eventos de cada clave quedan en el orden real y Silver los aplica por _offset_.

Ventajas frente a exportar un snapshot de RDS a S3: un solo mecanismo para histórico y cambios, el PAN queda excluido también en la carga inicial ([ADR-005](adr/ADR-005-tratamiento-del-pan.md)), la carga se reanuda desde el último bloque si el conector se reinicia, y la misma señal sirve para re-sincronizar una tabla en el futuro. Costo: unas horas de carga y capacidad temporal adicional en Event Hubs. Detalle en [`src/cdc`](../src/cdc).

### B.2 Patrones elegidos

| Necesidad                  | Patrón                                           | Por qué                                                               |
| -------------------------- | ------------------------------------------------ | --------------------------------------------------------------------- |
| Sincronización continua    | **Change Data Capture basado en log**            | Lee el binlog, no las tablas: impacto casi nulo en la BD              |
| Distribución entre nubes   | **Event streaming (pub/sub)**                    | Desacopla productor y consumidor; permite reproceso desde el _offset_ |
| Lectura vs escritura       | **CQRS**                                         | Modelo de lectura optimizado y separado del transaccional             |
| Organización del lake      | **Medallion (Bronze/Silver/Gold)**               | Trazabilidad del dato crudo, calidad progresiva, reproceso            |
| Estado actual de entidades | **Upsert por clave + SCD Tipo 2** en dimensiones | Historia de cambios de comercio (MCC, segmento) para análisis "as-of" |

### B.3 Justificación por atributo de calidad

- **Escalabilidad:** Event Hubs escala por unidades de capacidad y particiones; el volumen actual (≈5 eventos/s promedio por tabla principal) usa una fracción mínima. Databricks escala cómputo independiente del almacenamiento.
- **Resiliencia:** Debezium guarda _offsets_ del binlog; ante caída, reanuda sin pérdida. Event Hubs retiene eventos (reproceso). Delta Lake es transaccional (ACID) y versionado (_time travel_). Retención de binlog en RDS configurada (p. ej. 48 h) para absorber caídas del conector.
- **Latencia:** extremo a extremo de **1–3 minutos** (binlog → Gold), suficiente para monitoreo operativo y consultas de oficiales. La autorización mantiene su latencia porque no participa del flujo.
- **Costos:** solo viajan los **cambios** (no copias completas), lo que reduce el _egress_ de AWS. Cómputo serverless con auto-detención; _Predictive Optimization_ para mantenimiento de tablas.

Alternativas descartadas (DMS, extracciones batch, federación de consultas) en [ADR-001](adr/ADR-001-cdc-debezium-binlog.md).

---

## C. Modelado y optimización para analítica

### C.1 Estrategia de modelado (capa Gold)

Dos estructuras, cada una para un patrón de consulta:

1. **Modelo estrella (Kimball)** para BI y KPIs:
    - `fact_transaccion` — grano: una transacción. Métricas: monto, comisión, cuotas.
    - `dim_comercio` (SCD2: MCC, segmento, oficial), `dim_tarjeta` (marca, tipo), `dim_canal`, `dim_sucursal`, `dim_fecha`. Los estados de liquidación y contracargo viajan como atributos del hecho (baja cardinalidad).
2. **Tabla ancha de consulta operativa** `trx_consulta_operativa` para el front-end: una fila por transacción con los atributos de las 6 tablas ya unidos y el **estado vigente** resuelto. Reemplaza directamente a los SPs: los 11 filtros se aplican sobre una sola tabla, sin _joins_.

Detalle en [ADR-003](adr/ADR-003-modelado-gold-liquid-clustering.md) y código en [`src/databricks`](../src/databricks).

### C.2 Datos recientes vs histórico

| Capa de temperatura | Rango                                | Tratamiento                                                                                                              |
| ------------------- | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| **Caliente**        | Últimos 90 días                      | Consultas operativas; tablas agregadas diarias (materialized views) para KPIs; caché del warehouse                       |
| **Tibio**           | 90 días – 2 años                     | Misma tabla Gold; el _clustering_ por fecha hace que las consultas recientes no lean archivos antiguos (_data skipping_) |
| **Frío**            | > 2 años / requerimiento regulatorio | Bronze crudo movido a _Cool/Archive_ de ADLS por políticas de ciclo de vida; agregados mensuales para tendencias         |

No se duplican tablas por temperatura: el **data skipping** de Delta sobre una tabla bien organizada resuelve el acceso por rango sin complejidad adicional.

### C.3 Mecanismos de rendimiento

- **Liquid Clustering** en `trx_consulta_operativa` por `(fecha_trx, comercio_id, estado_liquidacion)`: las columnas más filtradas. Es incremental y se puede cambiar sin reescribir la tabla (a diferencia de particiones fijas o Z-ORDER).
- **Estadísticas por archivo** (min/max) para _data skipping_ en el resto de filtros.
- **Materialized views** de agregados (ventas por comercio/día, contracargos por marca) refrescadas por el pipeline.
- **Photon** (motor vectorizado) en el SQL Warehouse.
- **Predictive Optimization** para `OPTIMIZE`, `VACUUM` y `ANALYZE` automáticos.
- **Contrato de consulta**: rango de fechas obligatorio (máx. 93 días en la API), columnas proyectadas y paginación por _keyset_.

---

## D. Gobierno y gestión de datos

### D.1 Marco de referencia

Alineado a **DAMA-DMBOK 2**, cubriendo las áreas de: gobierno, metadatos, calidad, seguridad, arquitectura y ciclo de vida. Plataforma: **Unity Catalog** (gobierno técnico en Databricks) integrado con **Microsoft Purview** (catálogo corporativo y glosario de negocio, alineado con el ecosistema Azure de la organización).

### D.2 Calidad, trazabilidad, catalogación y entendimiento

| Necesidad                 | Cómo se garantiza                                                                                                                                                                                                                        |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Calidad**               | _Expectations_ en el pipeline (montos ≥ 0, claves no nulas, estados válidos); filas inválidas a cuarentena, no se descartan en silencio. **Cuadre diario** origen vs Gold (conteo y suma de montos por día y comercio), crítico en pagos |
| **Trazabilidad / linaje** | Linaje a nivel de **columna** automático en Unity Catalog desde Bronze hasta el dashboard; Bronze conserva metadatos de origen (archivo de binlog, posición, timestamp)                                                                  |
| **Catalogación**          | Unity Catalog: `catálogo.esquema.tabla` por dominio (`adquirencia.bronze/silver/gold`); escaneo en Purview para catálogo corporativo                                                                                                     |
| **Entendimiento**         | Glosario de negocio en Purview (qué es "transacción liquidada", "contracargo"); `COMMENT` en tablas y columnas; _tags_ de clasificación (`pci`, `pii`, `confidencial`)                                                                   |
| **Cambios de esquema**    | **Contrato de datos** con el equipo de la aplicación: cambios en `bco_t_*` se notifican y versionan; el pipeline alerta ante esquemas inesperados                                                                                        |
| **Ciclo de vida**         | Retención por capa, `VACUUM` y políticas de ADLS; auditoría vía _system tables_                                                                                                                                                          |

### D.3 Roles y responsabilidades (RACI resumido)

| Actividad                                | Data Owner (Negocio Adquirencia) | Data Steward | Data Custodian / Ing. de Datos | Seguridad / Cumplimiento | Arquitectura de Datos |
| ---------------------------------------- | :------------------------------: | :----------: | :----------------------------: | :----------------------: | :-------------------: |
| Definir significado y reglas de negocio  |              **A**               |      R       |               C                |            I             |           C           |
| Reglas de calidad y umbrales             |                A                 |    **R**     |               R                |            I             |           C           |
| Implementar pipelines y controles        |                I                 |      C       |            **R/A**             |            C             |           C           |
| Clasificación y acceso a datos sensibles |                A                 |      C       |               R                |          **R**           |           C           |
| Aprobación de nuevos productos de datos  |              **A**               |      R       |               C                |            C             |           R           |
| Estándares y patrones de arquitectura    |                I                 |      C       |               C                |            C             |        **R/A**        |

**Controles relevantes:** revisión trimestral de accesos, alertas sobre fallas de calidad, auditoría de consultas a datos sensibles, y comité de gobierno mensual con indicadores (% de tablas Gold documentadas, % de reglas de calidad cumplidas, incidentes).

---

## E. Explotación y consumo

### E.1 Capa de acceso por tipo de usuario

| Usuario                              | Herramienta                                                                        | Sobre qué datos               |
| ------------------------------------ | ---------------------------------------------------------------------------------- | ----------------------------- |
| Oficiales de operaciones (front-end) | **API de consultas** (Azure API Management → servicio de consulta → SQL Warehouse) | `gold.trx_consulta_operativa` |
| Analistas                            | Databricks SQL Editor, notebooks, Power BI                                         | Modelo estrella Gold          |
| Gerencia / negocio                   | Power BI y **AI/BI Dashboards**                                                    | Agregados y KPIs              |
| Usuario de negocio no técnico        | **Genie** (preguntas en lenguaje natural sobre un espacio curado)                  | Subconjunto Gold con glosario |

La API reemplaza a los SPs: construye SQL **parametrizado** a partir de una lista blanca de filtros (sin concatenar valores), exige rango de fechas y pagina. Ver [`src/api`](../src/api).

### E.2 Autoservicio, KPIs y monitoreo operacional

- **Autoservicio gobernado:** los usuarios consumen solo Gold certificado; Genie y dashboards se construyen sobre métricas definidas una vez (montos aprobados, ticket promedio, tasa de aprobación, tasa de contracargo, comisión por marca).
- **Monitoreo operacional:** pipeline en _streaming_ + **alertas de Databricks SQL** (p. ej. caída de la tasa de aprobación de un comercio > X% en 15 min, aumento de contracargos por MCC). Latencia de 1–3 min.
- **Monitoreo de la plataforma:** _lag_ del conector Debezium, _consumer lag_ en Event Hubs, frescura de cada tabla Gold y uso del warehouse (system tables).

### E.3 Seguridad y control de acceso

- **PCI DSS:** el PAN y los datos sensibles de autenticación se **excluyen en el conector** (no salen de AWS, tampoco en la carga inicial). Analítica trabaja con BIN, últimos 4 y token. Bronze detiene el pipeline si aparece una columna prohibida. Así Azure queda **fuera del alcance PCI**. Se evaluó cifrar el PAN con acceso por grupos y se descartó porque metería todo Azure en el alcance; si un área necesita el PAN completo, lo obtiene caso por caso con un servicio de detokenización dentro del CDE de AWS. Ver [ADR-005](adr/ADR-005-tratamiento-del-pan.md).
- **Identidad:** Microsoft Entra ID con grupos sincronizados (SCIM) a Databricks; acceso por grupo, nunca por usuario individual.
- **Mínimo privilegio:** `GRANT` por esquema/tabla en Unity Catalog; Bronze y Silver solo para ingeniería.
- **Seguridad a nivel de fila (RLS):** cada oficial ve solo sus comercios asignados. Se implementa con **vistas seguras** en el esquema `consumo` que filtran por `current_user()` contra una tabla de asignaciones mantenida por el Data Owner. Los usuarios no leen Gold directamente.
- **Enmascaramiento de columnas:** la identificación del comercio se enmascara con una función de Unity Catalog, salvo para el grupo autorizado.
- **Red:** VPN/interconexión privada AWS ↔ Azure; Event Hubs y ADLS con _private endpoints_; Databricks con VNet injection, sin IP pública y Private Link.
- **Secretos:** Azure Key Vault / AWS Secrets Manager; nada de credenciales en código.
- **Auditoría:** `system.access.audit` para saber quién consultó qué y cuándo.

Implementación en [`src/databricks/04_gobierno_seguridad.sql`](../src/databricks/04_gobierno_seguridad.sql).
