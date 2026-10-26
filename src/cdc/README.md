# CDC: RDS MySQL → Azure Event Hubs con Debezium

El conector corre en Kafka Connect sobre ECS Fargate (AWS), lee el binlog de la Réplica B (réplica de lectura de RDS dedicada al CDC) y publica en Azure Event Hubs usando su endpoint compatible con Kafka (`<namespace>.servicebus.windows.net:9093`, SASL*SSL/PLAIN). Un \_event hub* por tabla: `adq.adquirencia.<tabla>`.

El mismo conector hace la carga inicial y el flujo continuo, no hay exportaciones de archivos ni copias entre nubes.

## Decisiones de configuración

| Propiedad                         | Valor                                  | Motivo                                                                                                                                |
| --------------------------------- | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `database.hostname`               | Endpoint de la Réplica B               | La principal no recibe ni siquiera la lectura del binlog, y los SPs de la Réplica A no retrasan el CDC                                |
| `column.exclude.list`             | PAN y datos sensibles de autenticación | PCI DSS: el dato de titular no sale de AWS, tampoco en la carga inicial. Ver [ADR-005](../../docs/adr/ADR-005-tratamiento-del-pan.md) |
| `snapshot.mode`                   | `no_data`                              | Al arrancar solo registra la estructura de las tablas; los datos históricos llegan por _incremental snapshot_                         |
| `read.only`                       | `true`                                 | El _incremental snapshot_ no escribe nada en la base (requiere GTID activo)                                                           |
| `incremental.snapshot.chunk.size` | `10000`                                | Tamaño de cada bloque leído; bloques pequeños = consultas cortas sobre la Réplica B                                                   |
| `signal.enabled.channels`         | `kafka`                                | La carga se ordena con un mensaje en el event hub `adq-cdc-signals`                                                                   |
| `decimal.handling.mode`           | `string`                               | Montos sin pérdida de precisión (se castean a `DECIMAL` en Silver)                                                                    |
| `time.precision.mode`             | `connect`                              | `DATETIME` llega como epoch en milisegundos, sencillo de convertir                                                                    |
| Secretos                          | `FileConfigProvider`                   | Credenciales montadas desde AWS Secrets Manager; nunca en el JSON                                                                     |

## Carga inicial con _incremental snapshot_

El CDC solo ve los cambios que ocurren desde que arranca. Los 83 millones de registros existentes se traen con un **incremental snapshot**: Debezium lee cada tabla por bloques ordenados por clave primaria mientras sigue capturando cambios del binlog, y emite cada fila leída como un evento `op = 'r'` (de _read_).

### Pasos

1. **Requisitos:** GTID activo en la principal y en ambas réplicas ([`src/aws/01_replicas_rds.sh`](../aws/01_replicas_rds.sh)), usuario `svc_debezium` con sus permisos ([`src/mysql`](../mysql)) y el event hub `adq-cdc-signals` creado.
2. **Registrar el conector** ([`debezium-mysql-connector.json`](debezium-mysql-connector.json)). Empieza a emitir cambios nuevos de inmediato.
3. **Iniciar el pipeline** de Databricks en modo continuo.
4. **Enviar la señal** ([`senal-carga-inicial.json`](senal-carga-inicial.json)) al event hub `adq-cdc-signals`, con clave `adq`. Las tablas pequeñas van primero para que las dimensiones estén listas cuando lleguen las transacciones.
5. **Seguir el avance** con las métricas del conector. Si el conector se reinicia, el snapshot **continúa desde el último bloque**, no desde cero.
6. **Validar:** filas `op = 'r'` en Bronze por tabla frente al conteo de la tabla en la Réplica B, y cuadre diario (`gold.control_cuadre_diario`) frente a la Réplica A.

**Duración estimada:** con eventos de ~1 KB, 83 millones de filas son ~80 GB. Con 10 unidades de capacidad de Event Hubs (~10 MB/s de entrada) la carga toma del orden de 2 a 3 horas. Las unidades se suben durante la carga y se bajan al terminar.

## Monitoreo

- Métricas JMX del conector → CloudWatch: `MilliSecondsBehindSource` (retraso), `NumberOfCommittedTransactions` y métricas del snapshot (tabla y bloque en curso).
- _Consumer lag_ por event hub en Azure Monitor.
- Alarma si el retraso supera 5 minutos o si la retención de binlog (48 h) está en riesgo.
- Regla de calidad en Bronze que detiene el pipeline si aparece una columna prohibida (ver [ADR-005](../../docs/adr/ADR-005-tratamiento-del-pan.md)).
