"""
Bronze — Eventos CDC crudos

Una tabla Bronze por tabla de origen, alimentada por Debezium desde Azure Event Hubs
Por el mismo tópico llegan:
  - Carga inicial (incremental snapshot): eventos op = 'r'
  - Cambios continuos del binlog: op = 'c' | 'u' | 'd'

Defensa en profundidad (ADR-005): si un evento contiene una columna de datos de tarjeta
prohibida, el pipeline se DETIENE en lugar de guardarla.
"""
import dlt
from pyspark.sql import functions as F

CATALOG = "adquirencia"
SECRET_SCOPE = "kv-adquirencia"                         # respaldado por Azure Key Vault
EH_NAMESPACE = spark.conf.get("adq.eventhubs_namespace")

TABLAS_ORIGEN = [
    "bco_t_trx_maestra",
    "bco_t_trx_detalle",
    "bco_t_comercio_360",
    "bco_t_canal_digital",
    "bco_t_sucursal_geo",
    "bco_t_estado_workflow",
]

# Columnas que nunca deben llegar a Azure (excluidas en el conector Debezium).
COLUMNAS_PROHIBIDAS = ["pan", "track_data", "cvv"]
_patron = "|".join(COLUMNAS_PROHIBIDAS)
REGLA_SIN_DATOS_TARJETA = (
    f"NOT (coalesce(after, '') RLIKE '\"({_patron})\"\\\\s*:' "
    f"OR coalesce(before, '') RLIKE '\"({_patron})\"\\\\s*:')"
)

_eh_conn = dbutils.secrets.get(SECRET_SCOPE, "eventhubs-listen-connection-string")
KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{_eh_conn}";'
    ),
    "startingOffsets": "earliest",
    "failOnDataLoss": "true",
    "maxOffsetsPerTrigger": "200000",
}


def crear_bronze(tabla: str) -> None:
    @dlt.table(
        name=f"{CATALOG}.bronze.{tabla}",
        comment=f"Eventos CDC crudos de {tabla} (Debezium): carga inicial (op='r') y cambios. Capa Bronze.",
        table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
        cluster_by=["_fecha_ingesta"],
    )
    @dlt.expect_or_fail("sin_datos_de_tarjeta", REGLA_SIN_DATOS_TARJETA)
    def bronze():
        # Debezium serializa before/after como objetos JSON; se guardan como texto
        # para no fijar el esquema de negocio en Bronze.
        crudo = (
            spark.readStream.format("kafka")
            .options(**KAFKA_OPTIONS)
            .option("subscribe", f"adq.adquirencia.{tabla}")
            .load()
        )
        valor = F.col("value").cast("string")
        return crudo.select(
            F.get_json_object(valor, "$.op").alias("op"),
            F.get_json_object(valor, "$.before").alias("before"),
            F.get_json_object(valor, "$.after").alias("after"),
            F.get_json_object(valor, "$.source.ts_ms").cast("long").alias("src_ts_ms"),
            F.get_json_object(valor, "$.source.file").alias("binlog_file"),
            F.get_json_object(valor, "$.source.pos").cast("long").alias("binlog_pos"),
            F.col("topic").alias("_topic"),
            F.col("partition").alias("_particion"),
            F.col("offset").alias("_offset"),
            F.current_timestamp().alias("_ingesta_ts"),
            F.current_date().alias("_fecha_ingesta"),
        )


for t in TABLAS_ORIGEN:
    crear_bronze(t)
