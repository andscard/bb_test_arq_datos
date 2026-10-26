"""
Silver — Estado actual limpio y tipado de cada entidad

- Interpreta el JSON de Debezium (carga inicial op='r' y cambios) con el esquema de negocio.
- Aplica reglas de calidad: críticas descartan, de negocio solo alertan,
  y las filas inválidas se conservan en una tabla de cuarentena.
- APPLY CHANGES materializa el estado vigente:
    * SCD Tipo 1 para transacciones y catálogos.
    * SCD Tipo 2 para comercios (historia de MCC y segmento → análisis "as-of").
- Orden de aplicación: offset del evento en Event Hubs. Debezium publica todos los eventos
  de una misma clave en la misma partición y en orden real (su incremental snapshot descarta
  lecturas superadas por cambios del binlog), así que el offset es la secuencia correcta.
  Requisito operativo: no cambiar el número de particiones ni recrear los event hubs.
  src_ts_ms viaja en la secuencia solo como dato (fecha de inicio de versión en SCD2).

Nota: las columnas de las tablas bco_t_* son ilustrativas.
"""
import dlt
from pyspark.sql import functions as F

CATALOG = "adquirencia"

TABLAS = {
    "bco_t_trx_maestra": {
        "silver": "transaccion",
        "keys": ["trx_id"],
        "scd": 1,
        "columnas": {
            "trx_id": "BIGINT", "fecha_trx": "TIMESTAMP", "comercio_id": "BIGINT",
            "canal_id": "INT", "sucursal_id": "INT", "monto": "DECIMAL(18,2)",
            "moneda": "STRING", "codigo_respuesta": "STRING", "codigo_autorizacion": "STRING",
            "bin": "STRING", "ultimos4": "STRING", "token_tarjeta": "STRING",
        },
        "criticas": {
            "trx_id_presente": "trx_id IS NOT NULL",
            "fecha_valida": "fecha_trx IS NOT NULL",
        },
        "negocio": {
            "monto_no_negativo": "monto >= 0",
            "comercio_presente": "comercio_id IS NOT NULL",
            "moneda_valida": "moneda IN ('USD')",
        },
    },
    "bco_t_trx_detalle": {
        "silver": "transaccion_detalle",
        "keys": ["detalle_id"],
        "scd": 1,
        "columnas": {
            "detalle_id": "BIGINT", "trx_id": "BIGINT", "cuotas": "INT",
            "marca": "STRING", "tipo_tarjeta": "STRING", "tipo_pos": "STRING",
            "comision": "DECIMAL(18,4)",
        },
        "criticas": {"detalle_id_presente": "detalle_id IS NOT NULL", "trx_id_presente": "trx_id IS NOT NULL"},
        "negocio": {"cuotas_validas": "cuotas BETWEEN 1 AND 48", "comision_no_negativa": "comision >= 0"},
    },
    "bco_t_comercio_360": {
        "silver": "comercio",
        "keys": ["comercio_id"],
        "scd": 2,
        "historia": ["mcc", "categoria_mcc", "segmento", "oficial_id"],
        "columnas": {
            "comercio_id": "BIGINT", "ruc": "STRING", "razon_social": "STRING",
            "nombre_comercial": "STRING", "mcc": "STRING", "categoria_mcc": "STRING",
            "segmento": "STRING", "oficial_id": "STRING",
        },
        "criticas": {"comercio_id_presente": "comercio_id IS NOT NULL"},
        "negocio": {"mcc_formato": "mcc RLIKE '^[0-9]{4}$'"},
    },
    "bco_t_canal_digital": {
        "silver": "canal",
        "keys": ["canal_id"],
        "scd": 1,
        "columnas": {"canal_id": "INT", "canal": "STRING", "tipo_canal": "STRING"},
        "criticas": {"canal_id_presente": "canal_id IS NOT NULL"},
        "negocio": {},
    },
    "bco_t_sucursal_geo": {
        "silver": "sucursal",
        "keys": ["sucursal_id"],
        "scd": 1,
        "columnas": {
            "sucursal_id": "INT", "nombre_sucursal": "STRING", "provincia": "STRING",
            "ciudad": "STRING", "region": "STRING", "red_adquirente": "STRING",
        },
        "criticas": {"sucursal_id_presente": "sucursal_id IS NOT NULL"},
        "negocio": {},
    },
    "bco_t_estado_workflow": {
        "silver": "estado_workflow",
        "keys": ["workflow_id"],
        "scd": 1,
        "columnas": {
            "workflow_id": "BIGINT", "trx_id": "BIGINT", "tipo_proceso": "STRING",
            "estado": "STRING", "fecha_estado": "TIMESTAMP",
        },
        "criticas": {"workflow_id_presente": "workflow_id IS NOT NULL", "trx_id_presente": "trx_id IS NOT NULL"},
        "negocio": {"proceso_valido": "tipo_proceso IN ('COMPENSACION', 'LIQUIDACION', 'CONTRACARGO')"},
    },
}

METADATOS_CDC = ["op", "src_ts_ms", "binlog_file", "binlog_pos", "_offset"]


def _tipar(columna: str, tipo: str):
    """Convierte el valor (texto) al tipo de negocio.
    Debezium envía DATETIME como epoch-ms (time.precision.mode=connect); se acepta también
    texto ISO por robustez ante re-sincronizaciones o cargas manuales."""
    if tipo == "TIMESTAMP":
        return F.expr(
            f"coalesce(timestamp_millis(try_cast(d.{columna} AS BIGINT)), "
            f"try_cast(d.{columna} AS TIMESTAMP))"
        ).alias(columna)
    return F.expr(f"try_cast(d.{columna} AS {tipo})").alias(columna)


def _eventos_tipados(tabla: str, spec: dict):
    esquema = ", ".join(f"{c} STRING" for c in spec["columnas"])
    return (
        spark.readStream.table(f"{CATALOG}.bronze.{tabla}")
        # En un DELETE 'after' es nulo: la clave se toma de 'before'.
        .withColumn("d", F.from_json(F.coalesce("after", "before"), esquema))
        .select(*[_tipar(c, t) for c, t in spec["columnas"].items()], *METADATOS_CDC)
    )


def crear_silver(tabla: str, spec: dict) -> None:
    nombre = spec["silver"]
    vista = f"v_{nombre}_validado"
    todas = {**spec["criticas"], **spec["negocio"]}

    @dlt.view(name=vista)
    @dlt.expect_all_or_drop(spec["criticas"])   # sin clave no se puede aplicar el cambio
    @dlt.expect_all(spec["negocio"])            # se registran en métricas de calidad
    def validado():
        return _eventos_tipados(tabla, spec)

    if todas:
        @dlt.table(
            name=f"{CATALOG}.silver.cuarentena_{nombre}",
            comment=f"Eventos de {tabla} que incumplen reglas de calidad; revisión del Data Steward.",
            table_properties={"quality": "cuarentena"},
        )
        def cuarentena():
            cumple_todo = " AND ".join(f"coalesce(({r}), false)" for r in todas.values())
            return (
                _eventos_tipados(tabla, spec)
                .where(f"NOT ({cumple_todo})")
                .withColumn("_detectado_ts", F.current_timestamp())
            )

    destino = f"{CATALOG}.silver.{nombre}"
    dlt.create_streaming_table(
        name=destino,
        comment=f"Estado vigente de {tabla} (SCD tipo {spec['scd']}). Capa Silver.",
        table_properties={"quality": "silver"},
    )
    dlt.apply_changes(
        target=destino,
        source=vista,
        keys=spec["keys"],
        sequence_by=F.struct("_offset", "src_ts_ms"),
        apply_as_deletes=F.expr("op = 'd'"),
        except_column_list=METADATOS_CDC,
        stored_as_scd_type=spec["scd"],
        track_history_column_list=spec.get("historia"),
    )


for t, s in TABLAS.items():
    crear_silver(t, s)
