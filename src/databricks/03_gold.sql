-- =============================================================================
-- Gold — Modelo de consumo
--
--  1. trx_consulta_operativa : tabla para el front-end (reemplaza a los SPs)
--  2. Modelo estrella        : fact_transaccion + dimensiones, para BI
--  3. Agregados KPI          : métricas diarias precalculadas
--
-- Las materialized views se refrescan de forma incremental en cómputo serverless
-- cuando la consulta lo permite. Supuesto: relación 1:1 entre maestra y detalle.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Estado vigente por transacción (el workflow es un historial de N filas)
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW adquirencia.gold.trx_estado_vigente
COMMENT 'Último estado de compensación, liquidación y contracargo por transacción.'
AS
SELECT
    trx_id,
    MAX_BY(estado, fecha_estado) FILTER (WHERE tipo_proceso = 'COMPENSACION') AS estado_compensacion,
    MAX_BY(estado, fecha_estado) FILTER (WHERE tipo_proceso = 'LIQUIDACION')  AS estado_liquidacion,
    MAX(fecha_estado)            FILTER (WHERE tipo_proceso = 'LIQUIDACION')  AS fecha_liquidacion,
    COUNT(*)                     FILTER (WHERE tipo_proceso = 'CONTRACARGO') > 0 AS tiene_contracargo,
    MAX_BY(estado, fecha_estado) FILTER (WHERE tipo_proceso = 'CONTRACARGO')  AS estado_contracargo
FROM adquirencia.silver.estado_workflow
GROUP BY trx_id;

-- -----------------------------------------------------------------------------
-- 1. Tabla de consulta operativa
--    Liquid Clustering por las columnas más filtradas.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW adquirencia.gold.trx_consulta_operativa
CLUSTER BY (fecha_trx, comercio_id, estado_liquidacion)
COMMENT 'Una fila por transacción con atributos de comercio, tarjeta, canal, sucursal y estado vigente. Fuente única de la API de consultas operativas.'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
    t.trx_id,
    t.fecha_trx,
    CAST(t.fecha_trx AS DATE)          AS fecha,
    t.monto,
    t.moneda,
    t.codigo_respuesta,
    t.codigo_respuesta = '00'          AS aprobada,
    t.codigo_autorizacion,
    t.bin,
    t.ultimos4,
    -- comercio (versión vigente)
    t.comercio_id,
    c.nombre_comercial,
    c.ruc,
    c.mcc,
    c.categoria_mcc,
    c.segmento,
    c.oficial_id,
    -- tarjeta y POS
    d.marca,
    d.tipo_tarjeta,
    d.tipo_pos,
    d.cuotas,
    d.comision,
    -- canal y geografía
    t.canal_id,
    ca.canal,
    ca.tipo_canal,
    t.sucursal_id,
    s.nombre_sucursal,
    s.provincia,
    s.ciudad,
    -- estado vigente
    e.estado_compensacion,
    COALESCE(e.estado_liquidacion, 'PENDIENTE') AS estado_liquidacion,
    e.fecha_liquidacion,
    COALESCE(e.tiene_contracargo, false)        AS tiene_contracargo,
    e.estado_contracargo
FROM      adquirencia.silver.transaccion          t
LEFT JOIN adquirencia.silver.transaccion_detalle  d  ON d.trx_id = t.trx_id
LEFT JOIN adquirencia.silver.comercio             c  ON c.comercio_id = t.comercio_id
                                                    AND c.__END_AT IS NULL
LEFT JOIN adquirencia.silver.canal                ca ON ca.canal_id = t.canal_id
LEFT JOIN adquirencia.silver.sucursal             s  ON s.sucursal_id = t.sucursal_id
LEFT JOIN adquirencia.gold.trx_estado_vigente     e  ON e.trx_id = t.trx_id;

-- -----------------------------------------------------------------------------
-- 2. Modelo estrella
-- -----------------------------------------------------------------------------

-- Dimensión comercio SCD2: permite responder "¿en qué segmento estaba el comercio
-- cuando ocurrió la transacción?"
CREATE OR REFRESH MATERIALIZED VIEW adquirencia.gold.dim_comercio
COMMENT 'Comercios afiliados con historia de MCC, categoría, segmento y oficial (SCD tipo 2).'
AS
SELECT
    xxhash64(comercio_id, __START_AT.src_ts_ms)              AS comercio_sk,
    comercio_id, ruc, razon_social, nombre_comercial,
    mcc, categoria_mcc, segmento, oficial_id,
    -- la primera versión cubre todo el pasado (cargada desde el snapshot inicial)
    CASE WHEN ROW_NUMBER() OVER (PARTITION BY comercio_id ORDER BY __START_AT) = 1
         THEN TIMESTAMP'1900-01-01'
         ELSE timestamp_millis(__START_AT.src_ts_ms) END    AS vigente_desde,
    COALESCE(timestamp_millis(__END_AT.src_ts_ms), TIMESTAMP'9999-12-31') AS vigente_hasta,
    __END_AT IS NULL                                         AS es_actual
FROM adquirencia.silver.comercio;

CREATE OR REFRESH MATERIALIZED VIEW adquirencia.gold.dim_tarjeta
COMMENT 'Combinaciones de marca y tipo de tarjeta.'
AS
SELECT DISTINCT
    xxhash64(marca, tipo_tarjeta) AS tarjeta_sk,
    marca, tipo_tarjeta
FROM adquirencia.silver.transaccion_detalle;

CREATE OR REFRESH MATERIALIZED VIEW adquirencia.gold.dim_canal
AS SELECT canal_id, canal, tipo_canal FROM adquirencia.silver.canal;

CREATE OR REFRESH MATERIALIZED VIEW adquirencia.gold.dim_sucursal
AS SELECT sucursal_id, nombre_sucursal, provincia, ciudad, region, red_adquirente
   FROM adquirencia.silver.sucursal;

CREATE OR REFRESH MATERIALIZED VIEW adquirencia.gold.dim_fecha
AS
SELECT
    CAST(date_format(f, 'yyyyMMdd') AS INT) AS fecha_key,
    f                                       AS fecha,
    year(f) AS anio, quarter(f) AS trimestre, month(f) AS mes,
    date_format(f, 'MMMM') AS nombre_mes, dayofweek(f) AS dia_semana,
    dayofweek(f) IN (1, 7) AS es_fin_de_semana
FROM (SELECT explode(sequence(DATE'2024-01-01', DATE'2030-12-31', INTERVAL 1 DAY)) AS f);

-- Hecho: grano = una transacción
CREATE OR REFRESH MATERIALIZED VIEW adquirencia.gold.fact_transaccion
CLUSTER BY (fecha_key, comercio_sk)
COMMENT 'Hecho de transacciones de adquirencia. Grano: una transacción.'
AS
SELECT
    t.trx_id,
    CAST(date_format(t.fecha_trx, 'yyyyMMdd') AS INT) AS fecha_key,
    dc.comercio_sk,
    xxhash64(d.marca, d.tipo_tarjeta)                 AS tarjeta_sk,
    t.canal_id,
    t.sucursal_id,
    e.estado_liquidacion,
    COALESCE(e.tiene_contracargo, false)              AS tiene_contracargo,
    t.codigo_respuesta = '00'                         AS aprobada,
    t.monto,
    d.comision,
    d.cuotas
FROM      adquirencia.silver.transaccion         t
LEFT JOIN adquirencia.silver.transaccion_detalle d  ON d.trx_id = t.trx_id
LEFT JOIN adquirencia.gold.dim_comercio          dc ON dc.comercio_id = t.comercio_id
                                                   AND t.fecha_trx >= dc.vigente_desde
                                                   AND t.fecha_trx <  dc.vigente_hasta
LEFT JOIN adquirencia.gold.trx_estado_vigente    e  ON e.trx_id = t.trx_id;

-- -----------------------------------------------------------------------------
-- 3. Agregados KPI (dashboards, Genie, alertas operativas)
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW adquirencia.gold.kpi_comercio_diario
CLUSTER BY (fecha, comercio_id)
COMMENT 'KPIs diarios por comercio, marca y canal.'
AS
SELECT
    fecha, comercio_id, nombre_comercial, mcc, segmento, oficial_id, marca, canal,
    COUNT(*)                                          AS trx_total,
    COUNT_IF(aprobada)                                AS trx_aprobadas,
    ROUND(COUNT_IF(aprobada) / COUNT(*), 4)           AS tasa_aprobacion,
    SUM(monto) FILTER (WHERE aprobada)                AS monto_aprobado,
    ROUND(AVG(monto) FILTER (WHERE aprobada), 2)      AS ticket_promedio,
    SUM(comision)                                     AS comision_total,
    COUNT_IF(tiene_contracargo)                       AS contracargos,
    COUNT_IF(estado_liquidacion = 'LIQUIDADA')        AS trx_liquidadas
FROM adquirencia.gold.trx_consulta_operativa
GROUP BY ALL;

-- -----------------------------------------------------------------------------
-- 4. Control de cuadre (calidad): conteo y monto por día deben coincidir con el origen.
--    El job de cuadre compara esta vista con un resumen diario extraído de la réplica.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW adquirencia.gold.control_cuadre_diario
COMMENT 'Totales diarios del lakehouse para conciliar contra el sistema transaccional.'
AS
SELECT CAST(fecha_trx AS DATE) AS fecha, COUNT(*) AS trx, SUM(monto) AS monto
FROM adquirencia.silver.transaccion
GROUP BY ALL;
