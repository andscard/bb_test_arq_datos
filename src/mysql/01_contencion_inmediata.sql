-- =============================================================================
-- Fase 0 — Contención inmediata en Amazon RDS MySQL (semanas 1–2)
-- Objetivo: detener el impacto de las consultas analíticas sobre la autorización
-- mientras se construye la arquitectura objetivo. NO es la solución definitiva.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Diagnóstico: evidencia de causas raíz 
-- -----------------------------------------------------------------------------

-- 1.1 Consultas que más tiempo total consumen (los SPs analíticos deberían encabezar)
SELECT  DIGEST_TEXT,
        COUNT_STAR                          AS ejecuciones,
        ROUND(SUM_TIMER_WAIT / 1e12, 1)     AS seg_totales,
        ROUND(AVG_TIMER_WAIT / 1e12, 2)     AS seg_promedio,
        SUM_ROWS_EXAMINED                   AS filas_examinadas,
        SUM_ROWS_SENT                       AS filas_enviadas,
        SUM_NO_INDEX_USED                   AS ejec_sin_indice
FROM    performance_schema.events_statements_summary_by_digest
ORDER BY SUM_TIMER_WAIT DESC
LIMIT 15;
-- Señal clave: filas_examinadas >> filas_enviadas  => escaneos por falta de índice útil.

-- 1.2 Esperas por bloqueos
SELECT  wait_age_secs,
        waiting_query,             
        blocking_query,             
        blocking_pid,
        sql_kill_blocking_query
FROM    sys.innodb_lock_waits
ORDER BY wait_age_secs DESC;

-- 1.3 Transacciones/lecturas largas que retienen versiones (MVCC) y crecen el history list
SELECT  trx_id, trx_state, trx_started,
        TIMESTAMPDIFF(SECOND, trx_started, NOW()) AS seg_abierta,
        trx_rows_locked, LEFT(trx_query, 120) AS consulta
FROM    information_schema.innodb_trx
ORDER BY trx_started
LIMIT 10;

SELECT NAME, COUNT AS history_list_length
FROM   information_schema.innodb_metrics
WHERE  NAME = 'trx_rseg_history_len';

-- -----------------------------------------------------------------------------
-- 2. Aislar la carga analítica: 
--    Creacion de usuario dedicado con límites de conexion y configuracion de watchdog
--    Los SPs analíticos se reapuntan al endpoint de la Replica A
-------------------------------------------------------------------------------
CREATE USER IF NOT EXISTS 'svc_analitica'@'%'
    IDENTIFIED BY '<desde-AWS-Secrets-Manager>'
    WITH MAX_USER_CONNECTIONS 20;       

GRANT SELECT, EXECUTE ON adquirencia.* TO 'svc_analitica'@'%';

-- Watchdog (ejecutar cada minuto desde un scheduler externo):
-- lista las sesiones a terminar; luego se invoca CALL mysql.rds_kill(<id>) por cada una.
SELECT  id, user, time AS seg_ejecucion, LEFT(info, 120) AS consulta
FROM    information_schema.processlist
WHERE   user = 'svc_analitica'
  AND   command = 'Query'
  AND   time > 60;

-- -----------------------------------------------------------------------------
-- 3. Creación de índices compuestos en la réplica analítica
--    (parámetro read_only=0 en su parameter group)
--    Online DDL: no bloquea lecturas ni escrituras.
-- -----------------------------------------------------------------------------
ALTER TABLE bco_t_trx_maestra
    ADD INDEX ix_trx_comercio_fecha (comercio_id, fecha_trx),
    ALGORITHM = INPLACE, LOCK = NONE;

ALTER TABLE bco_t_trx_maestra
    ADD INDEX ix_trx_fecha_canal (fecha_trx, canal_id),
    ALGORITHM = INPLACE, LOCK = NONE;

-- Estado vigente: evita escanear todo el historial del workflow por transacción
ALTER TABLE bco_t_estado_workflow
    ADD INDEX ix_wf_trx_fecha (trx_id, fecha_estado),
    ALGORITHM = INPLACE, LOCK = NONE;

-- -----------------------------------------------------------------------------
-- 4. Preparación para CDC (Fase 1)
--    Parámetros de binlog y creación de la RÉPLICA B: src/aws/01_replicas_rds.sh
-- -----------------------------------------------------------------------------

-- 4.1 Conectado a la RÉPLICA B: retener 48 h de binlog (margen ante caídas de Debezium)
CALL mysql.rds_set_configuration('binlog retention hours', 48);
CALL mysql.rds_show_configuration;

-- 4.2 Conectado a la PRINCIPAL: usuario de Debezium (se replica a la Réplica B)
CREATE USER IF NOT EXISTS 'svc_debezium'@'%' IDENTIFIED BY '<desde-AWS-Secrets-Manager>';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
    ON *.* TO 'svc_debezium'@'%';
