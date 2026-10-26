-- =============================================================================
-- Gobierno y seguridad en Unity Catalog
-- Grupos sincronizados desde Microsoft Entra ID (SCIM).
--   grp_adq_ingenieria        : ingeniería de datos (custodios)
--   grp_adq_analistas         : analistas de negocio
--   grp_adq_oficiales         : oficiales de operaciones (front-end vía API)
--   grp_adq_supervision       : supervisores; ven todos los comercios
--   grp_adq_datos_sensibles   : autorizados a ver identificación completa del comercio
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Organización: un catálogo por dominio, un esquema por capa
-- -----------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS adquirencia COMMENT 'Dominio de Adquirencia de Tarjetas de Crédito';
CREATE SCHEMA  IF NOT EXISTS adquirencia.bronze   COMMENT 'Eventos CDC crudos. Solo ingeniería.';
CREATE SCHEMA  IF NOT EXISTS adquirencia.silver   COMMENT 'Entidades limpias y vigentes. Solo ingeniería.';
CREATE SCHEMA  IF NOT EXISTS adquirencia.gold     COMMENT 'Modelos certificados para analítica.';
CREATE SCHEMA  IF NOT EXISTS adquirencia.consumo  COMMENT 'Vistas seguras expuestas a usuarios y API.';
CREATE SCHEMA  IF NOT EXISTS adquirencia.gobierno COMMENT 'Tablas de control: asignaciones, reglas, cuadres.';

ALTER SCHEMA adquirencia.bronze SET TAGS ('capa' = 'bronze', 'owner_negocio' = 'Gerencia Adquirencia');
ALTER SCHEMA adquirencia.silver SET TAGS ('capa' = 'silver', 'owner_negocio' = 'Gerencia Adquirencia');
ALTER SCHEMA adquirencia.gold   SET TAGS ('capa' = 'gold',   'certificado'   = 'true');

-- -----------------------------------------------------------------------------
-- 2. Mínimo privilegio
-- -----------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG adquirencia TO `grp_adq_ingenieria`, `grp_adq_analistas`, `grp_adq_oficiales`;

GRANT ALL PRIVILEGES ON SCHEMA adquirencia.bronze TO `grp_adq_ingenieria`;
GRANT ALL PRIVILEGES ON SCHEMA adquirencia.silver TO `grp_adq_ingenieria`;
GRANT ALL PRIVILEGES ON SCHEMA adquirencia.gold   TO `grp_adq_ingenieria`;

GRANT USE SCHEMA, SELECT ON SCHEMA adquirencia.consumo TO `grp_adq_analistas`, `grp_adq_oficiales`;
-- Los analistas no leen gold directamente: consumen vistas con RLS y máscaras aplicadas.

-- -----------------------------------------------------------------------------
-- 3. Seguridad a nivel de fila: cada oficial ve solo sus comercios asignados
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS adquirencia.gobierno.asignacion_oficial (
    usuario     STRING  COMMENT 'Correo corporativo (Entra ID)',
    oficial_id  STRING  COMMENT 'Código de oficial en bco_t_comercio_360',
    vigente     BOOLEAN
) COMMENT 'Mapeo usuario → cartera de comercios. Mantenido por el Data Owner.';

-- -----------------------------------------------------------------------------
-- 4. Enmascaramiento de columnas sensibles
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION adquirencia.gobierno.mask_identificacion(valor STRING)
RETURNS STRING
COMMENT 'Muestra solo los últimos 4 caracteres salvo para grp_adq_datos_sensibles.'
RETURN CASE
    WHEN is_account_group_member('grp_adq_datos_sensibles') THEN valor
    ELSE concat(repeat('*', greatest(length(valor) - 4, 0)), right(valor, 4))
END;

-- -----------------------------------------------------------------------------
-- 5. Vistas seguras de consumo para Api y Power BI (RLS + máscara).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW adquirencia.consumo.v_trx_operativa
COMMENT 'Transacciones para consulta operativa. Filtrada por cartera del usuario; identificación del comercio enmascarada.'
AS
SELECT
    t.* EXCEPT (ruc),
    adquirencia.gobierno.mask_identificacion(t.ruc) AS ruc
FROM adquirencia.gold.trx_consulta_operativa t
WHERE is_account_group_member('grp_adq_supervision')
   OR EXISTS (
        SELECT 1
        FROM adquirencia.gobierno.asignacion_oficial a
        WHERE a.usuario = current_user()
          AND a.oficial_id = t.oficial_id
          AND a.vigente
   );

CREATE OR REPLACE VIEW adquirencia.consumo.v_kpi_comercio_diario
COMMENT 'KPIs diarios por comercio con la misma regla de cartera.'
AS
SELECT k.*
FROM adquirencia.gold.kpi_comercio_diario k
WHERE is_account_group_member('grp_adq_supervision')
   OR is_account_group_member('grp_adq_analistas')
   OR EXISTS (
        SELECT 1 FROM adquirencia.gobierno.asignacion_oficial a
        WHERE a.usuario = current_user() AND a.oficial_id = k.oficial_id AND a.vigente
   );

ALTER VIEW adquirencia.consumo.v_trx_operativa SET TAGS ('clasificacion' = 'confidencial', 'pci' = 'sin_pan');

-- -----------------------------------------------------------------------------
-- 6. Auditoría y linaje (system tables)
-- -----------------------------------------------------------------------------

-- Identificacion de accceso a datos de consumo en los últimos 7 días
SELECT event_time, user_identity.email, action_name,
       request_params.full_name_arg AS objeto
FROM   system.access.audit
WHERE  service_name = 'unityCatalog'
  AND  request_params.full_name_arg LIKE 'adquirencia.consumo.%'
  AND  event_date >= current_date() - INTERVAL 7 DAYS
ORDER BY event_time DESC;

-- Linaje: Tablas que alimentan la vista operativa (impacto ante un cambio de esquema)
SELECT DISTINCT source_table_full_name, target_table_full_name
FROM   system.access.table_lineage
WHERE  target_table_full_name LIKE 'adquirencia.%'
  AND  event_date >= current_date() - INTERVAL 30 DAYS;
