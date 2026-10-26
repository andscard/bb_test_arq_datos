#!/usr/bin/env bash
# =============================================================================
# Réplicas de lectura de Amazon RDS MySQL
#
#   Réplica A (analítica, TEMPORAL) — Fase 0
#     Recibe los Stored Procedures actuales para que dejen de cargar la principal.
#     Se elimina en la Fase 2, cuando la API de consultas reemplaza a los SPs.
#
#   Réplica B (CDC, PERMANENTE) — Fase 1
#     Solo la usa Debezium para leer el binlog. Separada de la Réplica A para que
#     la carga de los SPs no retrase el CDC (replica lag).
#
# =============================================================================
set -euo pipefail

PRINCIPAL="adq-mysql-prod"
REPLICA_A="adq-mysql-replica-analitica"
REPLICA_B="adq-mysql-replica-cdc"
FAMILIA="mysql8.0"
CLASE="db.r6g.large"
SG_DATOS="sg-xxxxxxxx"          # security group privado

# -----------------------------------------------------------------------------
# Paso 0 — Principal: binlog apto para CDC
# Modificacion de parameter group
# -----------------------------------------------------------------------------
aws rds modify-db-parameter-group \
  --db-parameter-group-name adq-principal-params \
  --parameters \
    "ParameterName=binlog_format,ParameterValue=ROW,ApplyMethod=immediate" \
    "ParameterName=binlog_row_image,ParameterValue=FULL,ApplyMethod=immediate"
# OBLIGATORIO para la carga inicial (incremental snapshot de Debezium con read.only=true):
# GTID activo en la principal y en AMBAS réplicas (parámetros gtid-mode y
# enforce_gtid_consistency en cada parameter group).


# =============================================================================
# FASE 0 — Réplica A (analítica, temporal)
# =============================================================================
aws rds create-db-parameter-group \
  --db-parameter-group-name adq-replica-analitica-params \
  --db-parameter-group-family "$FAMILIA" \
  --description "Replica A: SPs analiticos (temporal)"

aws rds modify-db-parameter-group \
  --db-parameter-group-name adq-replica-analitica-params \
  --parameters \
    "ParameterName=read_only,ParameterValue=0,ApplyMethod=immediate" \
    "ParameterName=max_execution_time,ParameterValue=30000,ApplyMethod=immediate"
# read_only=0 SOLO para poder crear índices propios de esta réplica
# (ver src/mysql/01_contencion_inmediata.sql).

aws rds create-db-instance-read-replica \
  --db-instance-identifier "$REPLICA_A" \
  --source-db-instance-identifier "$PRINCIPAL" \
  --db-instance-class "$CLASE" \
  --db-parameter-group-name adq-replica-analitica-params \
  --vpc-security-group-ids "$SG_DATOS" \
  --no-publicly-accessible \
  --tags Key=fase,Value=0 Key=vida,Value=temporal Key=proposito,Value=sps-analiticos

aws rds wait db-instance-available --db-instance-identifier "$REPLICA_A"

# Endpoint que reemplaza al de la principal en la conexión que usan los SPs.
aws rds describe-db-instances --db-instance-identifier "$REPLICA_A" \
  --query "DBInstances[0].Endpoint.Address" --output text


# =============================================================================
# FASE 1 — Réplica B (CDC, permanente)
# =============================================================================
aws rds create-db-parameter-group \
  --db-parameter-group-name adq-replica-cdc-params \
  --db-parameter-group-family "$FAMILIA" \
  --description "Replica B: origen del binlog para Debezium"

aws rds modify-db-parameter-group \
  --db-parameter-group-name adq-replica-cdc-params \
  --parameters \
    "ParameterName=binlog_format,ParameterValue=ROW,ApplyMethod=immediate" \
    "ParameterName=binlog_row_image,ParameterValue=FULL,ApplyMethod=immediate"
# La réplica debe escribir en SU binlog los cambios que recibe (log_replica_updates).
# En MySQL 8 viene activado por defecto; se verifica en el paso de comprobación.

aws rds create-db-instance-read-replica \
  --db-instance-identifier "$REPLICA_B" \
  --source-db-instance-identifier "$PRINCIPAL" \
  --db-instance-class "$CLASE" \
  --db-parameter-group-name adq-replica-cdc-params \
  --vpc-security-group-ids "$SG_DATOS" \
  --no-publicly-accessible \
  --tags Key=fase,Value=1 Key=vida,Value=permanente Key=proposito,Value=cdc-debezium

aws rds wait db-instance-available --db-instance-identifier "$REPLICA_B"

aws rds modify-db-instance \
  --db-instance-identifier "$REPLICA_B" \
  --backup-retention-period 1 \
  --apply-immediately

aws rds wait db-instance-available --db-instance-identifier "$REPLICA_B"

# Endpoint del database.hostname del conector (src/cdc/).
aws rds describe-db-instances --db-instance-identifier "$REPLICA_B" \
  --query "DBInstances[0].Endpoint.Address" --output text


# =============================================================================
# FASE 2 — Retiro de la Réplica A
# Ejecutar SOLO cuando el front-end consume la API y los SPs están apagados.
# =============================================================================
# aws rds delete-db-instance \
#   --db-instance-identifier "$REPLICA_A" \
#   --skip-final-snapshot
