# Architecture Decision Records (ADR)

Un ADR documenta **una** decisión de arquitectura importante: el contexto que la motivó, las alternativas evaluadas, la decisión tomada y sus consecuencias (positivas y negativas). Permite que cualquier persona entienda en el futuro **por qué** el sistema es como es.

Formato usado (basado en Michael Nygard): **Estado · Contexto · Alternativas · Decisión · Consecuencias**.

| ADR                                               | Título                                                              | Estado   |
| ------------------------------------------------- | ------------------------------------------------------------------- | -------- |
| [001](ADR-001-cdc-debezium-binlog.md)             | CDC por binlog con Debezium sobre réplica dedicada                  | Aceptado |
| [002](ADR-002-lakehouse-azure-databricks.md)      | Lakehouse en Azure Databricks                                       | Aceptado |
| [003](ADR-003-modelado-gold-liquid-clustering.md) | Modelo estrella + tabla de consulta operativa con Liquid Clustering | Aceptado |
| [004](ADR-004-consumo-sql-warehouse-api.md)       | Consumo vía SQL Warehouse serverless y API parametrizada            | Aceptado |
| [005](ADR-005-tratamiento-del-pan.md)             | Tratamiento del número de tarjeta (PAN): excluir en el origen       | Aceptado |
