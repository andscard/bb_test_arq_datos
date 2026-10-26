# ADR-002: Lakehouse en Azure Databricks

**Estado:** Aceptado

## Contexto

El consumo analítico y de BI debe alinearse con Azure. Se requiere procesamiento en _streaming_ para monitoreo, consultas SQL de alta concurrencia, gobierno con linaje y un modelo único para ingeniería, analítica y autoservicio.

## Alternativas evaluadas

| Opción                                            | A favor                                                                                                                           | En contra                                                                                        |
| ------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| Data warehouse dedicado (Synapse dedicated pool)  | SQL maduro                                                                                                                        | Cómputo acoplado y aprovisionado; _streaming_ limitado; costo fijo                               |
| Microsoft Fabric                                  | Integración nativa con Power BI                                                                                                   | Menor control fino sobre pipelines CDC y _streaming_ a esta escala; curva de adopción del equipo |
| Réplica analítica en otro motor relacional        | Rápido de montar                                                                                                                  | Sigue sin resolver gobierno, histórico ni escalado                                               |
| **Azure Databricks + Delta Lake + Unity Catalog** | Streaming y batch en el mismo motor; SQL Warehouse serverless; linaje automático; formato abierto (Delta); experiencia del equipo | Requiere disciplina en costos de cómputo                                                         |

## Decisión

Implementar un Lakehouse en Azure Databricks sobre ADLS Gen2 con:

- **Lakeflow Declarative Pipelines** (antes Delta Live Tables) para Bronze/Silver/Gold, con _expectations_ de calidad y `APPLY CHANGES` para CDC.
- **Unity Catalog** como capa de gobierno técnico (permisos, linaje, auditoría), integrado con Microsoft Purview para el catálogo corporativo.
- **Databricks SQL Serverless** para consumo.

## Consecuencias

- Un solo motor para ingesta en tiempo casi real, transformación y consumo.
- Datos en formato abierto (Delta/Parquet): sin dependencia de un motor propietario.
- Power BI se conecta de forma nativa; se mantiene la alineación con Azure.
- Costos variables: se controlan con serverless + auto-stop, políticas de cómputo y presupuestos por _tags_.
