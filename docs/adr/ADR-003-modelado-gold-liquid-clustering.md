# ADR-003: Modelo estrella + tabla de consulta operativa con Liquid Clustering

**Estado:** Aceptado

## Contexto

Los usuarios combinan hasta 11 filtros sobre cientos de millones de transacciones (crecimiento ≈ 460k/día). Hay dos patrones de consulta distintos:

1. **Operativo:** buscar transacciones concretas (comercio X, marca Y, estado Z, últimos 7 días) → pocas filas, respuesta en segundos.
2. **Analítico:** agregar millones de filas (ventas por MCC y mes) → BI y KPIs.

## Alternativas evaluadas

| Opción                                                     | A favor                                                                      | En contra                                                               |
| ---------------------------------------------------------- | ---------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| Replicar el modelo normalizado y consultar con _joins_     | Sin modelado                                                                 | Traslada el problema a otro motor                                       |
| Solo modelo estrella                                       | Estándar para BI                                                             | La búsqueda operativa sigue requiriendo 6 _joins_                       |
| Particionamiento fijo por fecha + Z-ORDER                  | Conocido                                                                     | Riesgo de archivos pequeños; cambiar columnas exige reescribir la tabla |
| **Estrella + tabla ancha operativa con Liquid Clustering** | Cada patrón tiene su estructura óptima; clustering incremental y modificable | Duplicación controlada de datos en Gold                                 |

## Decisión

- **Modelo estrella** (`fact_transaccion` + dimensiones; `dim_comercio` como SCD Tipo 2) para BI.
- **`trx_consulta_operativa`**: tabla desnormalizada, una fila por transacción con estado vigente resuelto, para el front-end.
- **Liquid Clustering** por `fecha_trx, comercio_id, estado_liquidacion` (filtros más usados) y _data skipping_ por estadísticas para el resto.
- **Materialized views** de agregados diarios para KPIs.

## Consecuencias

- Las consultas operativas leen una sola tabla y solo los archivos relevantes.
- Las claves de clustering se pueden ajustar según la telemetría de uso, sin reescritura completa.
- Duplicación de datos en Gold: costo de almacenamiento bajo frente al ahorro de cómputo.
- La tabla operativa depende del pipeline: se monitorea su frescura.
