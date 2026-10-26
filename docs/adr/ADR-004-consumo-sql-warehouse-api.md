# ADR-004: Consumo vía SQL Warehouse serverless y API parametrizada

**Estado:** Aceptado

## Contexto

El front-end ejecuta hoy Stored Procedures en MySQL a través de Amazon API Gateway de forma síncrona. Las consultas largas exceden el tiempo de integración (HTTP 504), pero siguen ejecutándose en la base. Hay picos de concurrencia en horarios de cierre y conciliación.

## Alternativas evaluadas

| Opción                                              | A favor                                                                                | En contra                                                |
| --------------------------------------------------- | -------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| Mantener SPs apuntando a la réplica                 | Sin cambios en el front                                                                | Mismo modelo y mismos límites; solo traslada la carga    |
| Front conectado directo al warehouse                | Simple                                                                                 | Expone credenciales y SQL libre; sin control de contrato |
| Motor de búsqueda dedicado (p. ej. Elasticsearch)   | Latencias muy bajas                                                                    | Otro sistema a sincronizar, gobernar y pagar             |
| **API de consultas sobre SQL Warehouse serverless** | Contrato controlado; escalado por concurrencia; gobierno de Unity Catalog aplica igual | Latencia de segundos (no milisegundos)                   |

## Decisión

- Un servicio de consultas expuesto por **Azure API Management** que traduce filtros a SQL parametrizado (lista blanca de 11 filtros) contra `gold.trx_consulta_operativa`.
- Reglas: rango de fechas obligatorio (máx. 93 días), paginación, timeout por sentencia, límite de filas.
- Exportaciones grandes: patrón asíncrono (`202 Accepted` + consulta de estado).
- El servicio consulta con la identidad del usuario (o un _service principal_ con RLS por atributo), de modo que las reglas de fila y máscara de Unity Catalog se aplican.
- Se retiran los SPs analíticos de MySQL al finalizar la migración.

## Consecuencias

- Desaparecen errros 504 por diseño.
- Imposible inyectar SQL ni lanzar consultas sin límite.
- Concurrencia absorbida por el autoescalado del warehouse y su caché de resultados.
- Requiere cambios en el front-end (nuevo contrato de API); se migra por pantallas.
