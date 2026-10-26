# ADR-005: Tratamiento del número de tarjeta (PAN) en el entorno analítico

**Estado:** Aceptado

## Contexto

El sistema transaccional de AWS almacena el **PAN** (número completo de la tarjeta) y forma parte del CDE (entorno de datos del titular) bajo PCI DSS, que exige auditoría anual a todo sistema que almacene, procese o transmita datos de tarjeta.
La analítica en Azure necesita identificar emisores, correlacionar contracargos por tarjeta y mostrar la tarjeta a un oficial. La pregunta de diseño es si el PAN debe llegar a Azure y cómo protegerlo. Los datos sensibles de autenticación (banda o chip, CVV, PIN) quedan descartados: PCI prohíbe guardarlos tras la autorización, aun cifrados.

## Necesidades analíticas y dato suficiente

| Necesidad                                                                           | Dato suficiente                                                          |
| ----------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| Emisor, país y producto de la tarjeta                                               | **BIN** (primeros 6 u 8 dígitos)                                         |
| Mostrar la tarjeta a un oficial                                                     | **Últimos 4 dígitos**                                                    |
| Correlacionar transacciones de una misma tarjeta (fraude, contracargos recurrentes) | **Token** de la tarjeta (valor fijo, sin relación matemática con el PAN) |
| Disputa ante la red (Visa, Mastercard)                                              | PAN completo, gestionado en el sistema operativo de AWS (ya es CDE)      |

## Alternativas evaluadas

| Opción                                                                  | Azure en alcance PCI |           Cubre la analítica           | Complejidad y costo                                                                                                                                                                    |
| ----------------------------------------------------------------------- | :------------------: | :------------------------------------: | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Llevar el PAN en claro y enmascararlo en las consultas                  |          Sí          |                   Sí                   | Inaceptable: el dato queda almacenado legible (la máscara solo oculta al consultar)                                                                                                    |
| Cifrar el PAN por columna y descifrar solo para un grupo                |          Sí          |                   Sí                   | Alta: cifrado antes de salir de AWS (transformación propia en el conector), llaves en HSM, rotación, separación de funciones, control de administradores y auditoría PCI de todo Azure |
| **Excluir el PAN en el origen; llevar BIN, últimos 4 y token**          |          No          |                   Sí                   | Baja: una propiedad del conector (`column.exclude.list`)                                                                                                                               |
| Excluir el PAN + servicio de detokenización en AWS para casos puntuales |          No          | Sí, y el PAN se consulta caso por caso | Media: servicio adicional dentro del CDE                                                                                                                                               |

## Decisión

1. El PAN y los datos sensibles de autenticación se excluyen en el conector Debezium y nunca salen de AWS. Esto aplica también a la carga inicial, porque se hace con el mismo conector (_incremental snapshot_, ver [ADR-001](ADR-001-cdc-debezium-binlog.md)).
2. El entorno analítico recibe BIN, últimos 4 dígitos y token de la tarjeta.
3. La capa Bronze permite detener el pipeline si un evento contiene alguna columna prohibida (por ejemplo, si alguien quita la exclusión del conector por error).
4. Si un área demuestra que necesita el PAN completo, se implementa un servicio de detokenización dentro del CDE de AWS: recibe un token, verifica el permiso del usuario, registra la consulta y devuelve solo ese PAN. El PAN no se almacena en Azure.

## Consecuencias

- Soluciones de Azure quedan fuera del alcance de PCI DSS, el mayor ahorro de cumplimiento del diseño.
- Se aplica la minimización de datos (PCI, requisitos 3 y 7), y la analítica de fraude y contracargos funciona igual con el token.
- Se Requiere un token estable por tarjeta en el origen. Si no existe, se genera en AWS con un hash con llave (HMAC) cuya llave nunca sale del CDE. Usar un token sin relación matemática con el PAN también evita que se pueda reconstruir a partir del BIN y los últimos 4.
- Quienes hoy ven el PAN completo migran al servicio de detokenización; se valida con el negocio antes de la Fase 2.
