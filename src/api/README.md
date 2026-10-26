# API de Consulta Operativa

Servicio en **FastAPI** que reemplaza a los Stored Procedures analíticos de MySQL. Consulta `adquirencia.consumo.v_trx_operativa` en Databricks SQL Serverless con la identidad del usuario, por lo que la seguridad por fila y las máscaras se aplican automáticamente.

| Endpoint | Comportamiento |
|----------|----------------|
| `POST /v1/transacciones/consulta` | Síncrono. 11 filtros, rango de fechas obligatorio (máx. 93 días), paginación por *keyset*, timeout de 25 s |
| `POST /v1/exportaciones` | Asíncrono. Devuelve `202` con un id; usa la Statement Execution API |
| `GET /v1/exportaciones/{id}` | Estado y enlaces de descarga del CSV |

La lógica crítica (contrato de filtros y construcción de SQL parametrizado) está aislada en [`app/filtros.py`](app/filtros.py) y cubierta por tests unitarios.

```bash
cd src/api
pip install -r requirements.txt
python -m pytest -q tests        # 9 tests
```

Ejemplo de solicitud:

```json
POST /v1/transacciones/consulta?page_size=100
{
  "fecha_desde": "2026-09-01",
  "fecha_hasta": "2026-09-30",
  "marca": ["VISA"],
  "estado_liquidacion": ["PENDIENTE"],
  "canal": ["E-COMMERCE"],
  "monto_min": "100.00"
}
```
