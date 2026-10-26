"""
API de consultas operativas de adquirencia.

Publicada detrás de Azure API Management (validación de JWT de Entra ID, cuotas por
consumidor, rate limiting). Consulta Databricks SQL Serverless con la identidad del
usuario, de modo que la seguridad por fila y las máscaras de Unity Catalog se aplican.

  POST /v1/transacciones/consulta   → síncrono, paginado, acotado (segundos)
  POST /v1/exportaciones            → asíncrono, 202 + id (sin timeouts de gateway)
  GET  /v1/exportaciones/{id}       → estado y enlaces de descarga
"""
from __future__ import annotations

import os
from datetime import date, datetime
from decimal import Decimal

import httpx
from databricks import sql as dbsql
from fastapi import Depends, FastAPI, Header, HTTPException, Query

from .filtros import (
    FiltrosConsulta,
    a_parametros_statement_api,
    codificar_cursor,
    construir_consulta,
)

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]            # adb-xxxx.azuredatabricks.net
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]
HTTP_PATH = f"/sql/1.0/warehouses/{WAREHOUSE_ID}"
TIMEOUT_CONSULTA_SEG = "25"                                 # menor que el timeout del gateway

app = FastAPI(title="API Consulta Operativa — Adquirencia", version="1.0.0")


def token_usuario(authorization: str = Header(...)) -> str:
    """APIM ya validó el JWT. En producción el token se intercambia por uno con audiencia
    de Azure Databricks mediante el flujo On-Behalf-Of (MSAL); aquí se simplifica."""
    if not authorization.startswith("Bearer "):
        raise HTTPException(401, "Token requerido")
    return authorization.removeprefix("Bearer ")


def _serializar(valor):
    if isinstance(valor, Decimal):
        return str(valor)                       # montos sin pérdida de precisión
    if isinstance(valor, (datetime, date)):
        return valor.isoformat()
    return valor


@app.post("/v1/transacciones/consulta")
def consultar(
    filtros: FiltrosConsulta,
    page_size: int = Query(100, ge=1, le=500),
    cursor: str | None = None,
    token: str = Depends(token_usuario),
):
    try:
        sentencia, params = construir_consulta(filtros, page_size, cursor)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc

    with dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=HTTP_PATH,
        access_token=token,
        session_configuration={"STATEMENT_TIMEOUT": TIMEOUT_CONSULTA_SEG},
    ) as conn, conn.cursor() as cur:
        cur.execute(sentencia, parameters=params)
        columnas = [c[0] for c in cur.description]
        filas = cur.fetchall()

    hay_mas = len(filas) > page_size
    filas = filas[:page_size]
    datos = [{c: _serializar(v) for c, v in zip(columnas, f)} for f in filas]
    siguiente = (
        codificar_cursor(filas[-1][columnas.index("fecha_trx")], filas[-1][columnas.index("trx_id")])
        if hay_mas else None
    )
    return {"datos": datos, "cantidad": len(datos), "siguiente_cursor": siguiente}


@app.post("/v1/exportaciones", status_code=202)
def crear_exportacion(filtros: FiltrosConsulta, token: str = Depends(token_usuario)):
    """Ejecución asíncrona con la Statement Execution API: responde de inmediato."""
    sentencia, params = construir_consulta(filtros, page_size=None)
    resp = httpx.post(
        f"https://{DATABRICKS_HOST}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "warehouse_id": WAREHOUSE_ID,
            "statement": sentencia,
            "parameters": a_parametros_statement_api(params),
            "wait_timeout": "0s",              # no esperar: devolver el id
            "disposition": "EXTERNAL_LINKS",   # resultado grande → enlaces de descarga
            "format": "CSV",
        },
        timeout=10,
    )
    resp.raise_for_status()
    return {"exportacion_id": resp.json()["statement_id"], "estado": "EN_PROCESO"}


@app.get("/v1/exportaciones/{exportacion_id}")
def estado_exportacion(exportacion_id: str, token: str = Depends(token_usuario)):
    resp = httpx.get(
        f"https://{DATABRICKS_HOST}/api/2.0/sql/statements/{exportacion_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    resp.raise_for_status()
    cuerpo = resp.json()
    estado = cuerpo["status"]["state"]
    enlaces = [
        e["external_link"] for e in cuerpo.get("result", {}).get("external_links", [])
    ] if estado == "SUCCEEDED" else []
    return {"exportacion_id": exportacion_id, "estado": estado, "enlaces": enlaces}
