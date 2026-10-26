"""
Contrato de filtros y construcción de SQL parametrizado.

Reemplaza la lógica de los Stored Procedures con 11 filtros combinables:
  1 rango de fechas (obligatorio)   5 estado de liquidación   9 segmento
  2 comercio                        6 canal                  10 sucursal
  3 marca                           7 tipo de POS            11 rango de montos
  4 tipo de tarjeta                 8 MCC

Principios:
  - Los nombres de columna están fijos en código (lista blanca); el usuario solo aporta valores.
  - Los valores viajan como parámetros con nombre, nunca concatenados → sin inyección SQL.
  - Rango de fechas obligatorio y acotado, paginación por keyset y límite de filas.
"""
from __future__ import annotations

import base64
import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator

VISTA = "adquirencia.consumo.v_trx_operativa"
MAX_DIAS_RANGO = 93
MAX_VALORES_LISTA = 50

COLUMNAS_SALIDA = [
    "trx_id", "fecha_trx", "monto", "moneda", "aprobada", "codigo_autorizacion",
    "bin", "ultimos4", "comercio_id", "nombre_comercial", "mcc", "segmento",
    "marca", "tipo_tarjeta", "tipo_pos", "cuotas", "canal", "nombre_sucursal",
    "estado_liquidacion", "tiene_contracargo",
]


class Marca(str, Enum):
    VISA = "VISA"
    MASTERCARD = "MASTERCARD"
    AMEX = "AMEX"
    DINERS = "DINERS"
    DISCOVER = "DISCOVER"


class TipoTarjeta(str, Enum):
    CREDITO = "CREDITO"
    DEBITO = "DEBITO"
    PREPAGO = "PREPAGO"


class FiltrosConsulta(BaseModel):
    fecha_desde: date
    fecha_hasta: date
    comercio_id: list[int] | None = Field(None, min_length=1, max_length=MAX_VALORES_LISTA)
    marca: list[Marca] | None = Field(None, min_length=1)
    tipo_tarjeta: TipoTarjeta | None = None
    estado_liquidacion: list[str] | None = Field(None, min_length=1, max_length=10)
    canal: list[str] | None = Field(None, min_length=1, max_length=10)
    tipo_pos: list[str] | None = Field(None, min_length=1, max_length=10)
    mcc: list[str] | None = Field(None, min_length=1, max_length=MAX_VALORES_LISTA)
    segmento: list[str] | None = Field(None, min_length=1, max_length=10)
    sucursal_id: list[int] | None = Field(None, min_length=1, max_length=MAX_VALORES_LISTA)
    monto_min: Decimal | None = Field(None, ge=0)
    monto_max: Decimal | None = Field(None, ge=0)

    @model_validator(mode="after")
    def validar_rangos(self) -> "FiltrosConsulta":
        if self.fecha_hasta < self.fecha_desde:
            raise ValueError("fecha_hasta debe ser mayor o igual a fecha_desde")
        if (self.fecha_hasta - self.fecha_desde).days > MAX_DIAS_RANGO:
            raise ValueError(
                f"El rango máximo es de {MAX_DIAS_RANGO} días; use /v1/exportaciones para rangos mayores"
            )
        if self.monto_min is not None and self.monto_max is not None and self.monto_min > self.monto_max:
            raise ValueError("monto_min no puede ser mayor que monto_max")
        if self.mcc and any(not (m.isdigit() and len(m) == 4) for m in self.mcc):
            raise ValueError("Cada MCC debe tener 4 dígitos")
        return self


# Lista blanca: campo del contrato → columna de la vista
FILTROS_LISTA = {
    "comercio_id": "comercio_id",
    "marca": "marca",
    "estado_liquidacion": "estado_liquidacion",
    "canal": "canal",
    "tipo_pos": "tipo_pos",
    "mcc": "mcc",
    "segmento": "segmento",
    "sucursal_id": "sucursal_id",
}


def _valor(v: Any) -> Any:
    return v.value if isinstance(v, Enum) else v


def construir_consulta(
    filtros: FiltrosConsulta,
    page_size: int | None = 100,
    cursor: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Devuelve (sql, parametros). page_size=None genera la consulta completa (exportación)."""
    condiciones = ["fecha_trx >= :fecha_desde", "fecha_trx < :fecha_hasta_excl"]
    params: dict[str, Any] = {
        "fecha_desde": datetime.combine(filtros.fecha_desde, time.min),
        "fecha_hasta_excl": datetime.combine(filtros.fecha_hasta + timedelta(days=1), time.min),
    }

    for campo, columna in FILTROS_LISTA.items():
        valores = getattr(filtros, campo)
        if not valores:
            continue
        nombres = []
        for i, v in enumerate(dict.fromkeys(valores)):  # sin duplicados, orden estable
            nombre = f"{campo}_{i}"
            params[nombre] = _valor(v)
            nombres.append(f":{nombre}")
        condiciones.append(f"{columna} IN ({', '.join(nombres)})")

    if filtros.tipo_tarjeta:
        condiciones.append("tipo_tarjeta = :tipo_tarjeta")
        params["tipo_tarjeta"] = filtros.tipo_tarjeta.value
    if filtros.monto_min is not None:
        condiciones.append("monto >= :monto_min")
        params["monto_min"] = filtros.monto_min
    if filtros.monto_max is not None:
        condiciones.append("monto <= :monto_max")
        params["monto_max"] = filtros.monto_max

    if cursor:
        c_fecha, c_trx = decodificar_cursor(cursor)
        condiciones.append(
            "(fecha_trx < :cursor_fecha OR (fecha_trx = :cursor_fecha AND trx_id < :cursor_trx))"
        )
        params["cursor_fecha"] = c_fecha
        params["cursor_trx"] = c_trx

    sql = (
        f"SELECT {', '.join(COLUMNAS_SALIDA)}\n"
        f"FROM {VISTA}\n"
        f"WHERE " + "\n  AND ".join(condiciones) + "\n"
        "ORDER BY fecha_trx DESC, trx_id DESC"
    )
    if page_size is not None:
        if not 1 <= page_size <= 500:
            raise ValueError("page_size debe estar entre 1 y 500")
        sql += f"\nLIMIT {int(page_size) + 1}"  # +1 para saber si hay página siguiente
    return sql, params


def codificar_cursor(fecha_trx: datetime, trx_id: int) -> str:
    crudo = json.dumps({"f": fecha_trx.isoformat(), "id": trx_id}).encode()
    return base64.urlsafe_b64encode(crudo).decode()


def decodificar_cursor(cursor: str) -> tuple[datetime, int]:
    try:
        datos = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        return datetime.fromisoformat(datos["f"]), int(datos["id"])
    except Exception as exc:  # cursor manipulado o corrupto
        raise ValueError("cursor inválido") from exc


def a_parametros_statement_api(params: dict[str, Any]) -> list[dict[str, str]]:
    """Convierte los parámetros al formato de la Databricks SQL Statement Execution API."""
    salida = []
    for nombre, valor in params.items():
        if isinstance(valor, bool):
            tipo, texto = "BOOLEAN", str(valor).lower()
        elif isinstance(valor, int):
            tipo, texto = "BIGINT", str(valor)
        elif isinstance(valor, Decimal):
            tipo, texto = "DECIMAL(18,2)", str(valor)
        elif isinstance(valor, datetime):
            tipo, texto = "TIMESTAMP", valor.isoformat(sep=" ")
        elif isinstance(valor, date):
            tipo, texto = "DATE", valor.isoformat()
        else:
            tipo, texto = "STRING", str(valor)
        salida.append({"name": nombre, "value": texto, "type": tipo})
    return salida
