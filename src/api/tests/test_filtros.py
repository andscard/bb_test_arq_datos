from datetime import date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.filtros import (
    FiltrosConsulta,
    a_parametros_statement_api,
    codificar_cursor,
    construir_consulta,
    decodificar_cursor,
)

BASE = {"fecha_desde": date(2026, 9, 1), "fecha_hasta": date(2026, 9, 30)}


def test_solo_rango_de_fechas_genera_consulta_minima():
    sql, params = construir_consulta(FiltrosConsulta(**BASE))
    assert "fecha_trx >= :fecha_desde" in sql
    assert "fecha_trx < :fecha_hasta_excl" in sql
    assert params["fecha_hasta_excl"] == datetime(2026, 10, 1)  # hasta inclusivo
    assert sql.rstrip().endswith("LIMIT 101")


def test_once_filtros_combinados_y_parametrizados():
    filtros = FiltrosConsulta(
        **BASE,
        comercio_id=[10, 20],
        marca=["VISA", "MASTERCARD"],
        tipo_tarjeta="CREDITO",
        estado_liquidacion=["LIQUIDADA"],
        canal=["E-COMMERCE"],
        tipo_pos=["DATAFONO"],
        mcc=["5411"],
        segmento=["PYME"],
        sucursal_id=[7],
        monto_min=Decimal("10.00"),
        monto_max=Decimal("500.00"),
    )
    sql, params = construir_consulta(filtros)
    assert "comercio_id IN (:comercio_id_0, :comercio_id_1)" in sql
    assert "marca IN (:marca_0, :marca_1)" in sql
    assert "tipo_tarjeta = :tipo_tarjeta" in sql
    assert params["marca_1"] == "MASTERCARD"
    assert params["monto_max"] == Decimal("500.00")
    assert sql.count(" AND ") == 12  # 13 condiciones (11 filtros, fechas y montos son rangos)


def test_valores_maliciosos_nunca_llegan_al_texto_sql():
    ataque = "x' OR '1'='1"
    sql, params = construir_consulta(FiltrosConsulta(**BASE, segmento=[ataque]))
    assert ataque not in sql
    assert params["segmento_0"] == ataque


def test_rango_de_fechas_excesivo_se_rechaza():
    with pytest.raises(ValidationError, match="rango máximo"):
        FiltrosConsulta(fecha_desde=date(2026, 1, 1), fecha_hasta=date(2026, 6, 30))


def test_fechas_invertidas_y_montos_invertidos_se_rechazan():
    with pytest.raises(ValidationError):
        FiltrosConsulta(fecha_desde=date(2026, 9, 30), fecha_hasta=date(2026, 9, 1))
    with pytest.raises(ValidationError):
        FiltrosConsulta(**BASE, monto_min=Decimal("100"), monto_max=Decimal("1"))


def test_marca_fuera_de_catalogo_y_mcc_invalido_se_rechazan():
    with pytest.raises(ValidationError):
        FiltrosConsulta(**BASE, marca=["MARCA_FALSA"])
    with pytest.raises(ValidationError):
        FiltrosConsulta(**BASE, mcc=["54A1"])


def test_paginacion_keyset_ida_y_vuelta():
    cursor = codificar_cursor(datetime(2026, 9, 15, 10, 30), 123456)
    assert decodificar_cursor(cursor) == (datetime(2026, 9, 15, 10, 30), 123456)
    sql, params = construir_consulta(FiltrosConsulta(**BASE), cursor=cursor)
    assert "trx_id < :cursor_trx" in sql
    assert params["cursor_trx"] == 123456


def test_cursor_manipulado_se_rechaza():
    with pytest.raises(ValueError, match="cursor inválido"):
        construir_consulta(FiltrosConsulta(**BASE), cursor="no-es-un-cursor")


def test_exportacion_sin_limite_y_parametros_tipados():
    sql, params = construir_consulta(FiltrosConsulta(**BASE, comercio_id=[5]), page_size=None)
    assert "LIMIT" not in sql
    tipos = {p["name"]: p["type"] for p in a_parametros_statement_api(params)}
    assert tipos == {
        "fecha_desde": "TIMESTAMP",
        "fecha_hasta_excl": "TIMESTAMP",
        "comercio_id_0": "BIGINT",
    }
