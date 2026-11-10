"""Pruebas de las alertas A1 y A2 segun docs/casos_prueba_alertas.md.

Usan los eventos reales de samples/ y la funcion pura rapidgo.alertas.evaluar_alertas.
Los casos llevan el mismo Id que en el documento (A1-01, A2-03, AC-01...).
"""
import json
from datetime import timedelta
from pathlib import Path

import pytest

from rapidgo.alertas import A1, A2, a_fecha, evaluar_alertas

SAMPLES = Path(__file__).resolve().parent.parent / "samples"


def _cargar(nombre):
    lineas = (SAMPLES / nombre).read_text(encoding="utf-8").splitlines()
    return [json.loads(x) for x in lineas if x.strip()]


CICLO = _cargar("eventos_ciclo_vida.jsonl")
DETENIDA = _cargar("eventos_entrega_detenida.jsonl")


def pedido(eventos, order_id):
    return [e for e in eventos if e["order_id"] == order_id]


ORD_123 = pedido(CICLO, "ORD-000123")
ORD_124 = pedido(CICLO, "ORD-000124")
ORD_126 = pedido(DETENIDA, "ORD-000126")


def hora(hhmmss):
    """'14:40:01' -> datetime del 2026-10-21 en UTC."""
    return a_fecha(f"2026-10-21T{hhmmss}Z")


def evento_extra(base, status, hhmmss):
    """Copia de un evento del pedido con otro estado y otra hora."""
    e = dict(base[0])
    e["status"] = status
    e["event_ts"] = f"2026-10-21T{hhmmss}Z"
    return e


# ------------------------------------------------------------------ A1
@pytest.mark.parametrize(
    "caso, eventos, ahora, esperado",
    [
        ("A1-01", ORD_123, "14:40:01", [A1]),
        ("A1-02", ORD_123, "14:40:00", []),
        ("A1-03", ORD_123, "14:37:00", []),
        ("A1-04", ORD_123, "14:30:00", []),
        ("A1-05", ORD_123, "14:45:00", []),
        ("A1-06", ORD_124, "14:50:00", []),
        ("A1-07", ORD_126, "14:31:00", [A1, A2]),
    ],
)
def test_a1(caso, eventos, ahora, esperado):
    assert evaluar_alertas(eventos, hora(ahora)) == esperado, caso


def test_a1_08_pedido_cancelado_no_dispara():
    eventos = ORD_126 + [evento_extra(ORD_126, "CANCELADO", "14:20:00")]
    assert evaluar_alertas(eventos, hora("14:35:00")) == []


# ------------------------------------------------------------------ A2
def test_a2_01_detenido_sin_eventos_dispara():
    assert A2 in evaluar_alertas(ORD_126, hora("14:28:01"))


def test_a2_02_justo_10_minutos_no_dispara():
    assert evaluar_alertas(ORD_126, hora("14:28:00")) == []


def test_a2_03_detenido_hace_5_minutos_no_dispara():
    assert evaluar_alertas(ORD_126, hora("14:23:00")) == []


def test_a2_04_latidos_cada_5_minutos_no_dispara():
    assert evaluar_alertas(ORD_124, hora("14:25:00")) == []


def test_a2_05_latidos_hasta_14_40_no_dispara_a2():
    assert A2 not in evaluar_alertas(ORD_123, hora("14:45:00"))


def test_a2_06_ultimo_latido_14_25_y_nada_mas_dispara():
    hasta_1425 = [e for e in ORD_123 if e["event_ts"] <= "2026-10-21T14:25:00Z"]
    assert A2 in evaluar_alertas(hasta_1425, hora("14:35:01"))


def test_a2_07_pedido_entregado_no_dispara():
    assert evaluar_alertas(ORD_124, hora("14:40:00")) == []


def test_a2_08_recogido_sin_eventos_no_dispara_a2():
    recogido = [e for e in ORD_126 if e["status"] in ("CREADO", "ASIGNADO", "RECOGIDO")]
    assert A2 not in evaluar_alertas(recogido, hora("14:40:00"))


def test_a2_09_evento_nuevo_reinicia_el_contador():
    reanuda = ORD_126 + [evento_extra(ORD_126, "EN_RUTA", "14:25:00")]
    assert A2 not in evaluar_alertas(reanuda, hora("14:28:01"))  # 3 min del ultimo
    assert A2 in evaluar_alertas(reanuda, hora("14:35:01"))      # 10 min 1 s


# ------------------------------------------------------------ combinados
def test_ac_01_a1_y_a2_a_la_vez():
    assert evaluar_alertas(ORD_126, hora("14:31:00")) == [A1, A2]


def test_ac_02_a1_sin_a2_si_hay_latido_reciente():
    assert evaluar_alertas(ORD_123, hora("14:40:01")) == [A1]


def test_ac_03_ord_000124_sin_alertas_en_todo_su_ciclo():
    for minuto in range(0, 120):
        ahora = hora("14:05:00") + timedelta(minutes=minuto)
        assert evaluar_alertas(ORD_124, ahora) == [], ahora


def test_sin_eventos_no_hay_alertas():
    assert evaluar_alertas([], hora("14:00:00")) == []
