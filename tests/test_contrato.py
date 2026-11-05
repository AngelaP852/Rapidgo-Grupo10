"""Tests del contrato del evento (docs/contrato_evento.md) sobre los samples reales."""
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parent.parent
SAMPLES = RAIZ / "samples"

CAMPOS = [
    "event_id", "team_id", "order_id", "status", "zone",
    "distance_km", "created_at", "promised_at", "event_ts",
]
ESTADOS = {
    "CREADO", "ASIGNADO", "RECOGIDO", "EN_RUTA",
    "ENTREGADO", "DETENIDO", "CANCELADO",
}
CAMPOS_FECHA = ["created_at", "promised_at", "event_ts"]


def cargar_eventos():
    """Lee los 3 samples: uno es JSON suelto y dos son JSON Lines."""
    eventos = [json.loads((SAMPLES / "eventos_pedido.json").read_text(encoding="utf-8"))]
    for nombre in ("eventos_ciclo_vida.jsonl", "eventos_entrega_detenida.jsonl"):
        for linea in (SAMPLES / nombre).read_text(encoding="utf-8").splitlines():
            if linea.strip():
                eventos.append(json.loads(linea))
    return eventos


def a_fecha(texto):
    return datetime.strptime(texto, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def promised_esperado(created_at, distance_km):
    """created_at + 10 min + 8 min x km, redondeado al múltiplo de 5 más cercano."""
    minutos = 10 + 8 * distance_km
    redondeado = math.floor(minutos / 5 + 0.5) * 5
    return created_at + timedelta(minutes=redondeado)


def validar_evento(e):
    """Devuelve la lista de errores del evento (vacía si cumple el contrato)."""
    errores = []
    for campo in CAMPOS:
        if campo not in e or e[campo] in (None, ""):
            errores.append(f"falta {campo}")
    if errores:
        return errores
    if not isinstance(e["distance_km"], (int, float)) or isinstance(e["distance_km"], bool):
        errores.append("distance_km no es numérico")
    elif e["distance_km"] <= 0:
        errores.append("distance_km debe ser > 0")
    if e["status"] not in ESTADOS:
        errores.append("status no permitido")
    try:
        c, p, t = (a_fecha(e[k]) for k in CAMPOS_FECHA)
    except (ValueError, TypeError):
        errores.append("fecha no es ISO 8601 UTC con Z")
        return errores
    if t < c:
        errores.append("event_ts < created_at")
    if p <= c:
        errores.append("promised_at debe ser > created_at")
    return errores


EVENTOS = cargar_eventos()


def test_hay_23_eventos_en_los_samples():
    assert len(EVENTOS) == 23


def test_event_id_es_unico():
    ids = [e["event_id"] for e in EVENTOS]
    assert len(ids) == len(set(ids))


@pytest.mark.parametrize("evento", EVENTOS, ids=lambda e: e["event_id"])
def test_cada_evento_cumple_el_contrato(evento):
    assert validar_evento(evento) == []


@pytest.mark.parametrize("evento", EVENTOS, ids=lambda e: e["event_id"])
def test_promised_at_sigue_la_formula_del_sla(evento):
    esperado = promised_esperado(a_fecha(evento["created_at"]), evento["distance_km"])
    assert a_fecha(evento["promised_at"]) == esperado


@pytest.mark.parametrize("evento", EVENTOS, ids=lambda e: e["event_id"])
def test_team_id_es_equipo_v1(evento):
    assert evento["team_id"] == "equipo-v1"


# --- Eventos que rompen el contrato: deben fallar (comprobado a propósito) ---
def _base():
    return dict(EVENTOS[0])


def test_falta_un_campo_falla():
    e = _base()
    del e["zone"]
    assert "falta zone" in validar_evento(e)


def test_distancia_cero_falla():
    e = _base()
    e["distance_km"] = 0
    assert "distance_km debe ser > 0" in validar_evento(e)


def test_estado_no_permitido_falla():
    e = _base()
    e["status"] = "PERDIDO"
    assert "status no permitido" in validar_evento(e)


def test_event_ts_anterior_a_created_at_falla():
    e = _base()
    e["event_ts"] = "2000-01-01T00:00:00Z"
    assert "event_ts < created_at" in validar_evento(e)


def test_promised_at_no_posterior_a_created_at_falla():
    e = _base()
    e["promised_at"] = e["created_at"]
    assert "promised_at debe ser > created_at" in validar_evento(e)


def test_fecha_sin_zona_utc_falla():
    e = _base()
    e["event_ts"] = "2026-10-21T14:20:00"
    assert "fecha no es ISO 8601 UTC con Z" in validar_evento(e)


# --- Fórmula del SLA: caso normal, límite y fuera de SLA ---
@pytest.mark.parametrize(
    "km, minutos",
    [(1.8, 25), (2.0, 25), (3.2, 35), (4.5, 45)],
)
def test_tabla_de_kpis_y_alertas(km, minutos):
    c = a_fecha("2026-10-21T14:00:00Z")
    assert promised_esperado(c, km) == c + timedelta(minutes=minutos)


def test_formula_limite_justo_en_multiplo_de_5():
    # 10 + 8 x 2.5 = 30 min exactos: no hay que redondear
    c = a_fecha("2026-10-21T14:00:00Z")
    assert promised_esperado(c, 2.5) == c + timedelta(minutes=30)


def test_entrega_fuera_de_sla_ord_000123():
    entregado = next(e for e in EVENTOS if e["order_id"] == "ORD-000123" and e["status"] == "ENTREGADO")
    assert a_fecha(entregado["event_ts"]) > a_fecha(entregado["promised_at"])


def test_entrega_dentro_de_sla_ord_000124():
    entregado = next(e for e in EVENTOS if e["order_id"] == "ORD-000124" and e["status"] == "ENTREGADO")
    assert a_fecha(entregado["event_ts"]) <= a_fecha(entregado["promised_at"])
