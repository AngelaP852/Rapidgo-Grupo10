"""Reglas de las alertas A1 y A2 como funcion pura (docs/kpis_y_alertas.md).

A1 SLA_EXCEDIDO:      pedido abierto y ahora > promised_at + 5 min.
A2 ENTREGA_DETENIDA:  pedido abierto, ultimo estado EN_RUTA o DETENIDO y mas de
                      10 min sin eventos nuevos.
Un pedido esta abierto hasta que recibe ENTREGADO o CANCELADO.
Los limites son estrictos (>): justo en el umbral no se dispara.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

A1 = "A1_SLA_EXCEDIDO"
A2 = "A2_ENTREGA_DETENIDA"
TOLERANCIA_A1 = timedelta(minutes=5)
SILENCIO_A2 = timedelta(minutes=10)
ESTADOS_CERRADOS = ("ENTREGADO", "CANCELADO")
ESTADOS_EN_MOVIMIENTO = ("EN_RUTA", "DETENIDO")
_FORMATO = "%Y-%m-%dT%H:%M:%SZ"


def a_fecha(texto: str) -> datetime:
    """'2026-10-21T14:00:00Z' -> datetime en UTC."""
    return datetime.strptime(texto, _FORMATO).replace(tzinfo=timezone.utc)


def evaluar_alertas(eventos: list, ahora: datetime) -> list:
    """Alertas que se disparan para UN pedido en el instante ``ahora``.

    ``eventos`` son los eventos del pedido (diccionarios del contrato). Solo se
    consideran los que ya ocurrieron (event_ts <= ahora). Devuelve una lista
    ordenada con los nombres de las alertas (vacia si no se dispara ninguna).
    """
    vistos = [e for e in eventos if a_fecha(e["event_ts"]) <= ahora]
    if not vistos:
        return []
    vistos.sort(key=lambda e: a_fecha(e["event_ts"]))
    ultimo = vistos[-1]

    if any(e["status"] in ESTADOS_CERRADOS for e in vistos):
        return []  # pedido cerrado: no hay alertas

    alertas = []
    if ahora > a_fecha(ultimo["promised_at"]) + TOLERANCIA_A1:
        alertas.append(A1)
    if (
        ultimo["status"] in ESTADOS_EN_MOVIMIENTO
        and ahora - a_fecha(ultimo["event_ts"]) > SILENCIO_A2
    ):
        alertas.append(A2)
    return sorted(alertas)
