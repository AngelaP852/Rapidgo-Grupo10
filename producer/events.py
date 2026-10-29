"""Generacion de eventos de pedidos segun docs/contrato_evento.md.

No depende de Kafka ni de ninguna libreria externa: solo genera diccionarios.
Con la misma ``seed`` y el mismo ``inicio`` se obtienen siempre los mismos eventos.

Perfiles de pedido:
  NORMAL     -> se entrega antes de promised_at.
  RETRASADO  -> se entrega despues de promised_at (dispara K3 y A1).
  DETENIDO   -> emite latidos EN_RUTA, pasa a DETENIDO y deja de emitir (dispara A2).
  CANCELADO  -> termina en CANCELADO antes de salir a ruta.
"""
from __future__ import annotations

import math
import random
from datetime import datetime, timedelta, timezone

TEAM_ID = "equipo-v1"
TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
ZONAS = ("Norte", "Sur", "Centro", "Oriente", "Occidente")
ESTADOS = ("CREADO", "ASIGNADO", "RECOGIDO", "EN_RUTA", "ENTREGADO", "DETENIDO", "CANCELADO")
LATIDO_MIN = 5  # un evento EN_RUTA cada 5 minutos mientras el pedido esta en ruta

PERFIL_NORMAL = "NORMAL"
PERFIL_RETRASADO = "RETRASADO"
PERFIL_DETENIDO = "DETENIDO"
PERFIL_CANCELADO = "CANCELADO"


# --------------------------------------------------------------------------- tiempo
def formatear_ts(dt: datetime) -> str:
    """datetime -> '2026-10-21T14:00:00Z' (siempre en UTC)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime(TS_FORMAT)


def parsear_ts(texto: str) -> datetime:
    """'2026-10-21T14:00:00Z' -> datetime con zona UTC."""
    return datetime.strptime(texto, TS_FORMAT).replace(tzinfo=timezone.utc)


# --------------------------------------------------------------------------- SLA
def minutos_sla(distance_km: float) -> int:
    """Minutos prometidos: 10 + 8 x km, redondeado al multiplo de 5 mas cercano.

    Las mitades se redondean hacia arriba (22.5 -> 25), no al par como hace round().
    """
    if distance_km <= 0:
        raise ValueError("distance_km debe ser mayor que 0")
    cuantos_de_5 = (10 + 8 * distance_km) / 5
    return int(math.floor(round(cuantos_de_5, 6) + 0.5)) * 5


def calcular_promised_at(created_at: datetime, distance_km: float) -> datetime:
    """Hora prometida de entrega: created_at + SLA."""
    return created_at + timedelta(minutes=minutos_sla(distance_km))


# --------------------------------------------------------------------------- evento
def construir_evento(
    *,
    event_id: str,
    order_id: str,
    status: str,
    zone: str,
    distance_km: float,
    created_at: datetime,
    event_ts: datetime,
    promised_at: datetime | None = None,
    team_id: str = TEAM_ID,
) -> dict:
    """Arma un evento con los 9 campos del contrato.

    Si no se pasa ``promised_at`` se calcula con la formula del SLA.
    """
    if status not in ESTADOS:
        raise ValueError(f"status invalido: {status!r}")
    if distance_km <= 0:
        raise ValueError("distance_km debe ser mayor que 0")
    if promised_at is None:
        promised_at = calcular_promised_at(created_at, distance_km)
    return {
        "event_id": event_id,
        "team_id": team_id,
        "order_id": order_id,
        "status": status,
        "zone": zone,
        "distance_km": distance_km,
        "created_at": formatear_ts(created_at),
        "promised_at": formatear_ts(promised_at),
        "event_ts": formatear_ts(event_ts),
    }


# --------------------------------------------------------------------------- ciclo de vida
def _min(n: int) -> timedelta:
    return timedelta(minutes=n)


def _linea_de_tiempo(rng, perfil, created_at, distance_km, promised_at):
    """Devuelve [(estado, datetime), ...] de un pedido segun su perfil."""
    t = created_at
    pasos = [("CREADO", t)]

    if perfil == PERFIL_CANCELADO:
        if rng.random() < 0.5:
            t += _min(rng.randint(1, 3))
            pasos.append(("ASIGNADO", t))
        t += _min(rng.randint(1, 5))
        pasos.append(("CANCELADO", t))
        return pasos

    t += _min(rng.randint(1, 2))
    pasos.append(("ASIGNADO", t))
    t += _min(rng.randint(2, 4))
    pasos.append(("RECOGIDO", t))
    t += _min(rng.randint(1, 2))
    en_ruta = t
    pasos.append(("EN_RUTA", en_ruta))

    if perfil == PERFIL_DETENIDO:
        latidos = rng.randint(1, 3)
        ultimo = en_ruta
        for k in range(1, latidos + 1):
            ultimo = en_ruta + _min(LATIDO_MIN * k)
            pasos.append(("EN_RUTA", ultimo))
        pasos.append(("DETENIDO", ultimo + _min(rng.randint(1, 4))))
        return pasos  # no hay mas eventos: el pedido queda detenido

    viaje_min = max(2, int(round(distance_km * rng.uniform(2.5, 4.5))))
    entrega = en_ruta + _min(viaje_min)
    if perfil == PERFIL_RETRASADO:
        entrega = max(entrega, promised_at + _min(rng.randint(3, 20)))
    else:  # NORMAL: entrega antes de la hora prometida
        entrega = min(entrega, promised_at - _min(1))
        entrega = max(entrega, en_ruta + _min(2))

    k = 1
    while en_ruta + _min(LATIDO_MIN * k) < entrega:
        pasos.append(("EN_RUTA", en_ruta + _min(LATIDO_MIN * k)))
        k += 1
    pasos.append(("ENTREGADO", entrega))
    return pasos


def _elegir_perfil(rng, p_retrasado, p_detenido, p_cancelado):
    r = rng.random()
    if r < p_cancelado:
        return PERFIL_CANCELADO
    r -= p_cancelado
    if r < p_detenido:
        return PERFIL_DETENIDO
    r -= p_detenido
    if r < p_retrasado:
        return PERFIL_RETRASADO
    return PERFIL_NORMAL


def generar_eventos(
    n_pedidos: int,
    seed: int | None = None,
    inicio: datetime | None = None,
    p_retrasado: float = 0.25,
    p_detenido: float = 0.10,
    p_cancelado: float = 0.05,
    primer_pedido: int = 1,
    primer_evento: int = 1,
) -> list[dict]:
    """Genera los eventos de ``n_pedidos`` pedidos, ordenados por event_ts.

    Los pedidos se crean con 0 a 2 minutos de diferencia a partir de ``inicio``.
    Los ids siguen el formato de los samples: ORD-000125 y EVT-000100.
    """
    if n_pedidos < 0:
        raise ValueError("n_pedidos no puede ser negativo")
    for nombre, p in (("p_retrasado", p_retrasado), ("p_detenido", p_detenido),
                      ("p_cancelado", p_cancelado)):
        if not 0 <= p <= 1:
            raise ValueError(f"{nombre} debe estar entre 0 y 1")
    if p_retrasado + p_detenido + p_cancelado > 1:
        raise ValueError("las probabilidades de los perfiles no pueden sumar mas de 1")

    rng = random.Random(seed)
    if inicio is None:
        inicio = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    elif inicio.tzinfo is None:
        inicio = inicio.replace(tzinfo=timezone.utc)

    crudos = []
    t_creacion = inicio
    for i in range(n_pedidos):
        numero = primer_pedido + i
        if i > 0:
            t_creacion += _min(rng.randint(0, 2))
        zona = rng.choice(ZONAS)
        distancia = round(rng.uniform(0.5, 12.0), 1)
        perfil = _elegir_perfil(rng, p_retrasado, p_detenido, p_cancelado)
        promised_at = calcular_promised_at(t_creacion, distancia)
        pasos = _linea_de_tiempo(rng, perfil, t_creacion, distancia, promised_at)
        for secuencia, (estado, ts) in enumerate(pasos):
            crudos.append((ts, numero, secuencia, estado, zona, distancia, t_creacion, promised_at))

    crudos.sort(key=lambda c: (c[0], c[1], c[2]))
    eventos = []
    for indice, (ts, numero, _sec, estado, zona, distancia, creado, prometido) in enumerate(crudos):
        eventos.append(construir_evento(
            event_id=f"EVT-{primer_evento + indice:06d}",
            order_id=f"ORD-{numero:06d}",
            status=estado,
            zone=zona,
            distance_km=distancia,
            created_at=creado,
            event_ts=ts,
            promised_at=prometido,
        ))
    return eventos
