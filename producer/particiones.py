"""Analisis de como se reparten los mensajes entre las particiones de Kafka.

Es logica pura (sin Kafka): la usan producer.py y check_partitions.py.
Entrada: pares (particion, key). La key del mensaje es el order_id.
"""
from __future__ import annotations

from collections import defaultdict


def analizar_distribucion(pares) -> dict:
    """Cuenta mensajes y pedidos por particion y detecta pedidos repartidos.

    Devuelve:
      total, sin_key, pedidos (cantidad de order_id distintos),
      por_particion:           {particion: n_mensajes}
      pedidos_por_particion:   {particion: n_pedidos_distintos}
      pedidos_en_varias:       [order_id, ...] con mensajes en mas de una particion
      ok:                      True si ningun pedido esta en mas de una particion
    """
    por_particion: dict = defaultdict(int)
    pedidos_en: dict = defaultdict(set)  # order_id -> {particiones}
    sin_key = 0
    total = 0
    for particion, key in pares:
        total += 1
        por_particion[particion] += 1
        if key is None:
            sin_key += 1
        else:
            pedidos_en[key].add(particion)

    pedidos_por_particion: dict = defaultdict(int)
    for particiones in pedidos_en.values():
        for p in particiones:
            pedidos_por_particion[p] += 1
    repartidos = sorted(k for k, ps in pedidos_en.items() if len(ps) > 1)
    return {
        "total": total,
        "sin_key": sin_key,
        "pedidos": len(pedidos_en),
        "por_particion": dict(sorted(por_particion.items())),
        "pedidos_por_particion": dict(sorted(pedidos_por_particion.items())),
        "pedidos_en_varias": repartidos,
        "ok": not repartidos,
    }


def texto_distribucion(resumen: dict) -> str:
    """Tabla en texto plano para imprimir en la terminal."""
    total = resumen["total"] or 1
    lineas = ["particion  mensajes  pedidos  % mensajes"]
    for p, n in resumen["por_particion"].items():
        pedidos = resumen["pedidos_por_particion"].get(p, 0)
        lineas.append(f"{p:>9}  {n:>8}  {pedidos:>7}  {100 * n / total:>9.1f}")
    lineas.append(
        f"total: {resumen['total']} mensajes, {resumen['pedidos']} pedidos, {resumen['sin_key']} sin key"
    )
    if resumen["ok"]:
        lineas.append("OK: cada order_id esta en una sola particion.")
    else:
        lineas.append(
            "ERROR: pedidos en mas de una particion: "
            + ", ".join(resumen["pedidos_en_varias"][:10])
        )
    return "\n".join(lineas)
