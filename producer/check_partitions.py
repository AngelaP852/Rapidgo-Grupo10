"""Verifica como quedaron repartidos en las particiones los mensajes de un topic.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date

if __package__ in (None, ""):  # ejecutado como "python producer/check_partitions.py"
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.particiones import analizar_distribucion, texto_distribucion  # noqa: E402


def leer_topic(bootstrap: str, topic: str, timeout_s: float = 20.0):
    """Devuelve [(particion, key_str_o_None), ...] de todos los mensajes del topic."""
    try:
        from kafka import KafkaConsumer, TopicPartition
    except ImportError:
        raise SystemExit("Falta kafka-python: python -m pip install -r requirements.txt")

    consumidor = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False)
    try:
        particiones = consumidor.partitions_for_topic(topic)
        if not particiones:
            raise SystemExit(f"No existe el topic {topic!r} en {bootstrap}.")
        tps = [TopicPartition(topic, p) for p in sorted(particiones)]
        consumidor.assign(tps)
        consumidor.seek_to_beginning(*tps)
        fin = consumidor.end_offsets(tps)

        pares = []
        limite = time.monotonic() + timeout_s
        while time.monotonic() < limite:
            if all(consumidor.position(tp) >= fin[tp] for tp in tps):
                break
            for tp, mensajes in consumidor.poll(timeout_ms=1000).items():
                for m in mensajes:
                    key = m.key.decode("utf-8") if m.key else None
                    pares.append((m.partition, key))
        else:
            print("Aviso: se agoto el tiempo antes de leer todo el topic.", file=sys.stderr)
        return pares, len(tps)
    finally:
        consumidor.close()


def markdown_adr(resumen: dict, topic: str, n_particiones: int, nota: str = "") -> str:
    """Texto para la seccion 6 del ADR-001 con los numeros reales medidos."""
    total = resumen["total"] or 1
    filas = []
    for p, n in resumen["por_particion"].items():
        pedidos = resumen["pedidos_por_particion"].get(p, 0)
        filas.append(f"| {p} | {n} | {pedidos} | {100 * n / total:.1f} % |")
    detalle = f" con {nota}" if nota else ""
    if resumen["ok"]:
        conclusion = ("Ningun pedido tiene mensajes en mas de una particion: "
                      "la key `order_id` mantiene juntos los eventos de cada pedido.")
    else:
        repartidos = ", ".join(resumen["pedidos_en_varias"][:10])
        conclusion = f"HAY pedidos repartidos en varias particiones ({repartidos}). Revisar la key."
    mayor = max(resumen["por_particion"].values(), default=0)
    return "\n".join([
        "## 6. Estado de la evidencia",
        "",
        f"Medicion realizada el {date.today().isoformat()} sobre el topic `{topic}` "
        f"({n_particiones} particiones){detalle}.",
        "",
        "| Particion | Mensajes | Pedidos distintos | % de mensajes |",
        "|---|---|---|---|",
        *filas,
        "",
        f"- Total leido: {resumen['total']} mensajes de {resumen['pedidos']} pedidos.",
        f"- Mensajes sin key: {resumen['sin_key']}.",
        f"- La particion con mas mensajes concentra el {100 * mayor / total:.1f} % del total.",
        f"- {conclusion}",
        "",
        "Esta medicion comprueba la asignacion de particiones por key; no es una prueba de "
        "rendimiento. Con pocos pedidos el reparto entre particiones puede quedar desigual "
        "por azar.",
    ])


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Verifica la distribucion por particion de un topic")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="rapidgo.sla.entregas")
    p.add_argument("--timeout", type=float, default=20.0, help="segundos maximos de lectura (20)")
    p.add_argument("--markdown", action="store_true", help="imprime la seccion 6 del ADR-001")
    p.add_argument("--nota", default="", help='dato extra para el ADR, ej. "20 pedidos, --seed 42"')
    args = p.parse_args(argv)

    pares, n_particiones = leer_topic(args.bootstrap, args.topic, args.timeout)
    resumen = analizar_distribucion(pares)
    if args.markdown:
        print(markdown_adr(resumen, args.topic, n_particiones, args.nota))
    else:
        print(texto_distribucion(resumen))
    return 0 if resumen["ok"] and resumen["total"] > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
