"""Producer de RapidGo: genera el ciclo de vida de los pedidos y lo publica en Kafka.

Ejemplos (desde la raiz del repo):

  # Publicar 20 pedidos en Kafka (localhost:9092, topic rapidgo.sla.entregas)
  python -m producer.producer --n-pedidos 20 --seed 42

  # Sin Kafka: imprimir los eventos como JSON, uno por linea
  python -m producer.producer --n-pedidos 20 --seed 42 --dry-run

  # Guardar los eventos en un archivo (sirve para probar Spark leyendo archivos)
  python -m producer.producer --n-pedidos 20 --seed 42 --dry-run --salida eventos.jsonl

  # Ritmo "en vivo": 10 minutos simulados por segundo real
  python -m producer.producer --n-pedidos 20 --seed 42 --velocidad 10

La key de cada mensaje es order_id: todos los eventos de un pedido van a la misma
particion.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter

if __package__ in (None, ""):  # ejecutado como "python producer/producer.py"
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.events import generar_eventos, parsear_ts  # noqa: E402
from producer.particiones import analizar_distribucion, texto_distribucion  # noqa: E402

TOPIC_POR_DEFECTO = "rapidgo.sla.entregas"
BOOTSTRAP_POR_DEFECTO = "localhost:9092"  # listener EXTERNAL del docker-compose


def evento_a_json(evento: dict) -> str:
    return json.dumps(evento, ensure_ascii=False, separators=(",", ":"))


def publicar(eventos, productor, topic, velocidad=0.0, dormir=time.sleep):
    """Envia los eventos con key = order_id y devuelve la distribucion por particion.

    ``productor`` es un KafkaProducer (o un objeto con send/flush, util para pruebas).
    ``velocidad`` = minutos simulados por segundo real; 0 = sin pausas.
    """
    futuros = []
    anterior = None
    for evento in eventos:
        ts = parsear_ts(evento["event_ts"])
        if velocidad > 0 and anterior is not None:
            espera = (ts - anterior).total_seconds() / 60 / velocidad
            if espera > 0:
                dormir(espera)
        anterior = ts
        # Se serializa aqui (bytes) para no depender de la version de kafka-python.
        futuro = productor.send(
            topic,
            key=evento["order_id"].encode("utf-8"),
            value=evento_a_json(evento).encode("utf-8"),
        )
        futuros.append((evento["order_id"], futuro))
    productor.flush()
    pares = [(futuro.get(timeout=30).partition, key) for key, futuro in futuros]
    return analizar_distribucion(pares)


def crear_productor(bootstrap):
    try:
        from kafka import KafkaProducer
    except ImportError:
        raise SystemExit("Falta kafka-python: python -m pip install -r requirements.txt")
    return KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=5, linger_ms=20)


def resumen_eventos(eventos) -> str:
    por_estado = Counter(e["status"] for e in eventos)
    pedidos = {e["order_id"] for e in eventos}
    detalle = ", ".join(f"{k}={v}" for k, v in sorted(por_estado.items()))
    return f"{len(eventos)} eventos de {len(pedidos)} pedidos ({detalle})"


def leer_argumentos(argv=None):
    p = argparse.ArgumentParser(description="Producer de eventos de pedidos de RapidGo")
    p.add_argument("--n-pedidos", type=int, default=20, help="cantidad de pedidos (20)")
    p.add_argument("--seed", type=int, default=None, help="semilla para repetir los mismos eventos")
    p.add_argument("--inicio", default=None,
                   help="creacion del primer pedido, ej. 2026-10-21T14:00:00Z (por defecto: ahora)")
    p.add_argument("--p-retrasado", type=float, default=0.25, help="proporcion de pedidos retrasados (0.25)")
    p.add_argument("--p-detenido", type=float, default=0.10, help="proporcion de pedidos detenidos (0.10)")
    p.add_argument("--p-cancelado", type=float, default=0.05, help="proporcion de pedidos cancelados (0.05)")
    p.add_argument("--velocidad", type=float, default=0.0,
                   help="minutos simulados por segundo real; 0 = sin pausas (0)")
    p.add_argument("--bootstrap", default=BOOTSTRAP_POR_DEFECTO, help=f"broker de Kafka ({BOOTSTRAP_POR_DEFECTO})")
    p.add_argument("--topic", default=TOPIC_POR_DEFECTO, help=f"topic ({TOPIC_POR_DEFECTO})")
    p.add_argument("--dry-run", action="store_true", help="no usa Kafka: imprime los eventos en JSON")
    p.add_argument("--salida", default=None, help="archivo .jsonl donde guardar los eventos")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = leer_argumentos(argv)
    inicio = parsear_ts(args.inicio) if args.inicio else None
    try:
        eventos = generar_eventos(
            args.n_pedidos, seed=args.seed, inicio=inicio,
            p_retrasado=args.p_retrasado, p_detenido=args.p_detenido, p_cancelado=args.p_cancelado,
        )
    except ValueError as error:
        print(f"Parametros invalidos: {error}", file=sys.stderr)
        return 2

    if args.salida:
        with open(args.salida, "w", encoding="utf-8", newline="\n") as archivo:
            for evento in eventos:
                archivo.write(evento_a_json(evento) + "\n")
        print(f"Eventos guardados en {args.salida}", file=sys.stderr)

    if args.dry_run:
        for evento in eventos:
            print(evento_a_json(evento))
        print(resumen_eventos(eventos), file=sys.stderr)
        return 0

    try:
        productor = crear_productor(args.bootstrap)
    except Exception as error:  # noqa: BLE001
        print(f"No se pudo conectar con Kafka en {args.bootstrap}: {error}\n"
              "Revisa que Kafka este arriba (docker compose up -d).", file=sys.stderr)
        return 1
    try:
        print(f"Publicando {resumen_eventos(eventos)} en {args.topic} ({args.bootstrap})...", file=sys.stderr)
        resumen = publicar(eventos, productor, args.topic, velocidad=args.velocidad)
    except Exception as error:  # noqa: BLE001 - mostrar un mensaje claro al usuario
        print(f"No se pudo publicar: {error}\n"
              "Revisa que Kafka este arriba (docker compose up -d) y que el topic exista.",
              file=sys.stderr)
        return 1
    finally:
        productor.close()
    print(texto_distribucion(resumen), file=sys.stderr)
    return 0 if resumen["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
