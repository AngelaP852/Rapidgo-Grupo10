"""
Logica del pipeline batch de RapidGo (Variante 1 - SLA de entregas).

Se mantiene separada del DAG para poder probarla con pytest sin Airflow.
Lee el Data Lake que escribe Spark (processed/eventos, curated/alertas).
"""

import os
import socket
from pathlib import Path

import pandas as pd

DATALAKE_PATH = os.environ.get("DATALAKE_PATH", "/opt/airflow/datalake")
KAFKA_HOST = os.environ.get("KAFKA_HOST", "kafka")
KAFKA_PORT = int(os.environ.get("KAFKA_PORT", "29092"))

ESTADOS_CERRADOS = ("ENTREGADO", "CANCELADO")


def kafka_disponible(host=KAFKA_HOST, port=KAFKA_PORT, timeout=5) -> bool:
    """True si el broker acepta conexiones TCP."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def contar_parquets(ruta) -> int:
    """Cantidad de archivos .parquet dentro de una carpeta (0 si no existe)."""
    ruta = Path(ruta)
    if not ruta.exists():
        return 0
    return len(list(ruta.glob("*.parquet")))


def leer_eventos(datalake=DATALAKE_PATH) -> pd.DataFrame:
    """Lee los eventos validos de processed/eventos, con timestamps en UTC."""
    ruta = Path(datalake) / "processed" / "eventos"
    df = pd.read_parquet(ruta)
    for col in ("created_at", "promised_at", "event_ts"):
        df[col] = pd.to_datetime(df[col], utc=True)
    return df


def eventos_a_contrato_raw(df: pd.DataFrame) -> list:
    """Pasa cada evento a los campos que revisa quality_checks.validar_raw."""
    return [
        {
            "pedido": r["order_id"],
            "timestamps": r["event_ts"].isoformat() if pd.notna(r["event_ts"]) else None,
            "zona": r["zone"],
            "distancia": r["distance_km"],
            "estado": r["status"],
        }
        for r in df.to_dict("records")
    ]


def calcular_kpis(df: pd.DataFrame, ahora=None) -> list:
    """
    K1 tiempo promedio de entrega (min, ENTREGADO - created_at) por zona.
    K2 % fuera de SLA (ENTREGADO despues de promised_at / entregados * 100).
    K3 pedidos retrasados (abiertos con ahora > promised_at).
    """
    ahora = ahora if ahora is not None else pd.Timestamp.now(tz="UTC")

    entregados = df[df["status"] == "ENTREGADO"].copy()
    entregados["duracion_min"] = (
        entregados["event_ts"] - entregados["created_at"]
    ).dt.total_seconds() / 60
    entregados["fuera_sla"] = entregados["event_ts"] > entregados["promised_at"]

    ultimo = df.sort_values("event_ts").groupby("order_id").tail(1)
    abiertos = ultimo[~ultimo["status"].isin(ESTADOS_CERRADOS)]
    retrasados = abiertos[abiertos["promised_at"] < ahora]

    resultado = []
    for zona in sorted(df["zone"].dropna().unique()):
        ent = entregados[entregados["zone"] == zona]
        n_ent = len(ent)
        resultado.append(
            {
                "zona": zona,
                "tiempo_promedio": round(float(ent["duracion_min"].mean()), 2) if n_ent else 0.0,
                "porcentaje_fuera_sla": round(float(ent["fuera_sla"].mean()) * 100, 2) if n_ent else 0.0,
                "pedidos_retrasados": int((retrasados["zone"] == zona).sum()),
                "pedidos_entregados": n_ent,
            }
        )
    return resultado


def resumen_alertas(datalake=DATALAKE_PATH) -> dict:
    """Cuenta las alertas A1/A2 de curated/alertas (vacio si todavia no hay)."""
    ruta = Path(datalake) / "curated" / "alertas"
    if contar_parquets(ruta) == 0:
        return {}
    df = pd.read_parquet(ruta)
    if df.empty:
        return {}
    return {k: int(v) for k, v in df.groupby("alerta")["order_id"].nunique().items()}