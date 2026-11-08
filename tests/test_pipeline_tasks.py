"""
Pruebas de la logica del pipeline batch (Semana 3, issue #46).
"""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from airflow.utils import pipeline_tasks as pt  # noqa: E402
from airflow.utils.quality_checks import validar_curated, validar_raw  # noqa: E402


def _eventos():
    filas = [
        # ORD-1: entregado a tiempo (20 min, prometido 30)
        ("E1", "ORD-1", "CREADO", "Norte", 3.0, "2026-10-21T14:00:00Z", "2026-10-21T14:30:00Z", "2026-10-21T14:00:00Z"),
        ("E2", "ORD-1", "ENTREGADO", "Norte", 3.0, "2026-10-21T14:00:00Z", "2026-10-21T14:30:00Z", "2026-10-21T14:20:00Z"),
        # ORD-2: entregado tarde (40 min, prometido 30)
        ("E3", "ORD-2", "CREADO", "Norte", 3.0, "2026-10-21T14:00:00Z", "2026-10-21T14:30:00Z", "2026-10-21T14:00:00Z"),
        ("E4", "ORD-2", "ENTREGADO", "Norte", 3.0, "2026-10-21T14:00:00Z", "2026-10-21T14:30:00Z", "2026-10-21T14:40:00Z"),
        # ORD-3: sigue abierto y ya vencio
        ("E5", "ORD-3", "EN_RUTA", "Sur", 5.0, "2026-10-21T14:00:00Z", "2026-10-21T14:45:00Z", "2026-10-21T14:20:00Z"),
    ]
    df = pd.DataFrame(
        filas,
        columns=["event_id", "order_id", "status", "zone", "distance_km", "created_at", "promised_at", "event_ts"],
    )
    for col in ("created_at", "promised_at", "event_ts"):
        df[col] = pd.to_datetime(df[col], utc=True)
    return df


def test_calcular_kpis_por_zona():
    ahora = pd.Timestamp("2026-10-21T16:00:00Z")
    kpis = {k["zona"]: k for k in pt.calcular_kpis(_eventos(), ahora=ahora)}

    assert kpis["Norte"]["tiempo_promedio"] == 30.0
    assert kpis["Norte"]["porcentaje_fuera_sla"] == 50.0
    assert kpis["Norte"]["pedidos_retrasados"] == 0
    assert kpis["Sur"]["pedidos_retrasados"] == 1
    assert kpis["Sur"]["pedidos_entregados"] == 0


def test_kpis_pasan_validar_curated():
    assert validar_curated(pt.calcular_kpis(_eventos())) is True


def test_eventos_cumplen_contrato_raw():
    assert validar_raw(pt.eventos_a_contrato_raw(_eventos())) is True


def test_contar_parquets_y_lectura(tmp_path):
    ruta = tmp_path / "processed" / "eventos"
    ruta.mkdir(parents=True)
    assert pt.contar_parquets(ruta) == 0
    _eventos().to_parquet(ruta / "part-0.parquet")
    assert pt.contar_parquets(ruta) == 1
    assert len(pt.leer_eventos(tmp_path)) == 5


def test_resumen_alertas_sin_datos(tmp_path):
    assert pt.resumen_alertas(tmp_path) == {}