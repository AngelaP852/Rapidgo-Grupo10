"""Pruebas ampliadas de K1-K3 y de la calidad de datos (issue #51)."""
import sys
import os

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from airflow.utils import pipeline_tasks as pt  # noqa: E402
from airflow.utils.quality_checks import validar_curated, validar_raw  # noqa: E402

COLUMNAS = ["event_id", "order_id", "status", "zone", "distance_km",
            "created_at", "promised_at", "event_ts"]


def df(filas):
    d = pd.DataFrame(filas, columns=COLUMNAS)
    for c in ("created_at", "promised_at", "event_ts"):
        d[c] = pd.to_datetime(d[c], utc=True)
    return d


def fila(eid, oid, estado, zona, creado, prometido, evento):
    return (eid, oid, estado, zona, 3.0,
            f"2026-10-21T{creado}:00Z", f"2026-10-21T{prometido}:00Z", f"2026-10-21T{evento}:00Z")


AHORA = pd.Timestamp("2026-10-21T16:00:00Z")


def kpis_por_zona(datos, ahora=AHORA):
    return {k["zona"]: k for k in pt.calcular_kpis(datos, ahora=ahora)}


def test_k1_es_entrega_menos_creacion():
    datos = df([fila("1", "A", "ENTREGADO", "Norte", "14:00", "14:30", "14:20")])
    assert kpis_por_zona(datos)["Norte"]["tiempo_promedio"] == 20.0


def test_entrega_justo_en_promised_at_no_es_fuera_de_sla():
    datos = df([fila("1", "A", "ENTREGADO", "Norte", "14:00", "14:30", "14:30")])
    assert kpis_por_zona(datos)["Norte"]["porcentaje_fuera_sla"] == 0.0


def test_todas_fuera_de_sla_da_100():
    datos = df([fila("1", "A", "ENTREGADO", "Norte", "14:00", "14:30", "14:45")])
    assert kpis_por_zona(datos)["Norte"]["porcentaje_fuera_sla"] == 100.0


def test_k3_no_cuenta_cancelados():
    datos = df([fila("1", "A", "CANCELADO", "Sur", "14:00", "14:30", "14:10")])
    assert kpis_por_zona(datos)["Sur"]["pedidos_retrasados"] == 0


def test_k3_pedido_abierto_aun_no_vencido_no_cuenta():
    datos = df([fila("1", "A", "EN_RUTA", "Sur", "14:00", "14:30", "14:10")])
    antes = pd.Timestamp("2026-10-21T14:20:00Z")
    assert kpis_por_zona(datos, ahora=antes)["Sur"]["pedidos_retrasados"] == 0


def test_k3_usa_el_ultimo_estado_del_pedido():
    datos = df([
        fila("1", "A", "EN_RUTA", "Sur", "14:00", "14:30", "14:10"),
        fila("2", "A", "ENTREGADO", "Sur", "14:00", "14:30", "14:25"),
    ])
    assert kpis_por_zona(datos)["Sur"]["pedidos_retrasados"] == 0


def test_zona_sin_entregas_da_ceros():
    datos = df([fila("1", "A", "EN_RUTA", "Sur", "14:00", "14:30", "14:10")])
    k = kpis_por_zona(datos)["Sur"]
    assert k["tiempo_promedio"] == 0.0 and k["porcentaje_fuera_sla"] == 0.0


def test_las_zonas_salen_ordenadas():
    datos = df([
        fila("1", "A", "ENTREGADO", "Sur", "14:00", "14:30", "14:20"),
        fila("2", "B", "ENTREGADO", "Norte", "14:00", "14:30", "14:20"),
    ])
    assert [k["zona"] for k in pt.calcular_kpis(datos, ahora=AHORA)] == ["Norte", "Sur"]


def test_validar_raw_rechaza_campo_nulo():
    registro = {"pedido": "A", "timestamps": "2026-10-21T14:00:00Z",
                "zona": "Norte", "distancia": None, "estado": "ENTREGADO"}
    assert validar_raw([registro]) is False


def test_validar_raw_rechaza_si_no_es_lista():
    assert validar_raw({"pedido": "A"}) is False


def test_validar_curated_rechaza_tiempo_negativo():
    assert validar_curated([{"zona": "Norte", "tiempo_promedio": -1.0}]) is False


def test_validar_curated_acepta_limites_0_y_100():
    assert validar_curated([{"tiempo_promedio": 0.0, "porcentaje_fuera_sla": 0.0}]) is True
    assert validar_curated([{"tiempo_promedio": 5.0, "porcentaje_fuera_sla": 100.0}]) is True
