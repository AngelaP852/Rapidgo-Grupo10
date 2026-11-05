"""
Pruebas unitarias para las funciones de calidad de la Semana 3 (Variante 1).
"""

import pytest
from airflow.utils.quality_checks import validar_raw, validar_curated

def test_validar_raw_exitoso():
    datos_validos = [
        {
            "pedido": "ORD-1001",
            "timestamps": "2026-11-05T10:00:00Z",
            "zona": "Zona Norte",
            "distancia": 4.5,
            "estado": "ENTREGADO"
        }
    ]
    assert validar_raw(datos_validos) is True

def test_validar_raw_fallido():
    datos_incompletos = [{"pedido": "ORD-1002"}]
    assert validar_raw(datos_incompletos) is False

def test_validar_curated_exitoso():
    datos_curated = [
        {
            "zona": "Zona Norte",
            "tiempo_promedio": 25.4,
            "porcentaje_fuera_sla": 12.5
        }
    ]
    assert validar_curated(datos_curated) is True

def test_validar_curated_fallido():
    datos_curated_mal = [{"zona": "Zona Sur", "porcentaje_fuera_sla": 150.0}]
    assert validar_curated(datos_curated_mal) is False