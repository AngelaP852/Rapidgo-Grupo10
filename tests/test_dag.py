"""Revisa el DAG de Airflow sin necesitar Airflow instalado (issue #50).

Lee el archivo con ast: comprueba que compila, que tiene el dag_id esperado,
las 5 tareas y el orden de dependencias. No ejecuta el DAG.
"""
import ast
from pathlib import Path

RUTA = Path(__file__).resolve().parent.parent / "airflow" / "dags" / "rapidgo_sla_dag.py"
TAREAS = ["check_fuentes", "validar_raw", "agregar", "validar_curated", "reporte"]


def _arbol():
    return ast.parse(RUTA.read_text(encoding="utf-8"))


def test_el_dag_compila():
    compile(RUTA.read_text(encoding="utf-8"), str(RUTA), "exec")


def test_dag_id_esperado():
    assert "rapidgo_sla_pipeline_semana3" in RUTA.read_text(encoding="utf-8")


def test_tiene_las_5_tareas():
    ids = []
    for nodo in ast.walk(_arbol()):
        if isinstance(nodo, ast.Call) and getattr(nodo.func, "id", "") == "PythonOperator":
            for kw in nodo.keywords:
                if kw.arg == "task_id":
                    ids.append(kw.value.value)
    assert sorted(ids) == sorted(TAREAS)


def test_las_tareas_van_en_orden():
    texto = RUTA.read_text(encoding="utf-8")
    assert "t_check >> t_raw >> t_agregar >> t_curated >> t_reporte" in texto


def test_reintentos_configurados():
    texto = RUTA.read_text(encoding="utf-8")
    assert '"retries": 2' in texto
