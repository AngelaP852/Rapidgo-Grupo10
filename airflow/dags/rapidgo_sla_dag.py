"""
DAG de Airflow para RapidGo - Variante 1: SLA de Entregas (Semana 3, Issues #46 y #49)

check_fuentes -> validar_raw -> agregar -> validar_curated -> reporte

1. check_fuentes:   Kafka disponible y Data Lake con datos.
2. validar_raw:     valida los eventos con quality_checks.validar_raw.
3. agregar:         calcula K1, K2 y K3 por zona.
4. validar_curated: valida los KPIs con quality_checks.validar_curated.
5. reporte:         resumen de K1-K3 y alertas A1/A2.

Retries: default_args (2 reintentos, 1 minuto de espera).
XCom: validar_raw publica "conteo_eventos"; validar_curated y reporte lo leen.
Fallo controlado: con la Variable de Airflow "rapidgo_forzar_fallo" = "1",
check_fuentes falla en el primer intento para observar el reintento.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# El compose monta ./airflow/utils en /opt/airflow/utils
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.quality_checks import validar_curated, validar_raw  # noqa: E402
from utils import pipeline_tasks as pt  # noqa: E402

default_args = {
    "owner": "equipo-10-rapidgo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def check_fuentes(ti):
    """Tarea 1: Kafka responde y processed/eventos tiene archivos."""
    # Fallo controlado para la demo de retries (issue #49): solo si la Variable esta en "1"
    if Variable.get("rapidgo_forzar_fallo", default_var="0") == "1" and ti.try_number == 1:
        raise RuntimeError("Fallo controlado: primer intento forzado por la Variable rapidgo_forzar_fallo.")

    if not pt.kafka_disponible():
        raise RuntimeError(f"Kafka no responde en {pt.KAFKA_HOST}:{pt.KAFKA_PORT}")

    ruta = os.path.join(pt.DATALAKE_PATH, "processed", "eventos")
    n_archivos = pt.contar_parquets(ruta)
    if n_archivos == 0:
        raise RuntimeError(f"No hay datos en {ruta}. Publica eventos con el producer.")

    print(f"[check_fuentes] Kafka OK. Archivos parquet en processed/eventos: {n_archivos}")
    return {"kafka": True, "archivos_parquet": n_archivos}


def tarea_validar_raw(ti):
    """Tarea 2: valida el contrato de los eventos leidos del Data Lake."""
    registros = pt.eventos_a_contrato_raw(pt.leer_eventos())
    if not validar_raw(registros):
        raise ValueError("Los eventos no cumplen el contrato de la Variante 1.")
    print(f"[validar_raw] {len(registros)} eventos validos.")
    ti.xcom_push(key="conteo_eventos", value=len(registros))


def agregar():
    """Tarea 3: calcula K1, K2 y K3 por zona. El resultado viaja por XCom."""
    kpis = pt.calcular_kpis(pt.leer_eventos())
    if not kpis:
        raise ValueError("No se pudo calcular ningun KPI: no hay zonas en los datos.")
    print(f"[agregar] KPIs calculados para {len(kpis)} zonas.")
    return kpis


def tarea_validar_curated(ti):
    """Tarea 4: valida los KPIs que dejo la tarea agregar."""
    kpis = ti.xcom_pull(task_ids="agregar")
    conteo = ti.xcom_pull(task_ids="validar_raw", key="conteo_eventos")
    if not conteo:
        raise ValueError("No llego el conteo de eventos de validar_raw por XCom.")
    if not validar_curated(kpis):
        raise ValueError("Los KPIs de SLA tienen valores inconsistentes.")
    print(f"[validar_curated] KPIs consistentes (calculados sobre {conteo} eventos).")


def reporte(ti):
    """Tarea 5: imprime el resumen de K1-K3 y de alertas."""
    kpis = ti.xcom_pull(task_ids="agregar")
    conteo = ti.xcom_pull(task_ids="validar_raw", key="conteo_eventos")
    print("===== REPORTE DE SLA - RAPIDGO (Variante 1) =====")
    print(f"Eventos validados en esta corrida: {conteo}")
    for k in kpis:
        print(
            f"Zona {k['zona']}: K1 tiempo promedio = {k['tiempo_promedio']} min | "
            f"K2 fuera de SLA = {k['porcentaje_fuera_sla']}% | "
            f"K3 pedidos retrasados = {k['pedidos_retrasados']} "
            f"(entregados: {k['pedidos_entregados']})"
        )
        if k["porcentaje_fuera_sla"] > 20.0:
            print(f"  ALERTA: SLA excedido en la zona {k['zona']}")

    alertas = pt.resumen_alertas()
    if alertas:
        for nombre, cantidad in alertas.items():
            print(f"Alertas {nombre}: {cantidad} pedidos")
    else:
        print("Alertas: ninguna registrada en curated/alertas.")
    print("===== FIN DEL REPORTE =====")


with DAG(
    dag_id="rapidgo_sla_pipeline_semana3",
    default_args=default_args,
    description="Orquestacion batch de la Variante 1 - SLA de entregas (Semana 3)",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["rapidgo", "semana3", "variante1", "sla"],
) as dag:

    t_check = PythonOperator(task_id="check_fuentes", python_callable=check_fuentes)
    t_raw = PythonOperator(task_id="validar_raw", python_callable=tarea_validar_raw)
    t_agregar = PythonOperator(task_id="agregar", python_callable=agregar)
    t_curated = PythonOperator(task_id="validar_curated", python_callable=tarea_validar_curated)
    t_reporte = PythonOperator(task_id="reporte", python_callable=reporte)

    t_check >> t_raw >> t_agregar >> t_curated >> t_reporte