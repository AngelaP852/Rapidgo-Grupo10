"""
DAG de Airflow para RapidGo - Variante 1: SLA de Entregas
Orquestador batch para la Semana 3 (Issue #48):
1. Validar fuentes / Raw
2. Procesar job batch (Raw -> Curated con KPIs de SLA)
3. Validar calidad Curated
4. Generar reporte de SLA y alertas
5. Finalizar pipeline
"""

import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Asegurar que Airflow encuentre el módulo de utils localmente
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.quality_checks import validar_raw, validar_curated

# Configuración por defecto del DAG (incluye retries exigidos por la guía)
default_args = {
    'owner': 'equipo-10-rapidgo',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def task_validar_fuentes(**kwargs):
    """Tarea 1: Valida la integridad de los datos en la capa Raw."""
    datos_raw = [
        {"pedido": "ORD-501", "timestamps": "2026-11-05T08:30:00Z", "zona": "Norte", "distancia": 3.2, "estado": "ENTREGADO"},
        {"pedido": "ORD-502", "timestamps": "2026-11-05T09:15:00Z", "zona": "Sur", "distancia": 7.5, "estado": "RETRASADO"}
    ]
    
    valido = validar_raw(datos_raw)
    if not valido:
        raise ValueError("Fallo crítico: Los datos en la capa Raw no cumplen con el contrato de la Variante 1.")
    
    kwargs['ti'].xcom_push(key='raw_data', value=datos_raw)
    print("Tarea 1 [validar_raw] completada con éxito.")

def task_procesar_batch(**kwargs):
    """Tarea 2: Job batch que transforma Raw a Curated calculando KPIs de SLA."""
    ti = kwargs['ti']
    datos_raw = ti.xcom_pull(task_ids='validar_fuentes_task', key='raw_data')
    
    if not datos_raw:
        raise ValueError("No se encontraron datos crudos en XCom.")
    
    # Procesamiento batch simulado para la Variante 1 (SLA de entregas)
    datos_curated = [
        {
            "zona": "Norte",
            "tiempo_promedio": 22.5,
            "porcentaje_fuera_sla": 10.0,
            "pedidos_retrasados": 1
        },
        {
            "zona": "Sur",
            "tiempo_promedio": 45.0,
            "porcentaje_fuera_sla": 35.0,
            "pedidos_retrasados": 4
        }
    ]
    
    ti.xcom_push(key='curated_data', value=datos_curated)
    print("Tarea 2 [procesar_batch] completada: Datos transformados a Curated.")

def task_validar_curated_data(**kwargs):
    """Tarea 3: Valida la calidad de las métricas en la capa Curated."""
    ti = kwargs['ti']
    datos_curated = ti.xcom_pull(task_ids='procesar_batch_task', key='curated_data')
    
    valido = validar_curated(datos_curated)
    if not valido:
        raise ValueError("Fallo crítico: Las métricas Curated de SLA presentan inconsistencias.")
    
    print("Tarea 3 [validar_curated] completada con éxito.")

def task_generar_reporte_sla(**kwargs):
    """Tarea 4: Genera el reporte final de indicadores y reglas de alerta."""
    ti = kwargs['ti']
    datos_curated = ti.xcom_pull(task_ids='procesar_batch_task', key='curated_data')
    
    print("--- REPORTE DE SLA - RAPIDGO ---")
    for registro in datos_curated:
        print(f"Zona: {registro['zona']} | Tiempo Promedio: {registro['tiempo_promedio']} min | Fuera de SLA: {registro['porcentaje_fuera_sla']}%")
        if registro['porcentaje_fuera_sla'] > 20.0:
            print(f"🚨 ALERTA GENERADA: SLA excedido en la zona {registro['zona']}")
            
    print("Tarea 4 [generar_reporte] completada.")

def task_finalizar_pipeline(**kwargs):
    """Tarea 5: Cierre del proceso de orquestación batch de la semana 3."""
    print("Tarea 5 [finalizar_pipeline]: El DAG de la Semana 3 ha finalizado correctamente.")

# Definición del DAG
with DAG(
    'rapidgo_sla_pipeline_semana3',
    default_args=default_args,
    description='DAG de orquestación batch para la Variante 1 - SLA de entregas (Semana 3)',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['rapidgo', 'semana3', 'variante1', 'sla']
) as dag:

    validar_fuentes = PythonOperator(
        task_id='validar_fuentes_task',
        python_callable=task_validar_fuentes,
        provide_context=True
    )

    procesar_batch = PythonOperator(
        task_id='procesar_batch_task',
        python_callable=task_procesar_batch,
        provide_context=True
    )

    validar_curated = PythonOperator(
        task_id='validar_curated_task',
        python_callable=task_validar_curated_data,
        provide_context=True
    )

    generar_reporte = PythonOperator(
        task_id='generar_reporte_task',
        python_callable=task_generar_reporte_sla,
        provide_context=True
    )

    finalizar = PythonOperator(
        task_id='finalizar_pipeline_task',
        python_callable=task_finalizar_pipeline,
        provide_context=True
    )

    # Definición de dependencias estrictas (5 tareas secuenciales con XCom)
    validar_fuentes >> procesar_batch >> validar_curated >> generar_reporte >> finalizar