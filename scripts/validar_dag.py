"""Valida el DAG de Airflow importandolo con Airflow real (quality gate, issue #65).

Se ejecuta en un job aparte del CI, con Apache Airflow instalado. No va dentro de
tests/ porque Airflow instalado choca con el paquete local airflow/utils que usan
las pruebas unitarias.

Falla (codigo 1) si:
  - el DAG no se puede importar,
  - falta el dag_id esperado o alguna de las 5 tareas,
  - cambia el orden de dependencias,
  - se pierden los reintentos (retries < 1).

Uso (desde la raiz del repo):  python scripts/validar_dag.py
"""
import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
DAG_ID = "rapidgo_sla_pipeline_semana3"
CADENA = ["check_fuentes", "validar_raw", "agregar", "validar_curated", "reporte"]


def main() -> int:
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
    from airflow.models import DagBag

    bolsa = DagBag(dag_folder=str(RAIZ / "airflow" / "dags"), include_examples=False)
    errores = []

    for archivo, error in bolsa.import_errors.items():
        errores.append(f"No se pudo importar {archivo}: {error.strip().splitlines()[-1]}")

    dag = bolsa.dags.get(DAG_ID)
    if dag is None:
        errores.append(f"No existe el DAG '{DAG_ID}'.")
    else:
        ids = [t.task_id for t in dag.tasks]
        if sorted(ids) != sorted(CADENA):
            errores.append(f"Tareas distintas a las esperadas: {sorted(ids)}")
        for antes, despues in zip(CADENA, CADENA[1:]):
            if antes in dag.task_dict and despues not in dag.task_dict[antes].downstream_task_ids:
                errores.append(f"Falta la dependencia {antes} >> {despues}")
        if dag.default_args.get("retries", 0) < 1:
            errores.append("El DAG no tiene reintentos (default_args['retries']).")

    if errores:
        print("DAG INVALIDO:")
        for e in errores:
            print(f"  - {e}")
        return 1

    print(f"DAG OK: {DAG_ID} -> {' >> '.join(CADENA)} (retries={dag.default_args['retries']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
