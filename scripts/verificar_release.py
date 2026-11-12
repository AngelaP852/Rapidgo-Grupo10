"""Validacion previa al tag (quality gate, issue #65).

Comprueba que el repo tiene lo que la guia pide para el release:
  - archivos y carpetas obligatorios,
  - secciones del README,
  - ADR-001..003 y checkpoints de las semanas 1 a 3.

Con --estricto (pre-tag v1.0.0) ademas exige que el README ya tenga el enlace
al video final, es decir, que no quede el marcador VIDEO_PENDIENTE.

Uso:  python scripts/verificar_release.py [--estricto]
"""
import re
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent

ARCHIVOS = [
    "README.md",
    "requirements.txt",
    "docker-compose.yml",
    ".gitignore",
    ".github/workflows/ci.yml",
    "airflow/dags/rapidgo_sla_dag.py",
    "airflow/utils/quality_checks.py",
    "airflow/utils/pipeline_tasks.py",
    "producer/producer.py",
    "spark/streaming_job.py",
    "datalake/README.md",
    "docs/arquitectura.png",
    "docs/contrato_evento.md",
    "docs/kpis_y_alertas.md",
    "docs/checkpoints/semana-1.md",
    "docs/checkpoints/semana-2.md",
    "docs/checkpoints/semana-3.md",
    "docs/decisions/ADR-001-key-y-particiones-kafka.md",
    "docs/decisions/ADR-002-streaming-tiempo-del-evento.md",
    "docs/decisions/ADR-003-estrategia-orquestacion-recuperacion.md",
    "samples/eventos_pedido.json",
]

# Texto que debe aparecer en el README (guia, seccion 10).
SECCIONES_README = [
    "## Problema de negocio",
    "## Arquitectura",
    "## Ejecución local",
    "## Tabla de Contribuciones",
]


def main(estricto: bool) -> int:
    errores = []

    for ruta in ARCHIVOS:
        if not (RAIZ / ruta).is_file():
            errores.append(f"Falta el archivo obligatorio: {ruta}")

    readme_ruta = RAIZ / "README.md"
    readme = readme_ruta.read_text(encoding="utf-8") if readme_ruta.is_file() else ""
    for seccion in SECCIONES_README:
        if seccion not in readme:
            errores.append(f"README sin la seccion '{seccion}'")

    if estricto:
        if "VIDEO_PENDIENTE" in readme:
            errores.append("README todavia tiene VIDEO_PENDIENTE: falta el enlace al video final.")
        if not re.search(r"[Vv]ideo final[^\n]*https?://\S+", readme):
            errores.append("README no tiene el enlace (http/https) al video final.")
        if not re.search(r"Versi[oó]n actual[^\n]*v1\.0\.0", readme):
            errores.append("README no indica 'Versión actual' v1.0.0.")

    if errores:
        print("VALIDACION PREVIA AL TAG: FALLO")
        for e in errores:
            print(f"  - {e}")
        return 1

    modo = "estricto (pre-tag v1.0.0)" if estricto else "basico"
    print(f"VALIDACION PREVIA AL TAG: OK ({modo}, {len(ARCHIVOS)} archivos revisados)")
    return 0


if __name__ == "__main__":
    sys.exit(main("--estricto" in sys.argv[1:]))
