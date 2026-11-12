# Quality gate del CI (Semana 4)

Regla: **si el CI falla, no se libera.** El tag `v1.0.0` solo se crea con el último run de `main` en verde.

## Qué valida `.github/workflows/ci.yml`

| Job | Qué revisa | Falla si… |
|---|---|---|
| `test` | `ruff` sobre todo el repo, `compileall` (spark, airflow, rapidgo, tests, producer, scripts), `pytest` y `docker compose config --quiet` | se rompe el contrato del evento, una regla de negocio (K1–K3, A1, A2), la sintaxis o el compose |
| `dag-import` | Instala Airflow 2.10.5 (la misma versión del compose) e importa el DAG con `scripts/validar_dag.py` | el DAG no se importa, cambia el `dag_id`, faltan tareas, cambia el orden de dependencias o se pierden los reintentos |
| `quality-gate` | `scripts/verificar_release.py` y empaquetado (`git archive`) subido como artefacto | falta un archivo obligatorio (README, ADR-001..003, checkpoints, arquitectura…) o una sección del README |

`quality-gate` depende de `test` y `dag-import` (`needs`): no se ejecuta si alguno falló.

## Validación previa al tag (modo estricto)

`scripts/verificar_release.py --estricto` además exige que el README tenga el enlace al video final (sin `VIDEO_PENDIENTE`) y la versión actual `v1.0.0`. Se activa solo:

- al hacer push de un tag (`ref_type == tag`), o
- al ejecutar manualmente el workflow con `estricto = true` (**Actions → CI → Run workflow**).

Antes de crear el tag se corre una vez en modo estricto sobre `main`; si pasa, se crea el tag.

## Evidencia de que el CI falla cuando debe

Se probó en una copia del repo; cada cambio se descartó después:

| Qué se rompió | Dónde falla |
|---|---|
| Contrato: `status` inválido en `samples/eventos_pedido.json` | `test` → `test_cada_evento_cumple_el_contrato[EVT-000100]` |
| Regla: tolerancia de A1 de 5 a 6 min en `rapidgo/alertas.py` | `test` → 4 pruebas de `tests/test_alertas.py` |
| DAG: se quita la dependencia `validar_curated >> reporte` | `test` → `test_las_tareas_van_en_orden` y `dag-import` → "Falta la dependencia validar_curated >> reporte" |
| DAG: un import inexistente | `dag-import` → "No se pudo importar … ImportError" |

## Pruebas independientes de la máquina

Ninguna prueba lee `datalake/`, rutas absolutas ni Kafka: usan `samples/`, datos en memoria y `tmp_path`. Las pruebas unitarias corren sin Docker ni Airflow.

## Ejecutarlo en local

```
python -m pip install -r requirements.txt ruff
ruff check --select E,F --ignore E501 .
python -m compileall -q spark airflow rapidgo tests producer scripts
python -m pytest -v
python scripts/verificar_release.py            # o --estricto antes del tag
```
