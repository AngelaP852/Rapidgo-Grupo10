# Checkpoint Semana 3 - Orquestación, Calidad y Resiliencia

## 1. Qué quedó funcionando esta semana
- Funciones de validación de calidad **`validar_raw`** y **`validar_curated`** implementadas para la Variante 1 (SLA de entregas), validando contratos y métricas de negocio.
- **Servicio de Airflow en `docker-compose.yml`:** webserver, scheduler y una base Postgres para los metadatos, con la interfaz en http://localhost:8080.
- **DAG de Apache Airflow** (`rapidgo_sla_dag.py`) con 5 tareas encadenadas (`check_fuentes` $\rightarrow$ `validar_raw` $\rightarrow$ `agregar` $\rightarrow$ `validar_curated` $\rightarrow$ `reporte`) que leen los datos reales del Data Lake y calculan K1, K2 y K3 por zona. Los reintentos están en `default_args` (`retries=2`, `retry_delay` de 1 minuto) y las tareas intercambian datos mediante `XCom`.
- Pipeline de Integración Continua (CI) en GitHub Actions ampliado para validar sintaxis, ejecutar las pruebas automatizadas y validar el `docker-compose.yml`.

## 2. Valor o evidencia real observada en la ejecución
- **Pruebas locales exitosas:** Se ejecutó el conjunto completo de pruebas automatizadas mediante `pytest -v`, obteniendo **113 pruebas superadas**, lo que da estabilidad al código antes de integrarlo a la rama principal, Cifra final en la versión v1.0.0: **151 pruebas** (CI en verde sobre `main`).
- **Ejecución completa del DAG:** el 8 de noviembre de 2026 las 5 tareas terminaron en verde y el reporte validó 2161 eventos. Capturas: `docs/evidencias/semana3/03_airflow_ui.png`, `04_dag_graph_exitoso.png` y `05_log_reporte.png`.
- **XCom en uso real:** `validar_raw` publica el conteo de eventos (`conteo_eventos`) y `agregar` publica los KPIs por zona; `validar_curated` y `reporte` los leen para validar y para armar el reporte. Capturas: `07_xcom_validar_raw.png` y `08_xcom_usado_reporte.png`.
- **Dónde verlo en la UI:** el XCom se ve entrando a la tarea y abriendo la pestaña **XCom**, o en el menú **Admin → XComs**. Los reintentos se ven en **Grid** (la tarea pasa a `up_for_retry`) y en **Logs**, donde se elige el intento.



## 3. Decisión técnica tomada y por qué (ADR-003)
Detalle completo en [ADR-003](../decisions/adr-003-estrategia-orquestacion-recuperacion.md).

- **Problema:** ¿Cómo asegurar la resiliencia y el control de calidad entre la ingesta y el procesamiento analítico sin comprometer los indicadores de SLA?
- **Opciones consideradas:** 
  1. Omitir validaciones y procesar datos sin filtrar.
  2. Implementar validaciones estrictas con excepciones controladas y reintentos automáticos en el orquestador.
- **Decisión tomada:** Se seleccionó la **opción 2**. Se establecieron validaciones obligatorias en la capa Raw y Curated, configurando excepciones y reintentos para frenar anomalías de datos antes de generar los reportes gerenciales.

## 4. Problema real encontrado y cómo se resolvió (Fallo controlado)
- **Problema / Fallo provocado:** para comprobar los reintentos, la tarea `check_fuentes` falla a propósito en su primer intento cuando la Variable de Airflow `rapidgo_forzar_fallo` vale `1`. Con `retries=2` y `retry_delay` de 1 minuto, Airflow dejó la tarea en `up_for_retry` y la volvió a ejecutar.
- **Log / Mensaje real observado (intento 1, 8 de noviembre de 2026, 22:55 UTC):**
  ```text
  RuntimeError: Fallo controlado: primer intento forzado por la Variable rapidgo_forzar_fallo.
  ```
- **Recuperación (intento 2, un minuto después):**
  ```text
  [check_fuentes] Kafka OK. Archivos parquet en processed/eventos: 6
  ```
  El resto de las tareas terminó en verde. Capturas: `06a_retry_intento1_error.png`, `06b_retry_intento2_ok.png` y `06c_retry_up_for_retry.png`.
- **Validación de calidad:** si a un evento le falta un campo obligatorio, `validar_raw` registra `Fallo de calidad en RAW [Registro N]: Falta o es nulo el campo '<campo>'.` y la tarea se detiene con `ValueError` antes de calcular los KPIs.

## 5. Enlaces
- Issues: https://github.com/AngelaP852/Rapidgo-Grupo10/issues
- Pull Requests: https://github.com/AngelaP852/Rapidgo-Grupo10/pulls?q=is%3Apr
- Release: https://github.com/AngelaP852/Rapidgo-Grupo10/releases/tag/v0.3.0-orchestration
