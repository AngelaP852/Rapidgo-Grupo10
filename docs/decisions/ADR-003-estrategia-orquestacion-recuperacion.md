# ADR-003: Estrategia de Orquestación y Recuperación ante Fallos en Pipeline Batch

* **Estatus:** Aceptado
* **Fecha:** 2026-11-05
* **Autores:** Equipo 10 (Ana Karina Arenales, Angela María Palacios, Daniela Gómez)
* **Contexto:** Variante 1 - SLA de Entregas (RapidGo 360)

## Contexto y Problema

En la Semana 3 del proyecto RapidGo 360, se requiere orquestar el procesamiento batch de datos desde la capa Raw hasta la capa Curated. Los datos provenientes de la ingesta pueden presentar anomalías de calidad (eventos con campos faltantes o valores fuera de rango para la Variante 1, como tiempos o distancias nulas). Es necesario definir una estrategia de orquestación que no solo transforme los datos para calcular métricas de SLA, sino que también asegure la calidad y gestione los fallos de manera resiliente sin corromper los reportes ni detener indefinidamente los servicios.

## Opciones Consideradas

1. **Opción 1: Procesamiento directo sin validación intermedia ni reintentos.**

   - *Pros:* Implementación rápida y sencilla.
   - *Contras:* Los datos corruptos ingresarían a la capa Curated, distorsionando las métricas de SLA y generando alertas falsas. Ante cualquier error imprevisto, el pipeline colapsaría sin reintentar.
2. **Opción 2: Orquestación modular con Apache Airflow, validaciones estrictas y reintentos configurados (`retries`).**

   - *Pros:* Permite modularizar la ejecución en 5 tareas secuenciales, aislar fallos mediante validaciones con excepciones (`validar_raw` y `validar_curated`), transferir estado seguro vía `XCom` y permitir la recuperación automática mediante la política de reintentos en cada tarea.
   - *Contras:* Requiere una configuración más rigurosa de dependencias y pruebas unitarias integradas en CI/CD.

## Decisión Tomada

Se elige la **Opción 2**.

Se implementa el DAG `rapidgo_sla_pipeline_semana3` en Apache Airflow estructurado en 5 tareas (`check_fuentes` $\rightarrow$ `validar_raw` $\rightarrow$ `agregar` $\rightarrow$ `validar_curated` $\rightarrow$ `reporte`).

Se configuran en los `default_args` de Airflow las siguientes políticas de resiliencia:

- `retries: 2`: Permite reintentar automáticamente la tarea ante fallos transitorios de red o sistema.
- `retry_delay: timedelta(minutes=1)`: Otorga un margen de tiempo razonable antes del reintento.
- Uso de **`XCom`** para garantizar la inmutabilidad y trazabilidad de los datos intercambiados entre tareas: `validar_raw` publica `conteo_eventos` y `agregar` publica los KPIs por zona, que leen `validar_curated` y `reporte`.
- Captura de **excepciones explícitas (`ValueError`)** en las tareas de validación para interrumpir la secuencia de forma controlada si los contratos de la Variante 1 no se cumplen.

## Consecuencias

- **Positivas:**
  - Garantía de integridad en los KPIs de SLA (se detiene la consolidación si Raw o Curated no pasan la validación de calidad).
  - Resiliencia comprobable ante fallos temporales de infraestructura mediante la política de reintentos.
  - Trazabilidad total de ejecuciones en los logs de Airflow y en las pruebas de CI (`pytest`).
- **Negativas / Mitigaciones:**
  - Requiere mantener sincronizados los contratos de datos de la Variante 1 entre el entorno local, Airflow y GitHub Actions.

## Estado de Validación

Validado mediante la suite de 151 pruebas unitarias locales (`pytest`) y la ejecución del pipeline de Integración Continua en GitHub Actions.

La recuperación ante fallos se comprobó en Airflow el 8 de noviembre de 2026: con la Variable `rapidgo_forzar_fallo` en `1`, el primer intento de `check_fuentes` falló, la tarea pasó a `up_for_retry` y el segundo intento, un minuto después, terminó bien junto con el resto del DAG. Capturas en `docs/evidencias/semana3/` (`06a_retry_intento1_error.png`, `06b_retry_intento2_ok.png`, `06c_retry_up_for_retry.png`).
