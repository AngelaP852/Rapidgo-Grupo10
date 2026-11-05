# Checkpoint Semana 3 - Orquestación, Calidad y Resiliencia

## 1. Qué quedó funcionando esta semana
- Funciones de validación de calidad **`validar_raw`** y **`validar_curated`** implementadas para la Variante 1 (SLA de entregas), validando contratos y métricas de negocio.
- **DAG de Apache Airflow** (`rapidgo_sla_dag.py`) configurado con 5 tareas encadenadas (`validar_fuentes_task` $\rightarrow$ `procesar_batch_task` $\rightarrow$ `validar_curated_task` $\rightarrow$ `generar_reporte_task` $\rightarrow$ `finalizar_pipeline_task`), con manejo de reintentos (`retries=1`) e intercambio de datos mediante `XCom`.
- Pipeline de Integración Continua (CI) en GitHub Actions ampliado para validar sintaxis y ejecución de pruebas automatizadas.

## 2. Valor o evidencia real observada en la ejecución
- **Pruebas locales exitosas:** Se ejecutó el conjunto completo de pruebas automatizadas mediante `pytest -v`, obteniendo un resultado de **84 pruebas superadas con éxito (`84 passed in 0.66s`)**, lo que garantiza la estabilidad del código antes de integrarlo a la rama principal.
- El uso de `XCom` en el orquestador permitió transferir exitosamente los registros procesados entre las fases batch y curated.

## 3. Decisión técnica tomada y por qué (ADR-003)
- **Problema:** ¿Cómo asegurar la resiliencia y el control de calidad entre la ingesta y el procesamiento analítico sin comprometer los indicadores de SLA?
- **Opciones consideradas:** 
  1. Omitir validaciones y procesar datos sin filtrar.
  2. Implementar validaciones estrictas con excepciones controladas y reintentos automáticos en el orquestador.
- **Decisión tomada:** Se seleccionó la **opción 2**. Se establecieron validaciones obligatorias en la capa Raw y Curated, configurando excepciones y reintentos para frenar anomalías de datos antes de generar los reportes gerenciales.

## 4. Problema real encontrado y cómo se resolvió (Fallo controlado)
- **Problema / Fallo provocado:** Durante las pruebas del flujo de validación, al simular la llegada de un lote de eventos vacíos o con campos obligatorios faltantes (`pedido`, `distancia`, `estado`), el sistema disparó una excepción controlada para evitar corromper las métricas.
- **Log / Mensaje real observado:**
  ```text
  Fallo crítico: Los datos en la capa Raw no cumplen con el contrato de la Variante 1.
  ValueError: Falta o es nulo el campo obligatorio 'distancia'.