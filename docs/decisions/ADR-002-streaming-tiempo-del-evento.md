# ADR-002: Procesamiento en streaming: tiempo del evento, watermark y validación del contrato

- **Proyecto:** RapidGo 360
- **Variante:** 1 — SLA de entregas
- **Estado:** Propuesta de la Semana 2 (confirmar con el equipo)
- **Semana:** 2

## 1. Contexto y problema

Spark Structured Streaming debe validar cada evento, calcular K1 y K2 por ventanas de 5 minutos y detectar K3, A1 y A2 con estado por pedido. Hay que decidir con qué reloj se mide el tiempo, cuánto retraso se tolera y qué se hace con los eventos que incumplen el contrato.

## 2. Decisión técnica

| Tema | Decisión |
|---|---|
| Reloj | Tiempo del evento (`event_ts`) y watermark del stream, no el reloj del computador |
| Watermark | 5 minutos (`--watermark-min`) |
| Validación | Estricta: un valor nulo o mal escrito nunca es válido. Los rechazados se guardan en `rejected/` con el motivo |
| Regla del SLA | Se exige `promised_at = created_at + 10 min + 8 min × distance_km`, redondeado al múltiplo de 5 (se puede desactivar con `--sin-validar-sla`) |
| Avisos | Un aviso por pedido y regla; A2 se reinicia si el pedido emite un evento nuevo |
| Salidas | Parquet en `raw`, `rejected`, `processed` y `curated`, con un checkpoint por consulta |

## 3. Alternativas consideradas

| Alternativa | Ventaja | Desventaja |
|---|---|---|
| Tiempo de procesamiento (reloj) | Más simple | Los resultados cambian al reprocesar y con datos simulados; no se puede probar de forma repetible |
| Watermark de 2 min | Avisos y ventanas más rápidos | Menos tolerancia a eventos tardíos |
| Watermark de 10 min | Más tolerancia | Avisos y ventanas tardan más en aparecer |
| Validación laxa (filtrar solo lo evidente) | Menos código | Los eventos con nulos o fechas mal escritas desaparecían sin dejar rastro |
| No validar la fórmula del SLA | Más tolerante con datos externos | K2 y A1 podrían calcularse sobre promesas inconsistentes |

## 4. Consecuencias

**Beneficios esperados:**

- Con los mismos datos se obtienen los mismos resultados, y se pueden verificar con `tests/integracion_spark.py`.
- Nada se pierde en silencio: todo evento queda en `raw`, y los inválidos en `rejected` con su motivo.

**Limitaciones:**

- Los avisos por inactividad (A2) y el cierre de las últimas ventanas solo ocurren cuando llegan eventos posteriores que hacen avanzar el watermark. Si el flujo se detiene, los últimos minutos quedan pendientes.
- Los eventos que lleguen con más retraso que el watermark pueden descartarse.
- Un checkpoint viejo no es compatible con cambios en el código de una consulta: hay que borrar `datalake/checkpoints/`.

## 5. Plan de validación

1. Ejecutar `python tests/integracion_spark.py`: genera eventos, mezcla inválidos y duplicados, corre el job sin Kafka y compara el Data Lake con un cálculo independiente en pandas.
2. Ejecutar el flujo completo con Docker (Kafka, producer y Spark) y revisar `python spark/leer_datalake.py`.

## 6. Estado de la evidencia

La prueba de integración (`tests/integracion_spark.py`) usa datos generados por el producer, compara el Data Lake con un cálculo independiente en pandas y no mide rendimiento.

Corrida completa con Docker (Kafka, producer y Spark) el 2026-11-06, con `python -m producer.producer --n-pedidos 200 --seed 42`:

| Capa | Registros |
|---|---|
| `raw/eventos` | 2161 (todo lo publicado) |
| `rejected/eventos` | 0 (el producer solo genera eventos válidos) |
| `processed/eventos` | 2161 |
| `curated/kpis` | 126 combinaciones de ventana y zona |
| `curated/alertas` | 149: K3_RETRASADO 71, A1_SLA_EXCEDIDO 63, A2_ENTREGA_DETENIDA 15 |

- Los 15 avisos A2 corresponden a los 15 pedidos detenidos que generó el producer.
- Los totales de avisos coinciden con los que se obtienen de los mismos eventos con un cálculo independiente en pandas.
- De las 127 combinaciones de ventana y zona de los pedidos entregados, 126 están cerradas; la última queda pendiente hasta que llegue un evento posterior (limitación descrita en la sección 4).
- Capturas: [`05_datalake.png`](../evidencias/semana2/05_datalake.png) y [`06_alertas.png`](../evidencias/semana2/06_alertas.png).
- Los eventos con fecha anterior al watermark se guardan en `raw/` pero no pasan a `processed/`. Al probar el reinicio hay que publicar el lote nuevo con `--inicio` posterior al último evento anterior.

[COMPLETAR tras la prueba con Docker: reinicio de Spark desde el checkpoint, con los conteos antes y después.]

## 7. Referencias internas

- [`spark/streaming_job.py`](../../spark/streaming_job.py) y [`spark/alertas_estado.py`](../../spark/alertas_estado.py)
- [`docs/kpis_y_alertas.md`](../kpis_y_alertas.md): reglas de K1 a K3, A1 y A2.
- [`docs/casos_prueba_alertas.md`](../casos_prueba_alertas.md): casos de las alertas.
- [`datalake/README.md`](../../datalake/README.md): capas del Data Lake.
