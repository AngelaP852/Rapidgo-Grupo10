# Checkpoint Semana 2

## Decisión más importante
Procesar los eventos con el tiempo del evento (`event_ts`) y un watermark de 5 minutos, y validar cada evento contra el contrato: los que no cumplen van a `rejected/` con su motivo en lugar de perderse (ver [ADR-002](../decisions/ADR-002-streaming-tiempo-del-evento.md)).

## Riesgo principal
Los avisos por inactividad (A2) y el cierre de las últimas ventanas de KPIs dependen de que lleguen eventos posteriores: si el flujo se detiene, los últimos minutos quedan sin avisar. Además, un evento más viejo que el watermark queda en `raw/` pero no pasa a `processed/`. Para la Semana 3 hay que decidir si `evaluar_alertas` en Airflow recalcula sobre `processed/` con una hora de corte.

## Evidencia
- **Particiones de Kafka:** 200 pedidos y 2161 mensajes, repartidos 794, 690 y 677 en las 3 particiones, 0 sin key y ningún `order_id` en más de una partición ([ADR-001, sección 6](../decisions/ADR-001-key-y-particiones-kafka.md)). Captura: `docs/evidencias/semana2/04_particiones.png`.
- **Data Lake:** `raw` 2161, `rejected` 0, `processed` 2161, `curated/kpis` 126 y `curated/alertas` 149 (K3 71, A1 63, A2 15). Capturas: `05_datalake.png` y `06_alertas.png`.
- **Pruebas:** 188 pruebas pasan (`01_pytest.png`) y el CI está en verde en GitHub Actions (`02_github_actions.png`).  Cifra final en la versión v1.0.0: **151 pruebas** (CI en verde sobre `main`).
 
## Plan Semana 3
- DAG de Airflow con `validar_fuentes`, `controlar_calidad`, `agregar_kpis`, `generar_reporte` y `evaluar_alertas`.
- Reintentos y fallo controlado.
- Airflow en el `docker-compose.yml`.
- CI ampliado.

## Enlaces
- Issues: https://github.com/AngelaP852/Rapidgo-Grupo10/issues
- Pull Requests: https://github.com/AngelaP852/Rapidgo-Grupo10/pulls?q=is%3Apr
- Release: https://github.com/AngelaP852/Rapidgo-Grupo10/releases/tag/v0.2.0-streaming
