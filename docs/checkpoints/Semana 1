# Checkpoint Semana 1

## Decisión más importante
Publicar los eventos en el topic `rapidgo.sla.entregas` con key `order_id` y 3 particiones, para que todos los eventos de un pedido lleguen en orden a la misma partición (ver `docs/decisions/ADR-001-key-y-particiones-kafka.md`).

## Riesgo principal
La alerta `ENTREGA_DETENIDA` necesita que el producer emita un latido `EN_RUTA` cada 5 min y que Spark mantenga estado por pedido con timeout. Si el producer no genera esos casos de forma realista, no podremos demostrar la alerta.

## Plan Semana 2
- Producer que genere el ciclo de vida del pedido, con retrasos, latidos y entregas detenidas.
- Kafka con 3 particiones y key `order_id`; medir la distribución real por partición.
- Spark Structured Streaming: parseo, ≥3 transformaciones, agregación por ventana de 5 min y 2 alertas.
- Persistencia en Parquet con checkpoint.
- Tests del contrato y de las reglas, y `ci.yml` activo en push, pull_request y workflow_dispatch.
- Roles de la Semana 2 según la guía: Kafka/Producer, Spark/Data Lake y Tests/CI.

## Enlaces
- Issues: https://github.com/USUARIO/Rapidgo-Grupo10/issues
- Pull Requests: https://github.com/USUARIO/Rapidgo-Grupo10/pulls?q=is%3Apr
- Release: https://github.com/USUARIO/Rapidgo-Grupo10/releases/tag/v0.1.0-foundation
