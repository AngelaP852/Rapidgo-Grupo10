# Contrato del evento de pedido – Variante 1 (SLA de entregas)

Cada evento representa un **cambio de estado de un pedido**.
Se publica en Kafka como JSON. La key del mensaje es `order_id`.

## Campos

| Campo | Tipo | Obligatorio | Descripción |
|---|---|---|---|
| event_id | string | Sí | Identificador único del evento |
| team_id | string | Sí | Identifica al equipo que genera el evento |
| order_id | string | Sí | Identificador del pedido |
| status | string | Sí | Estado del pedido (ver valores permitidos) |
| zone | string | Sí | Zona de entrega |
| distance_km | number | Sí | Distancia del recorrido en km (> 0) |
| created_at | string | Sí | Creación del pedido, ISO 8601 UTC |
| promised_at | string | Sí | Hora prometida de entrega, ISO 8601 UTC |
| event_ts | string | Sí | Momento en que ocurrió el evento, ISO 8601 UTC |

## Estados permitidos

CREADO → ASIGNADO → RECOGIDO → EN_RUTA → ENTREGADO

Estados especiales: DETENIDO (entrega parada), CANCELADO.

## Reglas de validación

- Todos los campos son obligatorios.
- `distance_km` debe ser mayor que 0.
- `status` debe ser uno de los valores permitidos.
- Las fechas van en ISO 8601 con zona UTC (terminan en `Z`).
- `event_ts` >= `created_at`.
- `promised_at` > `created_at`.

## Ejemplos

Ver `samples/eventos_pedido.json` y `samples/eventos_ciclo_vida.jsonl`.
