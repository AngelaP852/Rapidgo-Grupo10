# Indicadores y alertas – Variante 1 (SLA de entregas)

Los umbrales son **hipótesis iniciales**. Se revisarán en la Semana 2 con los datos que genere el producer.

## Cálculo del SLA

```
promised_at = created_at + 10 min + 8 min × distance_km
```

El resultado se redondea al múltiplo de 5 min más cercano.

| distance_km | Minutos prometidos | Redondeado |
|---|---|---|
| 1.8 | 10 + 14.4 = 24.4 | 25 |
| 2.0 | 10 + 16.0 = 26.0 | 25 |
| 3.2 | 10 + 25.6 = 35.6 | 35 |
| 4.5 | 10 + 36.0 = 46.0 | 45 |

## Indicadores

Se calculan en Spark por ventana de 5 min sobre `event_ts`, agrupados por `zone`.

| Id | Indicador | Definición | Evento usado |
|---|---|---|---|
| K1 | Tiempo promedio de entrega | promedio de `event_ts(ENTREGADO) − created_at`, en minutos | `ENTREGADO` |
| K2 | % fuera de SLA | pedidos con `event_ts(ENTREGADO) > promised_at` ÷ pedidos entregados × 100 | `ENTREGADO` |
| K3 | Pedidos retrasados | pedidos sin `ENTREGADO`/`CANCELADO` con `ahora > promised_at` | último estado de cada pedido |

### Ejemplo con `samples/eventos_ciclo_vida.jsonl`

| Pedido | Zona | Prometido | Entregado | Duración | ¿Fuera de SLA? |
|---|---|---|---|---|---|
| ORD-000124 | Norte | 14:30 | 14:26 | 21 min | No |
| ORD-000123 | Centro | 14:35 | 14:41 | 41 min | Sí (6 min tarde) |

## Alertas

| Id | Nombre | Regla | Umbral inicial |
|---|---|---|---|
| A1 | SLA_EXCEDIDO | pedido sin `ENTREGADO`/`CANCELADO` y `ahora > promised_at + 5 min` | 5 min de tolerancia |
| A2 | ENTREGA_DETENIDA | pedido en `EN_RUTA` o `DETENIDO` sin eventos nuevos por más de 10 min | 10 min sin eventos; latido cada 5 min |

### Notas de implementación

- A1 se evalúa por pedido abierto. Un pedido deja de estar abierto al recibir `ENTREGADO` o `CANCELADO`.
- A2 necesita estado por pedido en Spark con timeout. Si el producer no emite el latido `EN_RUTA` cada 5 min, la alerta se dispararía en pedidos sanos.
- Con `samples/eventos_ciclo_vida.jsonl`, ORD-000123 dispara A1 a las 14:40 (`promised_at + 5 min`), antes de entregarse a las 14:41.
- Con `samples/eventos_entrega_detenida.jsonl`, ORD-000126 queda `DETENIDO` a las 14:18 sin eventos posteriores, así que A2 se dispara a partir de las 14:28.
