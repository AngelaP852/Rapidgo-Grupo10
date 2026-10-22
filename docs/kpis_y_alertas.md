# KPIs y reglas de alerta – Variante 1 (SLA de entregas)

Los umbrales son valores iniciales (hipótesis). Se revisarán en la
Semana 2 con datos reales del producer y los cambios se documentarán
en un ADR.

## 1. Cálculo del SLA

`promised_at = created_at + 10 min + (8 min × distance_km)`,
redondeado al múltiplo de 5 minutos más cercano.

Los 8 min/km incluyen preparación, recogida y paradas. Fórmula
ajustada a los eventos de `samples/eventos_ciclo_vida.jsonl`:
3.2 km → 35 min y 1.8 km → 25 min.

Un pedido **cumple el SLA** si `event_ts[ENTREGADO] <= promised_at`.

## 2. KPIs

| KPI | Fórmula | Ventana | Agrupación |
|---|---|---|---|
| Tiempo promedio de entrega | promedio(`event_ts[ENTREGADO] − created_at`) | 5 min (tumbling) | zona |
| % fuera de SLA | pedidos con `event_ts[ENTREGADO] > promised_at` ÷ pedidos entregados × 100 | 5 min (tumbling) | zona |
| Pedidos retrasados | conteo de pedidos sin `ENTREGADO`/`CANCELADO` con `ahora > promised_at` | estado actual | zona |

Ejemplo con los samples: ORD-000123 tarda 41 min (6 min tarde) y
ORD-000124 tarda 21 min (cumple). Resultado: promedio 31 min,
50 % fuera de SLA.

Las ventanas usan el tiempo del evento (`event_ts`) con watermark
de 10 minutos.

## 3. Reglas de alerta

| Regla | Condición | Umbral inicial | Severidad |
|---|---|---|---|
| SLA excedido | pedido sin `ENTREGADO`/`CANCELADO` y `ahora > promised_at + tolerancia` | tolerancia = 5 min | Alta |
| Entrega detenida | pedido en `EN_RUTA` o `DETENIDO` sin eventos nuevos | 10 min | Media |

La regla de entrega detenida requiere que el producer emita un evento
`EN_RUTA` cada 5 minutos (latido) mientras el pedido está en ruta.
Por tanto, 10 min equivalen a 2 latidos perdidos.

## 4. Justificación de umbrales

| Umbral | Origen | Si es muy bajo | Si es muy alto |
|---|---|---|---|
| 10 min base + 8 min/km | Ajustado a los pedidos de ejemplo | SLA imposible de cumplir | SLA poco exigente |
| Tolerancia 5 min | Evita alertar por retrasos mínimos | Falsas alarmas | Detección tardía |
| Detenida 10 min | 2 latidos perdidos (latido cada 5 min) | Falsas alarmas | Pedido parado sin detectar |
| Ventana 5 min | Balance entre actualización y estabilidad | Datos ruidosos | Reacción lenta |

## 5. Plan de ajuste

En la Semana 2, con los eventos reales del producer, se revisará:
- la distribución real de tiempos de entrega,
- cuántas alertas se disparan por hora,
- cuántas son falsas alarmas.

Si hay cambios, se documentarán en el ADR-002.