# Casos de prueba de las alertas – Variante 1 (SLA de entregas)

Este documento define **qué debe pasar** con las alertas A1 y A2 en cada situación.
Las reglas vienen de [kpis_y_alertas.md](kpis_y_alertas.md) y los eventos de
`samples/eventos_ciclo_vida.jsonl` (ORD-000123 y ORD-000124) y
`samples/eventos_entrega_detenida.jsonl` (ORD-000126).

## Reglas que se prueban

| Id | Nombre | Se dispara cuando |
|---|---|---|
| A1 | SLA_EXCEDIDO | el pedido sigue abierto (sin `ENTREGADO`/`CANCELADO`) y `ahora > promised_at + 5 min` |
| A2 | ENTREGA_DETENIDA | el pedido está en `EN_RUTA` o `DETENIDO` y pasaron **más de** 10 min sin eventos nuevos |

Un pedido está **abierto** hasta que recibe `ENTREGADO` o `CANCELADO`.
Los límites son estrictos (`>`): justo en el umbral **no** se dispara.

## Datos de referencia

| Pedido | Zona | promised_at | A1 se dispara después de | Eventos relevantes |
|---|---|---|---|---|
| ORD-000123 | Centro | 14:35 | 14:40 | `EN_RUTA` 14:20, 14:25, 14:30, 14:35, 14:40; `ENTREGADO` 14:41 |
| ORD-000124 | Norte | 14:30 | 14:35 | `EN_RUTA` 14:13, 14:18, 14:23; `ENTREGADO` 14:26 |
| ORD-000126 | Sur | 14:25 | 14:30 | `EN_RUTA` 14:10, 14:15; `DETENIDO` 14:18; sin eventos después |

Todas las horas son del 2026-10-21, en UTC.

## A1 – SLA_EXCEDIDO

| Caso | Pedido | Situación | `ahora` | Resultado esperado |
|---|---|---|---|---|
| A1-01 | ORD-000123 | Abierto, ya pasó `promised_at + 5 min` | 14:40:01 | **Dispara A1** |
| A1-02 | ORD-000123 | Justo en el límite (`promised_at + 5 min`) | 14:40:00 | No dispara |
| A1-03 | ORD-000123 | Retrasado pero dentro de la tolerancia | 14:37:00 | No dispara |
| A1-04 | ORD-000123 | Antes de `promised_at` | 14:30:00 | No dispara |
| A1-05 | ORD-000123 | Ya recibió `ENTREGADO` (14:41), aunque llegó tarde | 14:45:00 | No dispara (pedido cerrado) |
| A1-06 | ORD-000124 | Entregado a tiempo (14:26 < 14:30) | 14:50:00 | No dispara |
| A1-07 | ORD-000126 | `DETENIDO` y sin cerrar, pasó el límite (14:30) | 14:31:00 | **Dispara A1** (y también A2, ver A2-01) |
| A1-08 | cualquiera | Pedido `CANCELADO` después de `promised_at + 5 min` | después del límite | No dispara |

## A2 – ENTREGA_DETENIDA

| Caso | Pedido | Situación | `ahora` | Resultado esperado |
|---|---|---|---|---|
| A2-01 | ORD-000126 | `DETENIDO` desde 14:18, sin eventos nuevos | 14:28:01 | **Dispara A2** |
| A2-02 | ORD-000126 | Justo 10 min sin eventos | 14:28:00 | No dispara |
| A2-03 | ORD-000126 | Detenido hace 5 min | 14:23:00 | No dispara |
| A2-04 | ORD-000124 | `EN_RUTA` con latidos cada 5 min (14:13, 14:18, 14:23) | 14:25:00 | No dispara |
| A2-05 | ORD-000123 | `EN_RUTA` con latidos hasta 14:40 | 14:45:00 | No dispara (último evento hace 5 min) |
| A2-06 | ORD-000123 | Hipotético: el último latido fue 14:25 y no llegó más | 14:35:01 | **Dispara A2** |
| A2-07 | ORD-000124 | Recibió `ENTREGADO` 14:26; pasan más de 10 min | 14:40:00 | No dispara (pedido cerrado) |
| A2-08 | hipotético | Pedido en `RECOGIDO` sin eventos por 15 min | cualquiera | No dispara A2 (solo aplica a `EN_RUTA`/`DETENIDO`) |
| A2-09 | ORD-000126 | `DETENIDO` y luego llega un `EN_RUTA` nuevo (se reanuda) | 10 min después del `DETENIDO` | No dispara; el contador de 10 min se reinicia con el evento nuevo |

## Casos combinados

| Caso | Situación | Resultado esperado |
|---|---|---|
| AC-01 | ORD-000126 a las 14:31 | Dispara A1 **y** A2 (dos alertas distintas para el mismo pedido) |
| AC-02 | ORD-000123 a las 14:40:01, con latido de 14:40 | Dispara A1; no dispara A2 (latido reciente) |
| AC-03 | ORD-000124 completo | Ninguna alerta durante todo su ciclo de vida |

## Cómo se verifica

1. Armar la lista de eventos del pedido (de los samples).
2. Fijar el valor de `ahora`.
3. Evaluar la regla y comparar con la columna "Resultado esperado".

Estos casos se prueban en `tests/test_alertas.py`, con las funciones puras de
`spark/alertas_estado.py` (sin Spark). Los resultados de la tabla son la **foto en el instante
`ahora`**: un pedido ya cerrado no genera avisos nuevos (A1-05 y A1-08).

El job de Spark procesa eventos que pueden llegar con retraso, así que además emite el aviso
**cuando llega el evento que revela que el límite ya se había cumplido**. Por ejemplo, ORD-000123
recibe `ENTREGADO` a las 14:41, después del límite de las 14:40: A1 se registra con `momento`
14:40 una sola vez, y a las 14:45 no se vuelve a generar nada. La prueba de punta a punta de ese
comportamiento es `tests/integracion_spark.py`.
