# ADR-001: Definición del topic, key y particiones de Kafka

- **Proyecto:** RapidGo 360
- **Variante:** 1 — SLA de entregas
- **Estado:** Propuesta inicial aceptada para el diseño
- **Semana:** 1

## 1. Contexto y problema

RapidGo necesita procesar los eventos generados durante el ciclo de vida de cada pedido para calcular los tiempos de entrega, verificar el cumplimiento del acuerdo de nivel de servicio (SLA) e identificar posibles retrasos.

Para organizar el flujo de eventos se debe definir un topic de Kafka, una clave para identificar los pedidos y un número inicial de particiones que facilite el procesamiento distribuido.

## 2. Decisión técnica

| Parámetro | Decisión |
|---|---|
| Topic | `rapidgo.sla.entregas` |
| Key | `order_id` |
| Número de particiones | 3 |
| Factor de replicación | 1 (un solo broker local) |

### 2.1. Topic: `rapidgo.sla.entregas`

El topic será el canal donde se publicarán los eventos de cambio de estado de los pedidos.

**Justificación:** su nombre identifica el proyecto (`rapidgo`), el propósito del análisis (`sla`) y el tipo de información procesada (`entregas`). Esta nomenclatura facilita la organización y el mantenimiento del sistema.

### 2.2. Key: `order_id`

Se utilizará el identificador único del pedido (`order_id`, según el [contrato del evento](../contrato_evento.md)) como clave de cada mensaje.

**Justificación:** un mismo pedido genera varios eventos (`CREADO`, `ASIGNADO`, `RECOGIDO`, `EN_RUTA`, `ENTREGADO`, y a veces `DETENIDO` o `CANCELADO`). Al usar el mismo `order_id` como clave, Kafka dirige esos mensajes a la misma partición, siempre que se mantengan la misma clave y el mismo número de particiones.

Kafka conserva el orden de los mensajes dentro de cada partición. Esto permite relacionar los eventos de un pedido y analizar su secuencia para calcular el tiempo de entrega y comprobar el cumplimiento del SLA.

No obstante, este mecanismo no garantiza que los eventos lleguen en orden cronológico real si se producen retrasos o si el productor los envía fuera de secuencia. Por eso el contrato incluye `event_ts`, que Spark usa como tiempo del evento.

### 2.3. Número de particiones: 3

**Justificación:**

- **Procesamiento en paralelo:** las particiones permiten que varios consumidores lean datos simultáneamente.
- **Escalabilidad:** permite experimentar con la distribución de eventos cuando aumente la cantidad de pedidos.
- **Complejidad controlada:** tres particiones son manejables para un proyecto académico, sin introducir complejidad innecesaria.
- **Pruebas técnicas:** durante la Semana 2 se comprobará la distribución real de los mensajes.

La elección de tres particiones es una decisión inicial de diseño, no un resultado de mediciones de rendimiento. La cantidad definitiva deberá evaluarse según el volumen de eventos, la tasa de llegada de mensajes y la capacidad de los consumidores.

## 3. Alternativas consideradas

| Alternativa | Ventaja | Desventaja |
|---|---|---|
| Una partición | Configuración sencilla y orden total dentro del topic. | Limita el paralelismo de consumo. |
| Tres particiones | Permite experimentar con el paralelismo y mantiene una configuración inicial sencilla. | Requiere evaluar la distribución y el consumo de mensajes. |
| Diez particiones | Ofrece más posibilidades de paralelismo. | Puede ser innecesario para la carga inicial y añade complejidad. |

También se consideró utilizar `zone` como clave. Sin embargo, los pedidos de una zona con mucha demanda podrían concentrarse en una misma partición. Por eso se prefiere `order_id`, que distribuye los pedidos según sus claves, aunque la distribución real dependerá de los datos y del algoritmo de particionamiento.

## 4. Consecuencias de la decisión

**Beneficios esperados:**

- Facilitar la agrupación de eventos por pedido.
- Conservar el orden relativo de los mensajes de un pedido dentro de su partición.
- Permitir pruebas de procesamiento concurrente.
- Establecer una configuración clara para la integración con Spark Structured Streaming.

**Limitaciones:**

- Tres particiones no garantizan que el sistema sea tres veces más rápido.
- El paralelismo efectivo depende de los consumidores, la carga y los recursos disponibles.
- Cambiar el número de particiones puede modificar la asignación de una misma clave a una partición, por lo que debe evaluarse si se necesita conservar el orden de los eventos históricos y futuros.
- La configuración debe validarse mediante pruebas antes de considerar un despliegue de producción.

## 5. Plan de validación (Semana 2)

1. Crear el topic `rapidgo.sla.entregas` con tres particiones (ya definido en `docker-compose.yml`).
2. Publicar varios eventos de prueba con diferentes valores de `order_id`.
3. Enviar varios eventos con el mismo `order_id` y verificar que se asignen a la misma partición.
4. Inspeccionar la distribución de los mensajes entre las tres particiones.
5. Integrar el consumo con Spark Structured Streaming y revisar el procesamiento de los eventos.

## 6. Estado de la evidencia

Esta decisión corresponde al diseño de la Semana 1. La configuración todavía debe implementarse y validarse en el entorno del proyecto. No se afirma que se hayan realizado pruebas de rendimiento. Los resultados de la validación se agregarán en esta sección durante la Semana 2.

## 7. Referencias internas

- [`README.md`](../../README.md): descripción general, arquitectura y configuración de Kafka.
- [`docs/contrato_evento.md`](../contrato_evento.md): campos del evento y reglas de validación.
- [`samples/eventos_pedido.json`](../../samples/eventos_pedido.json): ejemplo de evento.
- [`docs/checkpoints/semana-1.md`](../checkpoints/semana-1.md): resumen del entregable de la primera semana.
