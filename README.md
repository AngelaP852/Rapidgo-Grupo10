# RapidGo · Variante 1 — SLA de entregas

Flujo de datos Big Data de extremo a extremo para detectar retrasos e incumplimientos de los tiempos prometidos en las entregas de RapidGo. Proyecto final — Línea de Profundización I (Big Data) · Ingeniería de Sistemas y Computación · Facatativá.

Docente: Ing. Oscar Jobany Gomez Ochoa

| | |
|---|---|
| **Equipo** | Grupo 10 |
| **Integrantes** | Daniela Gomez Castro · Angela Maria Palacios Alonso · Ana Karina Arenales Bautista |
| **Variante** | 1 — SLA de entregas |
| **Versión actual** | v0.1.0-foundation (Semana 1 · diseño y organización) |
| **Tablero Kanban** | https://github.com/users/AngelaP852/projects/4/views/1 |

## Problema de negocio

RapidGo necesita detectar retrasos e incumplimientos de los tiempos prometidos a sus clientes. Cuando un pedido llega después de la hora prometida baja la satisfacción del cliente, y si el retraso se detecta al final del día ya no hay forma de reaccionar. Detectarlo en minutos permite reasignar repartidores, avisar al cliente y medir el cumplimiento por zona.

**Valor de la solución:** indicadores por ventanas de 5 minutos y alertas automáticas por pedido, con trazabilidad completa en un Data Lake por capas (raw → processed → curated).

## Arquitectura

![Arquitectura de RapidGo](docs/arquitectura.png)

| Componente | Función en la variante |
|---|---|
| Producer (Python) | Genera eventos de cambio de estado de pedidos y un latido `EN_RUTA` cada 5 min |
| Kafka | Transporta los eventos en el topic `rapidgo.sla.entregas` (key `order_id`, 3 particiones) |
| Spark Structured Streaming | Lee, valida el contrato, transforma, calcula KPIs por ventana y reglas de alerta |
| Data Lake (Parquet) | Capas raw → processed → curated, con checkpoint |
| Airflow | Valida la calidad, agrega, genera el reporte y evalúa alertas; reintentos y fallo controlado (Semana 3) |
| Docker, GitHub y GitHub Actions | Ejecución local, trazabilidad, integración continua y versiones |

## Contrato del evento

Evento de cambio de estado de un pedido. Detalle de campos y reglas en [`docs/contrato_evento.md`](docs/contrato_evento.md); ejemplos en `samples/eventos_pedido.json` (evento suelto), `samples/eventos_ciclo_vida.jsonl` (ciclo de vida de dos pedidos) y `samples/eventos_entrega_detenida.jsonl` (pedido que termina en `DETENIDO`).

```json
{
  "event_id": "EVT-000100",
  "team_id": "equipo-v1",
  "order_id": "ORD-000125",
  "status": "EN_RUTA",
  "zone": "Centro",
  "distance_km": 4.5,
  "created_at": "2026-10-21T14:00:00Z",
  "promised_at": "2026-10-21T14:45:00Z",
  "event_ts": "2026-10-21T14:20:00Z"
}
```

Estados: `CREADO → ASIGNADO → RECOGIDO → EN_RUTA → ENTREGADO`, más `DETENIDO` y `CANCELADO`.

## Kafka

| Topic | Clave | Particiones | Replicación | Retención |
|---|---|---|---|---|
| `rapidgo.sla.entregas` | `order_id` | 3 | 1 | 7 días |

Valores propuestos; la justificación y la decisión están en el [ADR-001](docs/decisions/ADR-001-key-y-particiones-kafka.md). La distribución real por partición se medirá en la Semana 2.

## Indicadores y alertas (umbrales iniciales)

Detalle completo en [`docs/kpis_y_alertas.md`](docs/kpis_y_alertas.md).

| Indicador | Definición |
|---|---|
| K1 Tiempo promedio de entrega | promedio de `ENTREGADO − created_at` por ventana de 5 min y zona |
| K2 % fuera de SLA | pedidos entregados después de `promised_at` ÷ entregados × 100 |
| K3 Pedidos retrasados | pedidos abiertos con `ahora > promised_at` |

| Alerta | Regla |
|---|---|
| A1 SLA_EXCEDIDO | pedido sin `ENTREGADO`/`CANCELADO` y `ahora > promised_at + 5 min` |
| A2 ENTREGA_DETENIDA | pedido en `EN_RUTA` o `DETENIDO` sin eventos nuevos por más de 10 min |

El SLA se calcula como `promised_at = created_at + 10 min + 8 min × distance_km`, redondeado al múltiplo de 5 min más cercano. Los umbrales son hipótesis iniciales y se revisarán en la Semana 2 con datos reales.

## Ejecución local

Requisitos: Docker Desktop y Python 3.10 o superior.

```powershell
# 1. Levantar Kafka, crear el topic (3 particiones) y arrancar Spark
docker compose up -d
docker compose logs -f spark          # esperar la línea "Streams iniciados" (Ctrl+C para salir del log)

# 2. En otra terminal: generar eventos y publicarlos en Kafka
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m producer.producer --n-pedidos 200 --seed 42

# 3. Ver lo que escribió Spark en datalake/ (raw, rejected, processed, curated)
python spark/leer_datalake.py

# 4. Verificar la distribución por partición (evidencia del ADR-001)
python -m producer.check_partitions --markdown --nota "200 pedidos, --seed 42"

# 5. Apagar (el topic y los datos de Kafka se pierden; datalake/ se conserva)
docker compose down
```

- `python -m producer.producer --help` lista las opciones; con `--dry-run` genera eventos sin usar Kafka.
- Los resultados de `datalake/` no se versionan. Para empezar de cero, ver [`datalake/README.md`](datalake/README.md).
- Las alertas y las ventanas de KPIs más recientes aparecen cuando llegan eventos posteriores (Spark usa el tiempo de los eventos).
- Prueba del job sin Kafka (requiere Java 17): `python tests/integracion_spark.py`.

## Plan de versiones

| Versión | Semana | Contenido |
|---|---|---|
| v0.1.0-foundation | 1 | Arquitectura, contrato, KPIs y alertas, Kanban, ADR-001 |
| v0.2.0-streaming | 2 | Producer + Kafka + Spark + Data Lake + integración continua |
| v0.3.0-orchestration | 3 | DAG de Airflow, calidad, fallo controlado, CI ampliado |
| v1.0.0 | 4 | Quality gate, README final, video |

## Forma de trabajo

Issue → Branch → Pull Request → CI → Review → Merge → Release. No se hacen cambios funcionales directamente sobre `main`.

## Documentación

- Contrato del evento: [`docs/contrato_evento.md`](docs/contrato_evento.md)
- Indicadores y alertas: [`docs/kpis_y_alertas.md`](docs/kpis_y_alertas.md)
- Registros de decisiones (ADR): [`docs/decisions/`](docs/decisions/)
- Checkpoints semanales: [`docs/checkpoints/`](docs/checkpoints/)

## Contribuciones (Semana 1)

| Integrante | Trabajo realizado |
|---|---|
| Ana Karina Arenales Bautista | Contrato JSON y ejemplos, KPIs y alertas, README inicial |
| Angela Maria Palacios Alonso | Creación del repositorio, particiones y topic, revisiones |
| Daniela Gomez Castro | Tablero Kanban |

Se actualizará cada semana. Enlace al video final: pendiente (Semana 4).
