# RapidGo – Variante 1: SLA de entregas

Proyecto Final – Línea de Profundización I: Big Data
Docente: Oscar Jobany Gomez Ochoa 
Ingeniería de Sistemas y Computación, Facatativá.


## Equipo (Grupo 10)

- Daniela Gomez Castro
- Angela Maria Palacios Alonso
- Ana Karina Arenales Bautista

## Problema de negocio

RapidGo necesita detectar retrasos e incumplimientos de los tiempos
prometidos a sus clientes. Esto es importante para mantener un mejor
orden y dar un mejor servicio a los clientes que usan la aplicación.

## Valor de la solución

Detectar en tiempo casi real los pedidos en riesgo de incumplir el SLA
y medir el cumplimiento por zona para actuar a tiempo.

## Arquitectura

Fuente de eventos → Kafka → Spark Structured Streaming →
Data Lake (Raw → Processed → Curated) → Airflow → Indicadores y alertas

_El diagrama (docs/arquitectura.png) se agregará cuando esté listo._

## Tecnologías

| Tecnología | Rol |
|---|---|
| Kafka | Transporte de eventos |
| Spark Structured Streaming | Procesamiento en tiempo real |
| Data Lake (Parquet) | Almacenamiento por capas |
| Airflow | Orquestación y control de calidad |
| Docker | Ejecución local |
| GitHub Actions | Integración continua |

## Estado del proyecto

- [ ] Semana 1 – Diseño (v0.1.0-foundation)
- [ ] Semana 2 – Streaming y CI (v0.2.0-streaming)
- [ ] Semana 3 – Airflow y calidad (v0.3.0-orchestration)
- [ ] Semana 4 – Release final (v1.0.0)