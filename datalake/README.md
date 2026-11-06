# Data Lake: raw / rejected / processed / curated

Lo escribe `spark/streaming_job.py` en formato Parquet. **Estas carpetas no se versionan**
(están en `.gitignore`); se regeneran al correr el job. Solo este README va al repositorio.

## Capas

| Carpeta | Contenido |
|---|---|
| `raw/eventos/` | Todo lo que llega de Kafka, tal cual: texto JSON original, partición, offset y hora de ingesta. Sirve para reprocesar. |
| `rejected/eventos/` | Eventos que incumplen el contrato, con el texto original y la columna `motivos` (ej. `distance_km_invalido`, `promised_at_no_coincide_con_sla`). |
| `processed/eventos/` | Eventos válidos y sin duplicados, con tipos correctos y estas columnas nuevas: `zone` normalizada, `minutos_desde_creacion`, `minutos_vs_promesa` y `fuera_sla` (solo en `ENTREGADO`). |
| `curated/kpis/` | K1 (tiempo promedio de entrega) y K2 (% fuera de SLA) por ventana de 5 minutos y zona, calculados solo con eventos `ENTREGADO`. |
| `curated/alertas/` | Avisos `K3_RETRASADO`, `A1_SLA_EXCEDIDO` y `A2_ENTREGA_DETENIDA`: uno por pedido y regla (A2 puede repetirse si el pedido vuelve a detenerse). |

El detalle de cada regla está en [`docs/kpis_y_alertas.md`](../docs/kpis_y_alertas.md).

## Checkpoints

`checkpoints/<consulta>/` guarda hasta dónde leyó cada salida (`raw`, `rejected`, `processed`,
`curated_kpis`, `curated_alertas`), de modo que al reiniciar el job continúa sin duplicar datos.

**Si cambias el código de una consulta, borra `datalake/checkpoints/`** antes de volver a
correr; un checkpoint viejo no es compatible con un plan distinto.

## Empezar de cero

```powershell
docker compose down
Remove-Item -Recurse -Force datalake\raw, datalake\rejected, datalake\processed, datalake\curated, datalake\checkpoints -ErrorAction SilentlyContinue
docker compose up -d
```

## Revisar lo que se escribió

```powershell
python spark/leer_datalake.py
```

## Por qué algunos resultados tardan en aparecer

Spark usa el **tiempo de los eventos**, no el reloj del computador. Una ventana de KPIs se cierra, y
un aviso por inactividad se emite, cuando llegan eventos posteriores que hacen avanzar el
watermark (`--watermark-min`, 5 por defecto). Con pocos eventos, los últimos minutos quedan
pendientes hasta que llegue más tráfico.
