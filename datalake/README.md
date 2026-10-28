# Data Lake: raw / processed / curated

## Estructura
- `raw/`: Eventos inválidos o que no cumplen el contrato (cuarentena).
- `processed/`: Eventos válidos, parseados, con transformaciones (`delay_minutes`, `is_out_of_sla`) y deduplicados.
- `curated/`: Resultados de negocio agregados:
  - `kpis/`: Indicadores K1 (tiempo promedio de entrega) y K2 (% fuera de SLA) calculados por ventana de 5 minutos y zona.
  - `alertas/`: Alertas A1 (SLA_EXCEDIDO) y A2 (ENTREGA_DETENIDA) calculadas con estado por pedido.

## Checkpoints
Los checkpoints se guardan en `datalake/checkpoints/` para garantizar el procesamiento exactly-once y permitir la recuperación ante fallos. Cada stream (`processed`, `curated_kpis`, `curated_alertas`, `raw_invalidos`) tiene su propio directorio de checkpoint.
