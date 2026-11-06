"""Muestra cuántos registros hay en cada capa del Data Lake y un ejemplo de cada una.

  python spark/leer_datalake.py                  # usa ./datalake
  python spark/leer_datalake.py ruta/al/datalake

Requiere pandas y pyarrow (ya estan en requirements.txt).
"""
import os
import sys

import pandas as pd

CAPAS = ["raw/eventos", "rejected/eventos", "processed/eventos", "curated/kpis", "curated/alertas"]


def main():
    base = sys.argv[1] if len(sys.argv) > 1 else "datalake"
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.max_colwidth", 60)
    for capa in CAPAS:
        ruta = os.path.join(base, capa)
        print(f"\n=== {capa} ===")
        try:
            df = pd.read_parquet(ruta)
        except Exception as error:  # noqa: BLE001
            print(f"(sin datos todavia: {type(error).__name__})")
            continue
        print(f"{len(df)} registros")
        if "value" in df.columns:  # el JSON original es largo: mostrar solo el inicio
            df["value"] = df["value"].str.slice(0, 70)
        if capa == "curated/alertas" and len(df):
            print(df.groupby("alerta").size().to_string())
        print(df.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
