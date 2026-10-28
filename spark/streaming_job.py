import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, when, unix_timestamp, window, avg, sum, count, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Definir el esquema según contrato_evento.md
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("team_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("created_at", StringType(), True),
    StructField("promised_at", StringType(), True),
    StructField("event_ts", StringType(), True)
])

def main():
    # Inicializar Spark
    spark = SparkSession.builder \
        .appName("RapidGo-SLA-Streaming") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    print("Iniciando lectura de Kafka...")

    # 2. Leer de Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "rapidgo.sla.entregas") \
        .option("startingOffsets", "latest") \
        .load()

    # 3. Parsear el JSON y convertir timestamps
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    final_df = parsed_df.withColumn(
        "event_ts", to_timestamp(col("event_ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    ).withColumn(
        "created_at", to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    ).withColumn(
        "promised_at", to_timestamp(col("promised_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )

    # 4. Validar el contrato (Tarea 3)
    valid_status = ["CREADO", "ASIGNADO", "RECOGIDO", "EN_RUTA", "ENTREGADO", "DETENIDO", "CANCELADO"]
    
    valid_df = final_df.filter(
        (col("distance_km") > 0) & 
        (col("status").isin(valid_status)) &
        (col("event_ts") >= col("created_at"))
    )

    invalid_df = final_df.filter(
        (col("distance_km") <= 0) | 
        (~col("status").isin(valid_status)) |
        (col("event_ts") < col("created_at"))
    )

    # --- TAREA 4 de ANNA : Transformaciones ---
    # Transformación 1: Minutos de retraso
    transformed_df = valid_df.withColumn(
        "delay_minutes",
        (unix_timestamp(col("event_ts")) - unix_timestamp(col("promised_at"))) / 60
    )
    
    # Transformación 2: Bandera fuera de SLA
    transformed_df = transformed_df.withColumn(
        "is_out_of_sla",
        when(col("delay_minutes") > 0, True).otherwise(False)
    )
    
    # Transformación 3: Deduplicación por event_id con watermark
    dedup_df = transformed_df.withWatermark("event_ts", "10 minutes") \
        .dropDuplicates(["event_id"])

    # 5. Escribir en 'processed' los datos transformados
    query_processed = dedup_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/opt/spark/datalake/processed/eventos") \
        .option("checkpointLocation", "/opt/spark/datalake/checkpoints/processed") \
        .start()

    # --- TAREA 5: KPIs por ventana de 5 min y zona (K1 y K2) ---
    
    # Filtrar solo ENTREGADOS para KPIs
    entregados_df = dedup_df.filter(col("status") == "ENTREGADO")
    
    kpis_df = entregados_df \
        .withWatermark("event_ts", "10 minutes") \
        .groupBy(
            window(col("event_ts"), "5 minutes"),
            col("zone")
        ) \
        .agg(
            # K1: Tiempo promedio de entrega (usando delay_minutes)
            round(avg("delay_minutes"), 2).alias("K1_tiempo_promedio_entrega"),
            # K2: % fuera de SLA
            round(
                (sum(when(col("is_out_of_sla") == True, 1).otherwise(0)) / count("*")) * 100, 
                2
            ).alias("K2_porcentaje_fuera_sla")
        )

    # Escribir KPIs en 'curated'
    query_curated = kpis_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/opt/spark/datalake/curated/kpis") \
        .option("checkpointLocation", "/opt/spark/datalake/checkpoints/curated_kpis") \
        .start()

    # 6. Escribir inválidos en 'raw'
    query_raw = invalid_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/opt/spark/datalake/raw/invalidos") \
        .option("checkpointLocation", "/opt/spark/datalake/checkpoints/raw_invalidos") \
        .start()

    print("Streams iniciados. Esperando datos...")
    
    # Mantener la aplicación corriendo
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()