import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
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
        
    # Reducir logs para ver mejor la salida en consola
    spark.sparkContext.setLogLevel("WARN")

    print("Iniciando lectura de Kafka...")

    # 2. Leer de Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "rapidgo.sla.entregas") \
        .option("startingOffsets", "latest") \
        .load()

    # 3. Parsear el JSON y convertir timestamps a tipo Timestamp real
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Convertir strings ISO 8601 (ej. 2026-10-21T14:20:00Z) a Timestamps
    final_df = parsed_df.withColumn(
        "event_ts", to_timestamp(col("event_ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    ).withColumn(
        "created_at", to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    ).withColumn(
        "promised_at", to_timestamp(col("promised_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )

    # 4. Validar el contrato (Tarea 3)
    # Reglas: distance_km > 0, status permitido, y event_ts >= created_at
    valid_status = ["CREADO", "ASIGNADO", "RECOGIDO", "EN_RUTA", "ENTREGADO", "DETENIDO", "CANCELADO"]
    
    # Filtro de eventos VÁLIDOS (Pasan a processed)
    valid_df = final_df.filter(
        (col("distance_km") > 0) & 
        (col("status").isin(valid_status)) &
        (col("event_ts") >= col("created_at"))
    )

    # Filtro de eventos INVÁLIDOS (Se van a raw/cuarentena)
    invalid_df = final_df.filter(
        (col("distance_km") <= 0) | 
        (~col("status").isin(valid_status)) |
        (col("event_ts") < col("created_at"))
    )

    # 5. Escribir válidos en 'processed' (Tarea 7 - parte 1)
    query_processed = valid_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/opt/spark/datalake/processed/eventos") \
        .option("checkpointLocation", "/opt/spark/datalake/checkpoints/processed") \
        .start()

    # 6. Escribir inválidos en 'raw' (cuarentena)
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