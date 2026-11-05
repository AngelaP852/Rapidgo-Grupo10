from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, unix_timestamp, when, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def main():
    # Inicialización de la sesión de Spark para Streaming
    spark = SparkSession.builder \
        .appName("RapidgoSLAStreaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Definición del esquema de contrato del evento
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("event_ts", TimestampType(), True),
        StructField("promised_at", TimestampType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True)
    ])

    # 1. Lectura del flujo de datos desde Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "rapidgo.sla.entregas") \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parseo del JSON a columnas estructuradas
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema).alias("data")) \
        .select("data.*")

    # 3. Aplicación de transformaciones y reglas de negocio / alertas corregidas
    # A1: Pedido activo cuyo event_ts sobrepasa el promised_at
    # A2: Pedido EN_RUTA o DETENIDO con más de 10 minutos (600 segundos) de inactividad respecto al tiempo actual
    processed_df = parsed_df.withColumn(
        "alerta_a1",
        when(
            (col("status").isin("ASIGNADO", "EN_RUTA")) & 
            (col("event_ts") > col("promised_at")), 
            lit(True)
        ).otherwise(lit(False))
    ).withColumn(
        "alerta_a2",
        when(
            (col("status").isin("EN_RUTA", "DETENIDO")) & 
            ((unix_timestamp(current_timestamp()) - unix_timestamp(col("event_ts"))) > 600), 
            lit(True)
        ).otherwise(lit(False))
    )

    # 4. Escritura en formato Parquet en el Data Lake con soporte de Checkpoint
    query = processed_df.writeStream \
        .format("parquet") \
        .option("path", "/app/datalake/silver/sla_entregas") \
        .option("checkpointLocation", "/app/datalake/checkpoint/sla_entregas") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
