import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, when, unix_timestamp, window, avg, sum, count, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import pandas as pd
from datetime import timedelta

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

# --- TAREA 6: Función de estado para alertas ---
def update_state_and_check_alerts(key, pdf_iter, state):
    # key es una tupla (order_id,)
    order_id = key[0]
    
    # Convertimos el iterador de Pandas a una lista para procesarlo
    pdf = list(pdf_iter)
    if not pdf:
        return iter([])
    
    # Tomamos el DataFrame combinado
    df = pd.concat(pdf)
    
    # Ordenamos por event_ts para asegurar el orden
    df = df.sort_values(by="event_ts")
    last_event = df.iloc[-1]
    
    # Actualizamos o inicializamos el estado
    if not state.exists:
        # Primer evento que vemos de este pedido
        new_state = {
            "last_status": last_event["status"],
            "last_event_ts": last_event["event_ts"],
            "promised_at": last_event["promised_at"],
            "zone": last_event["zone"]
        }
        state.update(new_state)
    else:
        # Actualizamos con el evento más reciente
        state.update({
            "last_status": last_event["status"],
            "last_event_ts": last_event["event_ts"],
            "promised_at": last_event["promised_at"],
            "zone": last_event["zone"]
        })
    
    current_state = state.get
    alerts = []
    
    # Solo evaluamos si el pedido NO está cerrado
    if current_state["last_status"] not in ["ENTREGADO", "CANCELADO"]:
        
        # --- ALERTA A1: SLA_EXCEDIDO ---
        # Si el último event_ts supera promised_at + 5 min
        sla_limit = current_state["promised_at"] + timedelta(minutes=5)
        if current_state["last_event_ts"] > sla_limit:
            alerts.append({
                "order_id": order_id,
                "alerta": "A1_SLA_EXCEDIDO",
                "event_ts": current_state["last_event_ts"],
                "zone": current_state["zone"]
            })
        
        # --- ALERTA A2: ENTREGA_DETENIDA ---
        # Si el pedido está EN_RUTA o DETENIDO
        if current_state["last_status"] in ["EN_RUTA", "DETENIDO"]:
            alerts.append({
                "order_id": order_id,
                "alerta": "A2_ENTREGA_DETENIDA",
                "event_ts": current_state["last_event_ts"],
                "zone": current_state["zone"]
            })
            
    return iter([pd.DataFrame(alerts)])

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
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "rapidgo.sla.entregas") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
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

    # --- TAREA 4: Transformaciones ---
    transformed_df = valid_df.withColumn(
        "delay_minutes",
        (unix_timestamp(col("event_ts")) - unix_timestamp(col("promised_at"))) / 60
    ).withColumn(
        "is_out_of_sla",
        when(col("delay_minutes") > 0, True).otherwise(False)
    )
    
    dedup_df = transformed_df.withWatermark("event_ts", "10 minutes") \
        .dropDuplicates(["event_id"])

    # 5. Escribir en 'processed' los datos transformados
    query_processed = dedup_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/opt/spark/datalake/processed/eventos") \
        .option("checkpointLocation", "/opt/spark/datalake/checkpoints/processed") \
        .start()

    # --- TAREA 5: KPIs ---
    entregados_df = dedup_df.filter(col("status") == "ENTREGADO")
    
    kpis_df = entregados_df \
        .withWatermark("event_ts", "10 minutes") \
        .groupBy(
            window(col("event_ts"), "5 minutes"),
            col("zone")
        ) \
        .agg(
            round(avg("delay_minutes"), 2).alias("K1_tiempo_promedio_entrega"),
            round(
                (sum(when(col("is_out_of_sla") == True, 1).otherwise(0)) / count("*")) * 100, 
                2
            ).alias("K2_porcentaje_fuera_sla")
        )

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

    # --- TAREA 6: Alertas con estado ---
    
    from pyspark.sql.streaming.state import GroupStateTimeout
    

    alerts_df = dedup_df \
        .groupBy("order_id") \
        .applyInPandasWithState(
            update_state_and_check_alerts,
            outputStructType="order_id string, alerta string, event_ts timestamp, zone string",
            stateStructType="last_status string, last_event_ts timestamp, promised_at timestamp, zone string",
            outputMode="append",
            timeoutConf=GroupStateTimeout.NoTimeout 
        )

    # Escribir alertas en 'curated/alertas'
    query_alerts = alerts_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/opt/spark/datalake/curated/alertas") \
        .option("checkpointLocation", "/opt/spark/datalake/checkpoints/curated_alertas") \
        .start()

    print("Streams iniciados. Esperando datos...")
    
    # Mantener la aplicación corriendo
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()