from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, when, unix_timestamp, window, avg, sum, count, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pandas as pd
from datetime import timedelta
import calendar
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
TOLERANCIA_A1 = timedelta(minutes=5)
SILENCIO_A2 = timedelta(minutes=10)
ESTADOS_CERRADOS = ["ENTREGADO", "CANCELADO"]
ESTADOS_EN_MOVIMIENTO = ["EN_RUTA", "DETENIDO"]


def _a_ms(dt):
    """datetime (UTC, sin zona) -> milisegundos desde 1970."""
    return calendar.timegm(dt.timetuple()) * 1000 + dt.microsecond // 1000


def update_state_and_check_alerts(key, pdf_iter, state):
    order_id = key[0]

    if state.hasTimedOut:
        # El watermark pasó un plazo y no llegaron eventos nuevos del pedido.
        last_status, last_event_ts, promised_at, zone, a1_sent, a2_sent = state.get
        ahora = pd.Timestamp(state.getCurrentWatermarkMs(), unit="ms").to_pydatetime()
    else:
        pdfs = list(pdf_iter)
        if not pdfs:
            return iter([])
        last_event = pd.concat(pdfs).sort_values(by="event_ts").iloc[-1]
        a1_sent = a2_sent = False
        if state.exists:
            old_status, old_ts, old_prom, old_zone, a1_sent, a2_sent = state.get
            if old_ts > last_event["event_ts"].to_pydatetime():
                last_event = pd.Series({
                    "status": old_status,
                    "event_ts": pd.Timestamp(old_ts),
                    "promised_at": pd.Timestamp(old_prom),
                    "zone": old_zone,
                })
        last_status = last_event["status"]
        last_event_ts = last_event["event_ts"].to_pydatetime()
        promised_at = last_event["promised_at"].to_pydatetime()
        zone = last_event["zone"]
        ahora = last_event_ts  # el propio evento ya ocurrió

    # Pedido cerrado: no hay alertas y se libera el estado
    if last_status in ESTADOS_CERRADOS:
        state.remove()
        return iter([])

    alerts = []
    # --- ALERTA A1: SLA_EXCEDIDO ---
    if not a1_sent and ahora > promised_at + TOLERANCIA_A1:
        alerts.append("A1_SLA_EXCEDIDO")
        a1_sent = True
    # --- ALERTA A2: ENTREGA_DETENIDA (más de 10 min sin eventos) ---
    if (not a2_sent and last_status in ESTADOS_EN_MOVIMIENTO
            and ahora - last_event_ts > SILENCIO_A2):
        alerts.append("A2_ENTREGA_DETENIDA")
        a2_sent = True

    # Próximo plazo en el que hay que volver a mirar este pedido
    plazos = []
    if not a1_sent:
        plazos.append(promised_at + TOLERANCIA_A1)
    if not a2_sent and last_status in ESTADOS_EN_MOVIMIENTO:
        plazos.append(last_event_ts + SILENCIO_A2)

    if plazos:
        state.update((last_status, last_event_ts, promised_at, zone, a1_sent, a2_sent))
        state.setTimeoutTimestamp(_a_ms(min(plazos)))
    else:
        state.remove()

    if not alerts:
        return iter([])
    return iter([pd.DataFrame([
        {"order_id": order_id, "alerta": a, "event_ts": last_event_ts, "zone": zone}
        for a in alerts
    ])])



def main():
    # Inicialización de la sesión de Spark para Streaming
    spark = SparkSession.builder \
        .appName("RapidgoSLAStreaming") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # 1. Lectura del flujo de datos desde Kafka
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
    dedup_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/opt/spark/datalake/processed/eventos") \
        .option("checkpointLocation", "/opt/spark/datalake/checkpoints/processed") \
        .start()

    # --- TAREA 5: KPIs ---
    entregados_df = dedup_df.filter(col("status") == "ENTREGADO")
    
    kpis_df = entregados_df \
        .groupBy(
            window(col("event_ts"), "5 minutes"),
            col("zone")
        ) \
        .agg(
            round(avg("delay_minutes"), 2).alias("K1_tiempo_promedio_entrega"),
            round(
                (sum(when(col("is_out_of_sla"), 1).otherwise(0)) / count("*")) * 100, 
                2
            ).alias("K2_porcentaje_fuera_sla")
        )

    kpis_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/opt/spark/datalake/curated/kpis") \
        .option("checkpointLocation", "/opt/spark/datalake/checkpoints/curated_kpis") \
        .start()

    # 6. Escribir inválidos
    invalid_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/opt/spark/datalake/raw/invalidos") \
        .option("checkpointLocation", "/opt/spark/datalake/checkpoints/invalidos") \
        .start()

    # --- TAREA 6: Alertas con estado ---
    from pyspark.sql.streaming.state import GroupStateTimeout

    alerts_df = dedup_df \
        .groupBy("order_id") \
        .applyInPandasWithState(
            update_state_and_check_alerts,
            outputStructType="order_id string, alerta string, event_ts timestamp, zone string",
            stateStructType="last_status string, last_event_ts timestamp, promised_at timestamp, zone string, a1_sent boolean, a2_sent boolean",
            outputMode="append",
            timeoutConf=GroupStateTimeout.EventTimeTimeout
        )

    # Escribir alertas en 'curated/alertas'
    alerts_df.writeStream \
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
