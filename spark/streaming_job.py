"""Job de Spark Structured Streaming de RapidGo: Kafka -> Data Lake (raw / processed / curated).

Capas (carpeta --datalake-dir, por defecto /opt/spark/datalake dentro del contenedor):
  raw/eventos         TODO lo que llega, tal cual (texto original + metadatos de Kafka)
  rejected/eventos    eventos que no cumplen el contrato, con el motivo del rechazo
  processed/eventos   eventos validos, tipados, transformados y sin duplicados
  curated/kpis        K1 y K2 por ventana de 5 min y zona (solo pedidos ENTREGADO)
  curated/alertas     K3_RETRASADO, A1_SLA_EXCEDIDO y A2_ENTREGA_DETENIDA (ver alertas_estado.py)
  checkpoints/<query> un checkpoint por cada salida

Uso en el contenedor (ver docker-compose.yml):
  spark-submit ... streaming_job.py --source kafka --bootstrap kafka:29092

Prueba sin Kafka (un archivo = un micro-lote; --una-vez procesa todo y termina):
  python spark/streaming_job.py --source files --input-dir eventos/ --datalake-dir /tmp/dl --una-vez

Si cambias el codigo de una consulta, borra datalake/checkpoints/ antes de volver a correr.
"""
import argparse
import os
import sys
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import StringType, StructField, StructType

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from alertas_estado import ESTADO_DDL, SALIDA_DDL, procesar_pedido  # noqa: E402

ESTADOS_VALIDOS = ["CREADO", "ASIGNADO", "RECOGIDO", "EN_RUTA", "ENTREGADO", "DETENIDO", "CANCELADO"]
CAMPOS = ["event_id", "team_id", "order_id", "status", "zone", "distance_km",
          "created_at", "promised_at", "event_ts"]
# Todo se lee como texto: un valor mal escrito (ej. distance_km = "abc") no tumba la fila,
# se rechaza con su motivo en lugar de perderse.
ESQUEMA = StructType([StructField(c, StringType(), True) for c in CAMPOS])
FORMATO_TS = "yyyy-MM-dd'T'HH:mm:ss'Z'"
REGEX_TS = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"


def leer_argumentos(argv=None):
    p = argparse.ArgumentParser(description="Streaming de eventos de pedidos de RapidGo")
    p.add_argument("--source", choices=["kafka", "files"], default="kafka")
    p.add_argument("--bootstrap", default="kafka:29092", help="listener INTERNAL del compose")
    p.add_argument("--topic", default="rapidgo.sla.entregas")
    p.add_argument("--starting-offsets", default="earliest",
                   help="earliest lee todo lo que ya hay en el topic; con checkpoint solo cuenta la 1a vez")
    p.add_argument("--max-offsets-por-lote", type=int, default=None)
    p.add_argument("--input-dir", default=None, help="carpeta de .jsonl (solo con --source files)")
    p.add_argument("--datalake-dir", default="/opt/spark/datalake")
    p.add_argument("--watermark-min", type=int, default=5, help="tolerancia a eventos tardios (min)")
    p.add_argument("--trigger-seg", type=int, default=10, help="cada cuantos segundos corre un micro-lote")
    p.add_argument("--una-vez", action="store_true", help="procesa lo disponible y termina")
    p.add_argument("--sin-validar-sla", action="store_true",
                   help="no exige promised_at = created_at + SLA (por defecto si se exige)")
    p.add_argument("--shuffle-partitions", type=int, default=4)
    return p.parse_args(argv)


def leer_origen(spark, args):
    """DataFrame con: value (texto original), kafka_partition, kafka_offset, kafka_timestamp, ingested_at."""
    if args.source == "kafka":
        lector = (spark.readStream.format("kafka")
                  .option("kafka.bootstrap.servers", args.bootstrap)
                  .option("subscribe", args.topic)
                  .option("startingOffsets", args.starting_offsets)
                  .option("failOnDataLoss", "false"))
        if args.max_offsets_por_lote:
            lector = lector.option("maxOffsetsPerTrigger", args.max_offsets_por_lote)
        df = lector.load().select(
            F.col("value").cast("string").alias("value"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
    else:
        if not args.input_dir:
            sys.exit("--source files necesita --input-dir")
        df = (spark.readStream.format("text").option("maxFilesPerTrigger", 1).load(args.input_dir)
              .select("value",
                      F.lit(None).cast("int").alias("kafka_partition"),
                      F.lit(None).cast("long").alias("kafka_offset"),
                      F.lit(None).cast("timestamp").alias("kafka_timestamp")))
    return df.withColumn("ingested_at", F.current_timestamp())


def parsear(raw):
    """Convierte el texto JSON en columnas tipadas. Lo que no se pueda convertir queda en null."""
    p = raw.withColumn("data", F.from_json(F.col("value"), ESQUEMA))
    for campo in CAMPOS:
        p = p.withColumn(campo, F.col("data." + campo))
    p = p.withColumn("distance_km_n", F.col("distance_km").cast("double"))
    for campo in ("created_at", "promised_at", "event_ts"):
        p = p.withColumn(
            campo + "_t",
            F.when(F.col(campo).rlike(REGEX_TS), F.to_timestamp(F.col(campo), FORMATO_TS)))
    return p


def agregar_motivos(df, validar_sla=True):
    """Columna 'motivos' (array de texto): vacia si el evento cumple el contrato.

    Un valor null NUNCA cuenta como valido: antes, los eventos con campos nulos
    desaparecian porque ni el filtro de validos ni el de invalidos los dejaba pasar.
    """
    d = F.col("distance_km_n")
    creado, prometido, evento = F.col("created_at_t"), F.col("promised_at_t"), F.col("event_ts_t")

    def falla(ok):
        return ~F.coalesce(ok, F.lit(False))

    def con_texto(c):
        return F.length(F.trim(F.col(c))) > 0

    fechas_ok = creado.isNotNull() & prometido.isNotNull() & evento.isNotNull()
    minutos_sla = F.floor(F.round((F.lit(10) + F.lit(8) * d) / 5, 6) + 0.5) * 5
    segundos_prometidos = F.unix_timestamp(prometido) - F.unix_timestamp(creado)

    reglas = [(c + "_faltante", falla(con_texto(c))) for c in ("event_id", "team_id", "order_id", "zone")]
    reglas += [
        ("status_invalido", falla(F.col("status").isin(ESTADOS_VALIDOS))),
        ("distance_km_invalido", falla(d > 0)),
        ("created_at_invalido", creado.isNull()),
        ("promised_at_invalido", prometido.isNull()),
        ("event_ts_invalido", evento.isNull()),
        ("event_ts_anterior_a_created_at", fechas_ok & (evento < creado)),
        ("promised_at_no_posterior_a_created_at", fechas_ok & (prometido <= creado)),
    ]
    if validar_sla:
        reglas.append(("promised_at_no_coincide_con_sla",
                       fechas_ok & (d > 0) & (segundos_prometidos != minutos_sla * 60)))
    todos = F.filter(F.array(*[F.when(cond, F.lit(nombre)) for nombre, cond in reglas]),
                     lambda x: x.isNotNull())
    # from_json NO devuelve null con un JSON roto: devuelve una fila con todos los campos en null.
    sin_campos = reduce(lambda a, b: a & b, [F.col(c).isNull() for c in CAMPOS])
    motivos = F.when(F.col("data").isNull() | sin_campos, F.array(F.lit("json_invalido"))).otherwise(todos)
    return df.withColumn("motivos", motivos)


def transformar(validos):
    """Eventos validos -> columnas tipadas + transformaciones de negocio."""
    desde_creacion = (F.unix_timestamp("event_ts_t") - F.unix_timestamp("created_at_t")) / 60
    contra_promesa = (F.unix_timestamp("event_ts_t") - F.unix_timestamp("promised_at_t")) / 60
    return validos.select(
        "event_id", "team_id", "order_id", "status",
        F.initcap(F.trim("zone")).alias("zone"),                        # T1: zona normalizada
        F.col("distance_km_n").alias("distance_km"),
        F.col("created_at_t").alias("created_at"),
        F.col("promised_at_t").alias("promised_at"),
        F.col("event_ts_t").alias("event_ts"),
        desde_creacion.alias("minutos_desde_creacion"),                  # T2
        contra_promesa.alias("minutos_vs_promesa"),                      # T3 (+ = despues de lo prometido)
        F.when(F.col("status") == "ENTREGADO",                           # T4: solo tiene sentido al entregar
               F.col("event_ts_t") > F.col("promised_at_t")).alias("fuera_sla"),
        "kafka_partition", "kafka_offset", "ingested_at",
    )


def escribir(df, nombre, carpeta, args):
    escritor = (df.writeStream.queryName(nombre).outputMode("append").format("parquet")
                .option("path", os.path.join(args.datalake_dir, carpeta))
                .option("checkpointLocation", os.path.join(args.datalake_dir, "checkpoints", nombre)))
    if args.una_vez:
        escritor = escritor.trigger(availableNow=True)
    else:
        escritor = escritor.trigger(processingTime=f"{args.trigger_seg} seconds")
    return escritor.start()


def main(argv=None):
    args = leer_argumentos(argv)
    spark = (SparkSession.builder.appName("RapidGo-SLA-Streaming")
             .config("spark.sql.session.timeZone", "UTC")   # los timestamps del contrato son UTC
             .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.addPyFile(os.path.join(os.path.dirname(os.path.abspath(__file__)), "alertas_estado.py"))

    raw = leer_origen(spark, args)
    parseado = agregar_motivos(parsear(raw), validar_sla=not args.sin_validar_sla)

    # 1) raw: todo lo que llega, sin tocar
    q_raw = escribir(raw, "raw", "raw/eventos", args)

    # 2) rejected: lo que incumple el contrato, con el motivo
    rechazados = parseado.filter(F.size("motivos") > 0).select(
        "value", F.array_join("motivos", ",").alias("motivos"),
        "kafka_partition", "kafka_offset", "ingested_at")
    q_rej = escribir(rechazados, "rejected", "rejected/eventos", args)

    # 3) processed: validos + transformaciones + deduplicacion (event_ts en la clave para que
    #    Spark pueda borrar el estado viejo con el watermark)
    procesados = (transformar(parseado.filter(F.size("motivos") == 0))
                  .withWatermark("event_ts", f"{args.watermark_min} minutes")
                  .dropDuplicates(["event_id", "event_ts"]))
    q_proc = escribir(procesados, "processed", "processed/eventos", args)

    # 4) curated/kpis: K1 y K2 solo sobre eventos ENTREGADO
    kpis = (procesados.filter(F.col("status") == "ENTREGADO")
            .groupBy(F.window("event_ts", "5 minutes").alias("ventana"), "zone")
            .agg(F.count("*").alias("pedidos_entregados"),
                 F.round(F.avg("minutos_desde_creacion"), 2).alias("k1_tiempo_promedio_min"),
                 F.round(F.sum(F.when(F.col("fuera_sla"), 1).otherwise(0)) * 100.0 / F.count("*"), 2)
                 .alias("k2_pct_fuera_sla"))
            .select(F.col("ventana.start").alias("ventana_inicio"), F.col("ventana.end").alias("ventana_fin"),
                    "zone", "pedidos_entregados", "k1_tiempo_promedio_min", "k2_pct_fuera_sla"))
    q_kpi = escribir(kpis, "curated_kpis", "curated/kpis", args)

    # 5) curated/alertas: K3, A1 y A2 con estado por pedido y timeout por tiempo del evento
    entrada = (procesados.withColumn("event_s", F.unix_timestamp("event_ts"))
               .withColumn("promised_s", F.unix_timestamp("promised_at")))
    alertas = (entrada.groupBy("order_id")
               .applyInPandasWithState(procesar_pedido, outputStructType=SALIDA_DDL,
                                       stateStructType=ESTADO_DDL, outputMode="append",
                                       timeoutConf=GroupStateTimeout.EventTimeTimeout)
               .select("order_id", "alerta", "zone", F.col("momento_s").cast("timestamp").alias("momento")))
    q_alert = escribir(alertas, "curated_alertas", "curated/alertas", args)

    consultas = [q_raw, q_rej, q_proc, q_kpi, q_alert]
    print("Streams iniciados:", ", ".join(q.name for q in consultas), flush=True)
    if args.una_vez:
        for q in consultas:
            q.awaitTermination()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
