"""Estado por pedido para K3, A1 y A2 (se usa con applyInPandasWithState).

Todo el tiempo se maneja en SEGUNDOS (epoch, UTC) y en tiempo del evento, no del reloj:
la hora "actual" de un pedido es la de su ultimo evento o la del watermark del stream.

Reglas (docs/kpis_y_alertas.md):
  K3_RETRASADO         el pedido sigue abierto y ahora > promised_at
  A1_SLA_EXCEDIDO      el pedido sigue abierto y ahora > promised_at + 5 min
  A2_ENTREGA_DETENIDA  pedido EN_RUTA/DETENIDO sin eventos nuevos por mas de 10 min

Cada aviso se emite UNA sola vez por pedido (las banderas k3/a1/a2 viven en el estado).
A2 se reinicia si el pedido vuelve a emitir un evento.

Uso en streaming_job.py:

    from pyspark.sql.streaming.state import GroupStateTimeout
    from alertas_estado import procesar_pedido, SALIDA_DDL, ESTADO_DDL

    entrada = (dedup_df
               .withColumn("event_s", unix_timestamp("event_ts"))
               .withColumn("promised_s", unix_timestamp("promised_at")))
    alertas = (entrada.groupBy("order_id")
               .applyInPandasWithState(procesar_pedido,
                                       outputStructType=SALIDA_DDL,
                                       stateStructType=ESTADO_DDL,
                                       outputMode="append",
                                       timeoutConf=GroupStateTimeout.EventTimeTimeout))
    # momento_s -> timestamp: alertas.withColumn("momento", col("momento_s").cast("timestamp"))

Requiere withWatermark("event_ts", ...) antes del groupBy y
spark.sql.session.timeZone=UTC.
"""
import pandas as pd

TERMINALES = ("ENTREGADO", "CANCELADO")
EN_RUTA_O_DETENIDO = ("EN_RUTA", "DETENIDO")
A1_TOLERANCIA_S = 5 * 60
A2_UMBRAL_S = 10 * 60

SALIDA_DDL = "order_id string, alerta string, zone string, momento_s long"
ESTADO_DDL = ("status string, zone string, promised_s long, last_s long, "
              "k3 boolean, a1 boolean, a2 boolean")
COLUMNAS_SALIDA = ["order_id", "alerta", "zone", "momento_s"]


def k3_se_cumple(status, promised_s, ahora):
    """K3: el pedido sigue abierto y ya paso promised_at."""
    return status not in TERMINALES and ahora > promised_s


def a1_se_cumple(status, promised_s, ahora):
    """A1: el pedido sigue abierto y ya paso promised_at + 5 min (limite estricto)."""
    return status not in TERMINALES and ahora > promised_s + A1_TOLERANCIA_S


def a2_se_cumple(status, last_s, ahora):
    """A2: EN_RUTA o DETENIDO y mas de 10 min sin eventos (limite estricto)."""
    return status in EN_RUTA_O_DETENIDO and ahora > last_s + A2_UMBRAL_S


def _evaluar(order_id, ahora, status, zone, promised_s, last_s, k3, a1, a2, salidas):
    """Emite los avisos que ya se cumplieron en 'ahora'. Devuelve las banderas nuevas."""
    if not k3 and k3_se_cumple(status, promised_s, ahora):
        salidas.append((order_id, "K3_RETRASADO", zone, promised_s))
        k3 = True
    if not a1 and a1_se_cumple(status, promised_s, ahora):
        salidas.append((order_id, "A1_SLA_EXCEDIDO", zone, promised_s + A1_TOLERANCIA_S))
        a1 = True
    if not a2 and a2_se_cumple(status, last_s, ahora):
        salidas.append((order_id, "A2_ENTREGA_DETENIDA", zone, last_s + A2_UMBRAL_S))
        a2 = True
    return k3, a1, a2


def procesar_pedido(key, pdf_iter, state):
    order_id = key[0]
    if state.exists:
        status, zone, promised_s, last_s, k3, a1, a2 = state.get  # state.get es una TUPLA
    else:
        status = zone = promised_s = last_s = None
        k3 = a1 = a2 = False
    salidas = []

    if state.hasTimedOut:
        # No llegaron eventos de este pedido: el watermark paso su fecha limite.
        ahora = state.getCurrentWatermarkMs() / 1000.0
        k3, a1, a2 = _evaluar(order_id, ahora, status, zone, promised_s, last_s, k3, a1, a2, salidas)
    else:
        eventos = pd.concat(list(pdf_iter)).sort_values("event_s")
        for fila in eventos.itertuples():
            if last_s is not None:
                # El tiempo avanzo hasta este evento: revisar lo que vencio antes de aplicarlo.
                k3, a1, a2 = _evaluar(order_id, fila.event_s, status, zone, promised_s, last_s,
                                      k3, a1, a2, salidas)
            promised_s = int(fila.promised_s)
            if last_s is None or fila.event_s >= last_s:  # ignorar eventos atrasados
                status, zone, last_s = fila.status, fila.zone, int(fila.event_s)
                a2 = False  # un evento nuevo reinicia la espera de A2
        if status in TERMINALES:
            state.remove()
            yield pd.DataFrame(salidas, columns=COLUMNAS_SALIDA).astype({"momento_s": "int64"})
            return
        ahora = state.getCurrentWatermarkMs() / 1000.0
        k3, a1, a2 = _evaluar(order_id, max(ahora, last_s), status, zone, promised_s, last_s,
                              k3, a1, a2, salidas)

    # state.update recibe una TUPLA en el orden de ESTADO_DDL (no un diccionario).
    state.update((status, zone, promised_s, last_s, k3, a1, a2))

    pendientes = []
    if not k3:
        pendientes.append(promised_s)
    if not a1:
        pendientes.append(promised_s + A1_TOLERANCIA_S)
    if not a2 and status in EN_RUTA_O_DETENIDO:
        pendientes.append(last_s + A2_UMBRAL_S)
    if pendientes:
        # El timeout no puede ser menor que el watermark actual.
        state.setTimeoutTimestamp(max(int(min(pendientes) * 1000), state.getCurrentWatermarkMs() + 1))

    yield pd.DataFrame(salidas, columns=COLUMNAS_SALIDA).astype({"momento_s": "int64"})
