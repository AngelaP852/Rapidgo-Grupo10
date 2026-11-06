"""Prueba la logica de K3/A1/A2 sin Spark, con un GroupState de mentira."""
from datetime import datetime, timezone

import pytest

pd = pytest.importorskip("pandas")
import alertas_estado as ae  # noqa: E402


def s(hhmm):
    h, m = map(int, hhmm.split(":"))
    return int(datetime(2026, 10, 21, h, m, tzinfo=timezone.utc).timestamp())


class EstadoFalso:
    """Imita pyspark.sql.streaming.state.GroupState (update y get usan tuplas)."""

    def __init__(self, watermark_s=0):
        self._valor = None
        self.watermark_ms = watermark_s * 1000
        self.hasTimedOut = False
        self.timeout_ms = None
        self.removido = False

    @property
    def exists(self):
        return self._valor is not None

    @property
    def get(self):
        return tuple(self._valor)

    def update(self, valor):
        assert isinstance(valor, tuple) and len(valor) == 7
        self._valor, self.removido = valor, False
        self.timeout_ms = None  # en Spark, cada llamada debe volver a fijar el timeout

    def remove(self):
        self._valor, self.removido = None, True

    def getCurrentWatermarkMs(self):
        return self.watermark_ms

    def setTimeoutTimestamp(self, ms):
        assert ms >= self.watermark_ms, "el timeout no puede ser menor que el watermark"
        self.timeout_ms = ms


def eventos(order, promised, lista):
    return pd.DataFrame([
        {"order_id": order, "status": st, "zone": "Centro", "event_s": s(t), "promised_s": s(promised)}
        for st, t in lista
    ])


def correr(state, df=None):
    it = iter([df]) if df is not None else iter([])
    out = list(ae.procesar_pedido((df.order_id.iloc[0] if df is not None else "X",), it, state))
    return pd.concat(out) if out else pd.DataFrame(columns=ae.COLUMNAS_SALIDA)


def tipos(out):
    return list(out.alerta)


def test_pedido_normal_no_genera_avisos_y_borra_estado():
    st = EstadoFalso(s("14:00"))
    out = correr(st, eventos("O1", "14:45", [("CREADO", "14:00"), ("EN_RUTA", "14:10"),
                                              ("EN_RUTA", "14:15"), ("EN_RUTA", "14:20"),
                                              ("EN_RUTA", "14:25"), ("ENTREGADO", "14:30")]))
    assert out.empty and st.removido


def test_entrega_tarde_dispara_k3_y_a1_aunque_el_ultimo_evento_sea_ENTREGADO():
    # ORD-000123 de los samples: prometido 14:35, entregado 14:41
    st = EstadoFalso(s("14:00"))
    out = correr(st, eventos("ORD-000123", "14:35", [
        ("CREADO", "14:05"), ("ASIGNADO", "14:08"), ("RECOGIDO", "14:12"), ("EN_RUTA", "14:20"),
        ("EN_RUTA", "14:25"), ("EN_RUTA", "14:30"), ("EN_RUTA", "14:35"), ("EN_RUTA", "14:40"),
        ("ENTREGADO", "14:41")]))
    assert tipos(out) == ["K3_RETRASADO", "A1_SLA_EXCEDIDO"]
    assert list(out.momento_s) == [s("14:35"), s("14:40")]
    assert st.removido


def test_entrega_con_3_min_de_retraso_solo_k3():
    st = EstadoFalso(s("14:00"))
    out = correr(st, eventos("O2", "14:30", [("CREADO", "14:00"), ("EN_RUTA", "14:10"),
                                              ("EN_RUTA", "14:15"), ("EN_RUTA", "14:20"),
                                              ("EN_RUTA", "14:25"), ("ENTREGADO", "14:33")]))
    assert tipos(out) == ["K3_RETRASADO"]


def test_pedido_detenido_dispara_por_timeout_y_solo_una_vez():
    # ORD-000126: prometido 14:25, DETENIDO a las 14:18 y despues silencio
    st = EstadoFalso(s("14:00"))
    out = correr(st, eventos("ORD-000126", "14:25", [
        ("CREADO", "14:00"), ("EN_RUTA", "14:10"), ("EN_RUTA", "14:15"), ("DETENIDO", "14:18")]))
    assert out.empty                       # a las 14:18 todavia no vence nada
    assert st.timeout_ms == s("14:25") * 1000  # el primer vencimiento es promised_at (K3)

    avisos = []
    for wm, esperado in [("14:26", ["K3_RETRASADO"]),
                         ("14:29", ["A2_ENTREGA_DETENIDA"]),   # 14:18 + 10 min = 14:28
                         ("14:31", ["A1_SLA_EXCEDIDO"]),       # 14:25 + 5 min = 14:30
                         ("14:50", [])]:                        # ya se emitieron los tres
        st.watermark_ms, st.hasTimedOut = s(wm) * 1000, True
        out = correr(st)
        assert tipos(out) == esperado, wm
        avisos += tipos(out)
    assert sorted(avisos) == ["A1_SLA_EXCEDIDO", "A2_ENTREGA_DETENIDA", "K3_RETRASADO"]
    assert st.timeout_ms is None or st.timeout_ms >= st.watermark_ms


def test_latido_normal_en_ruta_NO_dispara_a2():
    st = EstadoFalso(s("14:00"))
    out = correr(st, eventos("O3", "15:30", [("CREADO", "14:00"), ("EN_RUTA", "14:10"),
                                              ("EN_RUTA", "14:15"), ("EN_RUTA", "14:20")]))
    assert out.empty
    assert st.timeout_ms == s("14:30") * 1000   # A2 vence 10 min despues del ultimo latido


def test_silencio_entre_dos_eventos_dispara_a2_retroactivo():
    st = EstadoFalso(s("14:00"))
    out = correr(st, eventos("O4", "16:00", [("CREADO", "14:00"), ("EN_RUTA", "14:10"),
                                              ("EN_RUTA", "14:35")]))
    assert tipos(out) == ["A2_ENTREGA_DETENIDA"] and list(out.momento_s) == [s("14:20")]


def test_pedido_cancelado_a_tiempo_no_genera_avisos():
    st = EstadoFalso(s("14:00"))
    out = correr(st, eventos("O5", "14:45", [("CREADO", "14:00"), ("CANCELADO", "14:20")]))
    assert out.empty and st.removido


def test_cancelado_despues_de_vencer_el_sla_avisa_porque_estuvo_abierto():
    st = EstadoFalso(s("14:00"))
    out = correr(st, eventos("O5b", "14:20", [("CREADO", "14:00"), ("CANCELADO", "14:40")]))
    assert tipos(out) == ["K3_RETRASADO", "A1_SLA_EXCEDIDO"] and st.removido


def test_evento_nuevo_reinicia_a2():
    st = EstadoFalso(s("14:00"))
    correr(st, eventos("O6", "16:00", [("CREADO", "14:00"), ("EN_RUTA", "14:10")]))
    st.watermark_ms, st.hasTimedOut = s("14:21") * 1000, True
    assert tipos(correr(st)) == ["A2_ENTREGA_DETENIDA"]
    st.hasTimedOut = False
    out = correr(st, eventos("O6", "16:00", [("EN_RUTA", "14:25")]))
    assert out.empty and st._valor[-1] is False  # a2 reiniciado, puede volver a avisar
