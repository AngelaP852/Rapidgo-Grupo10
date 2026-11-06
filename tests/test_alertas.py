"""Casos de docs/casos_prueba_alertas.md: reglas K3, A1 y A2 evaluadas en un instante 'ahora'.

Se prueban las funciones puras de spark/alertas_estado.py, sin Spark. El estado de un pedido en
'ahora' es el de su ultimo evento con event_ts <= ahora (el pedido esta abierto hasta ENTREGADO o
CANCELADO).
"""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("pandas")  # alertas_estado importa pandas
import alertas_estado as ae  # noqa: E402

SAMPLES = Path(__file__).resolve().parent.parent / "samples"


def seg(hora):
    """'14:40:01' o '14:40' -> segundos epoch del 2026-10-21 (UTC)."""
    partes = [int(x) for x in hora.split(":")]
    h, m, s = (partes + [0])[:3]
    return int(datetime(2026, 10, 21, h, m, s, tzinfo=timezone.utc).timestamp())


def eventos_de(orden):
    lineas = []
    for nombre in ("eventos_ciclo_vida.jsonl", "eventos_entrega_detenida.jsonl"):
        lineas += (SAMPLES / nombre).read_text(encoding="utf-8").splitlines()
    return [e for e in map(json.loads, filter(str.strip, lineas)) if e["order_id"] == orden]


def a_segundos(texto):
    return int(datetime.strptime(texto, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())


def estado_en(eventos, ahora):
    """(status, promised_s, last_s) con los eventos ocurridos hasta 'ahora'."""
    hasta = [e for e in eventos if a_segundos(e["event_ts"]) <= ahora]
    ultimo = max(hasta, key=lambda e: a_segundos(e["event_ts"]))
    return ultimo["status"], a_segundos(ultimo["promised_at"]), a_segundos(ultimo["event_ts"])


def pedido(promised, *pasos):
    """Pedido hipotetico: pasos = ('EN_RUTA', '14:25'), ..."""
    return [{"status": s, "event_ts": f"2026-10-21T{h}:00Z", "promised_at": f"2026-10-21T{promised}:00Z"}
            for s, h in pasos]


def a1_en(eventos, hora):
    ahora = seg(hora)
    status, promised, _ = estado_en(eventos, ahora)
    return ae.a1_se_cumple(status, promised, ahora)


def a2_en(eventos, hora):
    ahora = seg(hora)
    status, _, ultimo = estado_en(eventos, ahora)
    return ae.a2_se_cumple(status, ultimo, ahora)


def k3_en(eventos, hora):
    ahora = seg(hora)
    status, promised, _ = estado_en(eventos, ahora)
    return ae.k3_se_cumple(status, promised, ahora)


O123, O124, O126 = eventos_de("ORD-000123"), eventos_de("ORD-000124"), eventos_de("ORD-000126")


@pytest.mark.parametrize("caso, eventos, ahora, esperado", [
    ("A1-01", O123, "14:40:01", True),
    ("A1-02", O123, "14:40:00", False),
    ("A1-03", O123, "14:37:00", False),
    ("A1-04", O123, "14:30:00", False),
    ("A1-05", O123, "14:45:00", False),
    ("A1-06", O124, "14:50:00", False),
    ("A1-07", O126, "14:31:00", True),
    ("A1-08", pedido("14:30", ("CREADO", "14:00"), ("CANCELADO", "14:36")), "14:50", False),
])
def test_a1(caso, eventos, ahora, esperado):
    assert a1_en(eventos, ahora) is esperado, caso


@pytest.mark.parametrize("caso, eventos, ahora, esperado", [
    ("A2-01", O126, "14:28:01", True),
    ("A2-02", O126, "14:28:00", False),
    ("A2-03", O126, "14:23:00", False),
    ("A2-04", O124, "14:25:00", False),
    ("A2-05", O123, "14:45:00", False),
    ("A2-06", pedido("15:30", ("EN_RUTA", "14:20"), ("EN_RUTA", "14:25")), "14:35:01", True),
    ("A2-07", O124, "14:40:00", False),
    ("A2-08", pedido("15:30", ("CREADO", "14:00"), ("RECOGIDO", "14:05")), "14:20:00", False),
])
def test_a2(caso, eventos, ahora, esperado):
    assert a2_en(eventos, ahora) is esperado, caso


def test_a2_09_un_evento_nuevo_reinicia_el_contador():
    reanudado = O126 + pedido("14:25", ("EN_RUTA", "14:27"))
    assert a2_en(reanudado, "14:36:00") is False   # 9 min desde el evento nuevo
    assert a2_en(reanudado, "14:37:00") is False   # justo 10 min
    assert a2_en(reanudado, "14:37:01") is True


def test_ac_01_a1_y_a2_a_la_vez_en_ord_000126():
    assert a1_en(O126, "14:31:00") is True
    assert a2_en(O126, "14:31:00") is True


def test_ac_02_ord_000123_dispara_a1_pero_no_a2():
    assert a1_en(O123, "14:40:01") is True
    assert a2_en(O123, "14:40:01") is False


@pytest.mark.parametrize("minuto", range(5, 60))
def test_ac_03_ord_000124_nunca_dispara_nada(minuto):
    hora = f"14:{minuto:02d}"
    assert not a1_en(O124, hora) and not a2_en(O124, hora) and not k3_en(O124, hora)


def test_k3_se_cumple_apenas_pasa_promised_at():
    assert k3_en(O123, "14:35:00") is False
    assert k3_en(O123, "14:35:01") is True
    assert k3_en(O123, "14:45:00") is False   # ya entregado
