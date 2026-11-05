"""Pruebas del producer: perfiles de pedido, reproducibilidad y reparto por particion.

No usan Kafka: el envio se prueba con un productor falso que reparte los
mensajes por la key, como hace Kafka.
"""
import json
import zlib
from collections import defaultdict
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from producer.events import generar_eventos, parsear_ts
from producer.particiones import analizar_distribucion
from producer.producer import publicar

INICIO = datetime(2026, 10, 21, 14, 0, tzinfo=timezone.utc)
SIN_PERFILES = {"p_retrasado": 0, "p_detenido": 0, "p_cancelado": 0}


def por_pedido(eventos):
    pedidos = defaultdict(list)
    for evento in eventos:
        pedidos[evento["order_id"]].append(evento)
    return pedidos


def generar(n=60, **perfil):
    """Genera n pedidos; por defecto todos son NORMAL."""
    opciones = dict(SIN_PERFILES)
    opciones.update(perfil)
    return generar_eventos(n, seed=3, inicio=INICIO, **opciones)


# ---------------------------------------------------------------- perfiles
def test_pedidos_normales_se_entregan_antes_de_promised_at():
    for lista in por_pedido(generar()).values():
        entregas = [e for e in lista if e["status"] == "ENTREGADO"]
        assert len(entregas) == 1
        assert parsear_ts(entregas[0]["event_ts"]) < parsear_ts(entregas[0]["promised_at"])


def test_pedidos_retrasados_se_entregan_despues_de_promised_at():
    for lista in por_pedido(generar(p_retrasado=1)).values():
        entregas = [e for e in lista if e["status"] == "ENTREGADO"]
        assert len(entregas) == 1
        assert parsear_ts(entregas[0]["event_ts"]) > parsear_ts(entregas[0]["promised_at"])


def test_pedidos_detenidos_terminan_en_detenido_sin_entrega():
    for lista in por_pedido(generar(p_detenido=1)).values():
        estados = [e["status"] for e in lista]
        assert estados[-1] == "DETENIDO"
        assert "ENTREGADO" not in estados
        assert "EN_RUTA" in estados


def test_pedidos_cancelados_terminan_en_cancelado_sin_ruta():
    for lista in por_pedido(generar(p_cancelado=1)).values():
        estados = [e["status"] for e in lista]
        assert estados[-1] == "CANCELADO"
        assert "EN_RUTA" not in estados
        assert "ENTREGADO" not in estados


def test_latido_en_ruta_cada_5_minutos():
    eventos = generar(80, p_retrasado=0.3, p_detenido=0.3)
    for lista in por_pedido(eventos).values():
        tiempos = [parsear_ts(e["event_ts"]) for e in lista if e["status"] == "EN_RUTA"]
        for antes, despues in zip(tiempos, tiempos[1:]):
            assert (despues - antes).total_seconds() == 5 * 60


# --------------------------------------------------------- generar_eventos
def test_misma_seed_da_los_mismos_eventos():
    assert generar_eventos(20, seed=7, inicio=INICIO) == generar_eventos(20, seed=7, inicio=INICIO)


def test_otra_seed_da_eventos_distintos():
    assert generar_eventos(20, seed=7, inicio=INICIO) != generar_eventos(20, seed=8, inicio=INICIO)


def test_eventos_ordenados_por_event_ts_y_con_ids_unicos():
    eventos = generar_eventos(40, seed=5, inicio=INICIO)
    tiempos = [parsear_ts(e["event_ts"]) for e in eventos]
    assert tiempos == sorted(tiempos)
    ids = [e["event_id"] for e in eventos]
    assert len(ids) == len(set(ids))


def test_cero_pedidos_da_lista_vacia():
    assert generar_eventos(0, seed=1, inicio=INICIO) == []


def test_numero_negativo_de_pedidos_falla():
    with pytest.raises(ValueError):
        generar_eventos(-1, seed=1, inicio=INICIO)


def test_probabilidad_fuera_de_rango_falla():
    with pytest.raises(ValueError):
        generar_eventos(5, seed=1, inicio=INICIO, p_retrasado=1.5)


def test_probabilidades_que_suman_mas_de_uno_fallan():
    with pytest.raises(ValueError):
        generar_eventos(5, seed=1, inicio=INICIO, p_retrasado=0.6, p_detenido=0.6)


def test_primer_pedido_y_primer_evento_cambian_la_numeracion():
    eventos = generar_eventos(3, seed=1, inicio=INICIO, primer_pedido=125, primer_evento=100)
    assert eventos[0]["order_id"].startswith("ORD-0001")
    assert {e["order_id"] for e in eventos} >= {"ORD-000125"}
    assert min(e["event_id"] for e in eventos) == "EVT-000100"


# ------------------------------------------------------------------ publicar
class ProductorFalso:
    """Imita a KafkaProducer: reparte por la key con 3 particiones."""

    def __init__(self, particiones=3):
        self.particiones = particiones
        self.enviados = []
        self.flush_llamado = False

    def send(self, topic, key, value):
        self.enviados.append((topic, key, value))
        particion = zlib.crc32(key) % self.particiones
        return SimpleNamespace(get=lambda timeout=None: SimpleNamespace(partition=particion))

    def flush(self):
        self.flush_llamado = True


def test_publicar_usa_order_id_como_key_y_envia_el_json_del_evento():
    eventos = generar_eventos(5, seed=2, inicio=INICIO)
    productor = ProductorFalso()
    publicar(eventos, productor, "rapidgo.sla.entregas")
    assert len(productor.enviados) == len(eventos)
    for evento, (topic, key, value) in zip(eventos, productor.enviados):
        assert topic == "rapidgo.sla.entregas"
        assert key == evento["order_id"].encode("utf-8")
        assert json.loads(value.decode("utf-8")) == evento
    assert productor.flush_llamado


def test_publicar_deja_cada_pedido_en_una_sola_particion():
    resumen = publicar(generar_eventos(30, seed=4, inicio=INICIO), ProductorFalso(), "t")
    assert resumen["ok"] is True
    assert resumen["pedidos_en_varias"] == []
    assert resumen["sin_key"] == 0
    assert resumen["pedidos"] == 30


def test_publicar_sin_velocidad_no_hace_pausas():
    pausas = []
    publicar(generar_eventos(5, seed=2, inicio=INICIO), ProductorFalso(), "t",
             velocidad=0, dormir=pausas.append)
    assert pausas == []


def test_publicar_con_velocidad_hace_pausas():
    pausas = []
    publicar(generar_eventos(5, seed=2, inicio=INICIO), ProductorFalso(), "t",
             velocidad=10, dormir=pausas.append)
    assert len(pausas) > 0
    assert all(p > 0 for p in pausas)


# ------------------------------------------------------ analizar_distribucion
def test_distribucion_correcta_cuando_cada_pedido_esta_en_una_particion():
    pares = [(0, "ORD-1"), (0, "ORD-1"), (1, "ORD-2"), (2, "ORD-3")]
    resumen = analizar_distribucion(pares)
    assert resumen["total"] == 4
    assert resumen["pedidos"] == 3
    assert resumen["por_particion"] == {0: 2, 1: 1, 2: 1}
    assert resumen["ok"] is True


def test_distribucion_detecta_un_pedido_en_dos_particiones():
    pares = [(0, "ORD-1"), (1, "ORD-1"), (2, "ORD-2")]
    resumen = analizar_distribucion(pares)
    assert resumen["pedidos_en_varias"] == ["ORD-1"]
    assert resumen["ok"] is False


def test_distribucion_cuenta_los_mensajes_sin_key():
    resumen = analizar_distribucion([(0, None), (1, "ORD-1")])
    assert resumen["sin_key"] == 1
    assert resumen["total"] == 2
