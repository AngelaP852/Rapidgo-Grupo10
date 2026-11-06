"""Prueba de punta a punta del job de Spark SIN Kafka (modo --source files).

Genera eventos con el producer, les mezcla casos invalidos y duplicados, los reparte en
varios archivos (cada archivo = un micro-lote) y compara las salidas del Data Lake con un
calculo independiente en pandas.

  python tests/integracion_spark.py

Requiere pyspark, pandas, pyarrow y Java 17. No forma parte de pytest (no empieza por test_).
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import pandas as pd

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)
from producer.events import generar_eventos  # noqa: E402

WM_MIN = 5
CHUNKS = 10
fallos = []


def comprobar(ok, mensaje):
    print(("  OK   " if ok else "  FALLA ") + mensaje)
    if not ok:
        fallos.append(mensaje)


def casos_invalidos():
    base = {"event_id": "EVT-INV-00", "team_id": "equipo-v1", "order_id": "ORD-INV-01", "status": "EN_RUTA",
            "zone": "Centro", "distance_km": 4.5, "created_at": "2026-10-21T14:00:00Z",
            "promised_at": "2026-10-21T14:45:00Z", "event_ts": "2026-10-21T14:20:00Z"}
    def v(n, **cambios):
        e = dict(base, event_id=f"EVT-INV-{n:02d}")
        e.update(cambios)
        for k in [k for k, x in e.items() if x is None]:
            del e[k]
        return e
    return [
        ("{esto no es json", ["json_invalido"]),
        (v(2, zone=None), ["zone_faltante"]),
        (v(3, status="EN_CAMINO"), ["status_invalido"]),
        (v(4, distance_km=0), ["distance_km_invalido"]),
        (v(5, event_ts="2026-10-21T13:50:00Z"), ["event_ts_anterior_a_created_at"]),
        (v(6, promised_at="2026-10-21T14:00:00Z"),
         ["promised_at_no_posterior_a_created_at", "promised_at_no_coincide_con_sla"]),
        (v(7, created_at="2026-10-21 14:00:00"), ["created_at_invalido"]),
        (v(8, promised_at="2026-10-21T14:50:00Z"), ["promised_at_no_coincide_con_sla"]),
        (v(9, order_id=""), ["order_id_faltante"]),
        (v(10, distance_km="abc"), ["distance_km_invalido"]),
    ]


def main():
    tmp = tempfile.mkdtemp(prefix="rapidgo_it_")
    entrada, lago = os.path.join(tmp, "entrada"), os.path.join(tmp, "datalake")
    os.makedirs(entrada)
    inicio = datetime(2026, 10, 21, 14, 0, tzinfo=timezone.utc)
    eventos = generar_eventos(150, seed=7, inicio=inicio, p_retrasado=0.3, p_detenido=0.15, p_cancelado=0.1)
    invalidos = casos_invalidos()

    # --- repartir en archivos por tiempo; invalidos y duplicados en archivos intermedios
    n = len(eventos)
    por_chunk = -(-n // CHUNKS)
    chunks = [eventos[i:i + por_chunk] for i in range(0, n, por_chunk)]
    lineas = [[json.dumps(e) for e in c] for c in chunks]
    for i, (linea, _) in enumerate(invalidos):
        lineas[1 + i % 3].append(linea if isinstance(linea, str) else json.dumps(linea))
    duplicados = chunks[3][:5]
    lineas[3] += [json.dumps(e) for e in duplicados]      # duplicados en el mismo archivo
    lineas[4] += [json.dumps(e) for e in duplicados]      # y otra vez en el archivo siguiente
    total_lineas = sum(len(x) for x in lineas)
    ahora = datetime.now().timestamp()
    for i, linea in enumerate(lineas):
        ruta = os.path.join(entrada, f"lote_{i:02d}.jsonl")
        with open(ruta, "w") as f:
            f.write("\n".join(linea) + "\n")
        os.utime(ruta, (ahora + 10 * i, ahora + 10 * i))   # Spark procesa los archivos por fecha de modificacion

    # --- ejecutar el job
    cmd = [sys.executable, os.path.join(RAIZ, "spark", "streaming_job.py"), "--source", "files",
           "--input-dir", entrada, "--datalake-dir", lago, "--una-vez", "--watermark-min", str(WM_MIN)]
    print("Ejecutando el job de Spark (puede tardar 1-2 min)...")
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stdout[-3000:], r.stderr[-6000:])
        sys.exit("El job fallo")

    def leer(ruta):
        return pd.read_parquet(os.path.join(lago, ruta))

    raw, rej, proc = leer("raw/eventos"), leer("rejected/eventos"), leer("processed/eventos")
    kpis, alertas = leer("curated/kpis"), leer("curated/alertas")

    print("\n[raw]")
    comprobar(len(raw) == total_lineas, f"raw guarda todo lo recibido ({len(raw)} de {total_lineas})")

    print("\n[rejected]")
    comprobar(len(rej) == len(invalidos), f"{len(rej)} rechazados de {len(invalidos)} invalidos enviados")
    for linea, motivos in invalidos:
        txt = linea if isinstance(linea, str) else json.dumps(linea)
        fila = rej[rej["value"] == txt]
        obtenidos = sorted(fila["motivos"].iloc[0].split(",")) if len(fila) else None
        comprobar(obtenidos == sorted(motivos), f"{txt[:48]:<48} -> {obtenidos}")

    print("\n[processed]")
    unicos = {e["event_id"] for e in eventos}
    comprobar(len(proc) == len(unicos) and set(proc.event_id) == unicos,
              f"{len(proc)} eventos validos unicos de {len(unicos)} esperados (duplicados eliminados)")
    comprobar(proc.event_id.is_unique, "ningun event_id repetido")
    comprobar(set(proc.zone) <= {"Norte", "Sur", "Centro", "Oriente", "Occidente"}, "zona normalizada")
    e = pd.DataFrame(eventos).drop_duplicates("event_id").set_index("event_id")
    p = proc.set_index("event_id")
    ref_desde = (pd.to_datetime(e.event_ts) - pd.to_datetime(e.created_at)).dt.total_seconds() / 60
    ref_vs = (pd.to_datetime(e.event_ts) - pd.to_datetime(e.promised_at)).dt.total_seconds() / 60
    comprobar(((p.minutos_desde_creacion - ref_desde.loc[p.index]).abs() < 1e-6).all(), "minutos_desde_creacion")
    comprobar(((p.minutos_vs_promesa - ref_vs.loc[p.index]).abs() < 1e-6).all(), "minutos_vs_promesa")
    ent = p[p.status == "ENTREGADO"]
    comprobar((ent.fuera_sla == (ref_vs.loc[ent.index] > 0)).all() and p[p.status != "ENTREGADO"].fuera_sla.isna().all(),
              "fuera_sla solo en ENTREGADO y correcto")

    print("\n[curated/kpis]")
    def t(valor):
        return pd.to_datetime(valor, utc=True)

    ts_chunks = [max(t(x["event_ts"]) for x in c) for c in chunks]
    w_seguro = ts_chunks[-2] - timedelta(minutes=WM_MIN)
    w_max = ts_chunks[-1]
    d = e.reset_index()
    d["et"], d["ct"], d["pt"] = t(d.event_ts), t(d.created_at), t(d.promised_at)
    ent_ref = d[d.status == "ENTREGADO"].copy()
    ent_ref["zone"] = ent_ref.zone.str.title()
    ent_ref["ini"] = ent_ref.et.dt.floor("5min")
    ent_ref["min"] = (ent_ref.et - ent_ref.ct).dt.total_seconds() / 60
    ent_ref["tarde"] = ent_ref.et > ent_ref.pt
    ref_k = ent_ref.groupby(["ini", "zone"]).agg(n=("min", "size"), k1=("min", "mean"), k2=("tarde", "mean")).reset_index()
    ref_k["fin"] = ref_k.ini + timedelta(minutes=5)
    k = kpis.copy()
    k["ini"] = t(k.ventana_inicio)
    obtenidas = set(zip(k.ini, k.zone))
    deben = {(r.ini, r.zone) for r in ref_k.itertuples() if r.fin <= w_seguro}
    pueden = {(r.ini, r.zone) for r in ref_k.itertuples() if r.fin <= w_max}
    comprobar(deben <= obtenidas <= pueden,
              f"{len(obtenidas)} ventanas cerradas (minimo {len(deben)}, maximo {len(pueden)})")
    j = k.merge(ref_k, on=["ini", "zone"])
    comprobar(len(j) == len(k) and (j.pedidos_entregados == j.n).all(), "pedidos_entregados coincide")
    comprobar(((j.k1_tiempo_promedio_min - j.k1).abs() < 0.01).all(), "K1 = promedio de ENTREGADO - created_at")
    comprobar(((j.k2_pct_fuera_sla - 100 * j.k2).abs() < 0.01).all(), "K2 = % entregados despues de promised_at")

    print("\n[curated/alertas]")
    esperadas_seguras, esperadas_posibles = set(), set()
    for oid, g in d.sort_values("et").groupby("order_id"):
        evs = list(g.itertuples())
        prom = evs[0].pt
        terminal = evs[-1].status in ("ENTREGADO", "CANCELADO")
        ultimo = evs[-1].et
        for tipo, umbral in (("K3_RETRASADO", prom), ("A1_SLA_EXCEDIDO", prom + timedelta(minutes=5))):
            clave = (oid, tipo, umbral)
            if ultimo > umbral:
                esperadas_seguras.add(clave)
            elif not terminal and umbral <= w_max:
                esperadas_posibles.add(clave)
                if umbral < w_seguro:
                    esperadas_seguras.add(clave)
        for i, ev in enumerate(evs):
            if ev.status not in ("EN_RUTA", "DETENIDO"):
                continue
            umbral = ev.et + timedelta(minutes=10)
            clave = (oid, "A2_ENTREGA_DETENIDA", umbral)
            if i + 1 < len(evs):
                if evs[i + 1].et > umbral:
                    esperadas_seguras.add(clave)
            elif not terminal and umbral <= w_max:
                esperadas_posibles.add(clave)
                if umbral < w_seguro:
                    esperadas_seguras.add(clave)
    a = alertas.copy()
    a["momento"] = t(a.momento)
    obtenidas = list(zip(a.order_id, a.alerta, a.momento))
    comprobar(len(obtenidas) == len(set(obtenidas)), "ningun aviso repetido")
    obtenidas = set(obtenidas)
    faltan, sobran = esperadas_seguras - obtenidas, obtenidas - (esperadas_seguras | esperadas_posibles)
    comprobar(not faltan, f"no falta ningun aviso esperado ({len(esperadas_seguras)} esperados, faltan {len(faltan)})")
    comprobar(not sobran, f"no hay avisos de mas ({len(obtenidas)} emitidos, sobran {len(sobran)})")
    if faltan:
        print("   faltan:", sorted(faltan)[:5])
    if sobran:
        print("   sobran:", sorted(sobran)[:5])
    por_tipo = a.groupby("alerta").size().to_dict()
    comprobar(set(por_tipo) == {"K3_RETRASADO", "A1_SLA_EXCEDIDO", "A2_ENTREGA_DETENIDA"},
              f"los tres tipos de aviso aparecen: {por_tipo}")

    shutil.rmtree(tmp, ignore_errors=True)
    print("\nRESULTADO:", "TODO OK" if not fallos else f"{len(fallos)} FALLAS")
    sys.exit(1 if fallos else 0)


if __name__ == "__main__":
    main()
