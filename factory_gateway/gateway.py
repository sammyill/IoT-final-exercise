"""
gateway.py — Gateway CoAP → InfluxDB
=====================================
Progetto didattico ITS 3° anno — Protocollo CoAP (RFC 7252)

Ruolo: SERVER CoAP che riceve i dati dai sensori e li scrive in InfluxDB.

Principi chiave:
1. Risponde SEMPRE con ACK 2.04 CHANGED al sensore (anche se InfluxDB è down)
   → il sensore sa che il gateway ha ricevuto il dato
2. Buffer interno (deque max 2000) per i dati quando InfluxDB non è disponibile
3. Task asyncio separato (influx_writer_loop) che svuota TUTTO il buffer ogni 1s (batch write)
4. Scritture InfluxDB in ThreadPoolExecutor (client sincrono, non blocca il loop)

Flusso:
  sensore --[POST CON UDP 5683]--> gateway --[HTTP]--> InfluxDB:8086
                                     ↑
                                   [ACK] sempre, subito
"""

import asyncio
import aiocoap
import aiocoap.resource as resource
import json
import os
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import urllib.request
import urllib.error

import aiohttp
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# ──────────────────────────────────────────────────────────────
# Configurazione da variabili d'ambiente
# ──────────────────────────────────────────────────────────────
INFLUX_URL    = os.getenv("INFLUX_URL",    "http://influxdb:8086")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN",  "my-super-token")
INFLUX_ORG    = os.getenv("INFLUX_ORG",    "its")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "iot")
COAP_PORT     = int(os.getenv("COAP_PORT", "5683"))

# URL del gateway HTTP che valuta soglie e invia email a Mailhog.
# Endpoint "factory" perché tutti i dati di questo gateway sono CoAP/factory.
AMQP_GATEWAY_URL = os.getenv("AMQP_GATEWAY_URL", "http://amqp-gateway:8000/factory")
# Sessione aiohttp globale, creata in main(). None se il forwarding è disabilitato.
http_session: "aiohttp.ClientSession | None" = None

# Buffer interno: se InfluxDB non risponde, i dati restano qui
INTERNAL_BUFFER_MAX = 2000
internal_buffer = deque(maxlen=INTERNAL_BUFFER_MAX)

# ThreadPoolExecutor per le scritture bloccanti su InfluxDB
executor = ThreadPoolExecutor(max_workers=1)

# Statistiche operative
stats = {
    "received":   0,   # messaggi CoAP ricevuti dai sensori
    "written":    0,   # punti scritti su InfluxDB con successo
    "buffered":   0,   # punti in attesa nel buffer interno
    "errors":     0,   # errori di scrittura InfluxDB
    "amqp_sent":  0,   # forward HTTP a amqp-gateway riusciti
    "amqp_fail":  0,   # forward HTTP a amqp-gateway falliti
}

# Client InfluxDB (inizializzato in main dopo il health check)
influx_client = None
write_api = None


# ──────────────────────────────────────────────────────────────
# Scrittura sincrona su InfluxDB — BATCH (eseguita in executor)
# ──────────────────────────────────────────────────────────────
def sync_write_batch(batch: list):
    """
    Scrive un BATCH di punti su InfluxDB in una singola chiamata HTTP.
    Molto più efficiente di scrivere un punto alla volta.

    Throughput: con 10 sensori × 1 msg/5s = 2 msg/s in ingresso,
    il batch svuota l'intera coda in ogni ciclo → latenza < 1s.

    Lancia eccezione in caso di errore (il chiamante reinserisce nel buffer).
    """
    points = []
    for data in batch:
        p = (
            Point("temperatura")
            .tag("sensor_id", data.get("sensor_id", "unknown"))
            .field("value",   float(data["temperatura"]))
            .field("seq",     int(data.get("seq", 0)))
        )
        # Usa il timestamp originale del sensore se disponibile
        if "timestamp" in data:
            p = p.time(int(data["timestamp"] * 1e9))  # secondi → nanosecondi
        points.append(p)

    write_api.write(bucket=INFLUX_BUCKET, record=points)


# ──────────────────────────────────────────────────────────────
# Task: svuota il buffer interno verso InfluxDB (batch write)
# ──────────────────────────────────────────────────────────────
async def influx_writer_loop():
    """
    Task asyncio che gira in background ogni secondo.

    PRIMA (problema): scriveva 1 punto ogni 2s = 0.5 msg/s.
    Con 10 sensori a 5s = 2 msg/s in entrata, il buffer si riempiva.

    ORA (fix): svuota TUTTO il buffer in un unico batch HTTP per ciclo.
    Una sola chiamata all'API InfluxDB → throughput limitato solo dalla rete.
    In caso di errore reinserisce il batch nel buffer e riprova.
    """
    loop = asyncio.get_event_loop()
    print("[INFLUX] Writer loop avviato — batch mode (ogni 1s svuota tutto il buffer)")

    while True:
        await asyncio.sleep(1)

        if not internal_buffer:
            continue

        # Drena TUTTO il buffer in un batch (max 500 per chiamata per sicurezza)
        batch = []
        while internal_buffer and len(batch) < 500:
            batch.append(internal_buffer.popleft())

        if not batch:
            continue

        try:
            await loop.run_in_executor(executor, sync_write_batch, batch)
            stats["written"] += len(batch)
            stats["errors"] = 0  # reset errori consecutivi dopo successo

            print(
                f"[INFLUX] ✓ Scritti {len(batch)} punti in batch | "
                f"totale={stats['written']} | buf={len(internal_buffer)}"
            )

        except Exception as e:
            stats["errors"] += 1
            # Reinserisce il batch nel buffer (in testa, ordine cronologico)
            for item in reversed(batch):
                internal_buffer.appendleft(item)
            print(
                f"[ERR] Batch write fallito (tentativo #{stats['errors']}): {e} | "
                f"buf={len(internal_buffer)}"
            )


# ──────────────────────────────────────────────────────────────
# Forward HTTP a amqp-gateway (alert/email)
# ──────────────────────────────────────────────────────────────
# Fire-and-forget: spediamo il dato a amqp-gateway senza
# bloccare la risposta CoAP al sensore. Se il forward fallisce,
# il dato è comunque già accodato per InfluxDB.
async def forward_to_amqp(data: dict) -> None:
    if http_session is None:
        return
    try:
        async with http_session.post(AMQP_GATEWAY_URL, json=data) as resp:
            if resp.status < 400:
                stats["amqp_sent"] += 1
            else:
                stats["amqp_fail"] += 1
    except Exception:
        # Best-effort: l'alerting non deve bloccare la pipeline dati.
        stats["amqp_fail"] += 1


# ──────────────────────────────────────────────────────────────
# Risorsa CoAP: /iot/temperatura
# ──────────────────────────────────────────────────────────────
class TemperaturaResource(resource.Resource):
    """
    Risorsa CoAP che gestisce i POST dai sensori.

    Regola fondamentale: risponde SEMPRE con 2.04 CHANGED, anche se
    InfluxDB è down. Il dato viene messo nel buffer interno e scritto
    appena InfluxDB torna disponibile.

    Questo garantisce che il sensore non tenga il dato nel suo buffer
    locale più del necessario — è il gateway che si occupa della persistenza.
    """

    async def render_post(self, request):
        try:
            raw = request.payload.decode("utf-8")
            data = json.loads(raw)
            stats["received"] += 1

            # Aggiungi timestamp di ricezione gateway
            data["gw_ts"] = time.time()

            # Inserisci nel buffer interno (la scrittura avviene nel writer_loop)
            if len(internal_buffer) == INTERNAL_BUFFER_MAX:
                # Buffer pieno: scarta il più vecchio (deque lo fa automaticamente)
                print(f"[BUFFER] Buffer interno pieno ({INTERNAL_BUFFER_MAX}), dato più vecchio scartato")

            internal_buffer.append(data)
            stats["buffered"] = len(internal_buffer)

            # Forward HTTP a amqp-gateway in background (fire-and-forget).
            # Non blocca la risposta ACK al sensore.
            asyncio.create_task(forward_to_amqp(data))

            # Log ogni 20 messaggi per non spammare
            if stats["received"] % 20 == 1:
                print(
                    f"[RX] ricevuti={stats['received']} | "
                    f"scritti={stats['written']} | "
                    f"buf={len(internal_buffer)} | "
                    f"ultimo: {data.get('sensor_id')} → {data.get('temperatura')}°C"
                )

        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"[ERR] Payload non valido: {e} | raw={request.payload[:100]}")
            # Rispondi comunque con CHANGED per liberare il sensore
        except Exception as e:
            print(f"[ERR] Errore inatteso: {e}")

        # SEMPRE rispondere 2.04 CHANGED → il sensore riceve ACK e non ritrasmette
        return aiocoap.Message(code=aiocoap.CHANGED)


# ──────────────────────────────────────────────────────────────
# Attesa health check InfluxDB
# ──────────────────────────────────────────────────────────────
async def wait_for_influxdb():
    """
    Aspetta che InfluxDB sia pronto (endpoint /health risponde 200).
    Riprova ogni 5 secondi con log.
    """
    health_url = f"{INFLUX_URL}/health"
    print(f"[INFLUX] Attendo InfluxDB su {health_url}...")
    attempt = 0
    while True:
        attempt += 1
        try:
            with urllib.request.urlopen(health_url, timeout=5) as resp:
                if resp.status == 200:
                    body = json.loads(resp.read().decode())
                    print(f"[INFLUX] InfluxDB pronto! Status: {body.get('status')}")
                    return
        except Exception as e:
            print(f"[INFLUX] Tentativo {attempt}: InfluxDB non pronto ({e}), riprovo tra 5s...")
        await asyncio.sleep(5)


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────
async def main():
    global influx_client, write_api, http_session

    print("=" * 55)
    print(f"[BOOT] CoAP Gateway avviato")
    print(f"[BOOT]   INFLUX_URL       = {INFLUX_URL}")
    print(f"[BOOT]   INFLUX_ORG       = {INFLUX_ORG}")
    print(f"[BOOT]   INFLUX_BUCKET    = {INFLUX_BUCKET}")
    print(f"[BOOT]   COAP_PORT        = {COAP_PORT}/udp")
    print(f"[BOOT]   BUFFER_MAX       = {INTERNAL_BUFFER_MAX} punti")
    print(f"[BOOT]   AMQP_GATEWAY_URL = {AMQP_GATEWAY_URL}")
    print(f"[BOOT]   Risorsa CoAP     = /iot/temperatura")
    print("=" * 55)

    # Sessione HTTP riusabile per il forwarding (un connector pool unico).
    http_session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=5),
    )

    # 1. Aspetta InfluxDB
    await wait_for_influxdb()

    # 2. Inizializza client InfluxDB
    influx_client = InfluxDBClient(
        url=INFLUX_URL,
        token=INFLUX_TOKEN,
        org=INFLUX_ORG,
    )
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    print(f"[INFLUX] Client InfluxDB inizializzato")

    # 3. Crea il server CoAP con le risorse
    root = resource.Site()
    root.add_resource(["iot", "temperatura"], TemperaturaResource())

    # Risorsa di discovery (well-known/core) — utile per debug con coap-client
    root.add_resource(
        [".well-known", "core"],
        resource.WKCResource(root.get_resources_as_linkheader)
    )

    # Bind su tutte le interfacce UDP porta 5683
    await aiocoap.Context.create_server_context(root, bind=("0.0.0.0", COAP_PORT))
    print(f"[BOOT] Server CoAP in ascolto su 0.0.0.0:{COAP_PORT}/udp")
    print(f"[BOOT] Pronto a ricevere POST su coap://0.0.0.0:{COAP_PORT}/iot/temperatura")

    # 4. Avvia il task di scrittura su InfluxDB in background
    asyncio.ensure_future(influx_writer_loop())

    # 5. Loop infinito — il server CoAP gira autonomamente
    print("[BOOT] Gateway operativo. Ctrl+C per fermare.")
    await asyncio.get_event_loop().create_future()


# ──────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n[BOOT] Gateway fermato")
        print(f"[BOOT] Statistiche finali: {stats}")
        if influx_client:
            influx_client.close()
