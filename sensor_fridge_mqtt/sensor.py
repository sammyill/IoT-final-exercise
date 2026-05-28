# ============================================================
# sensor.py — Simulatore sensore IoT con resilienza MQTT
# ============================================================
# Questo script simula un sensore di temperatura che invia
# dati via MQTT con le seguenti garanzie di affidabilità:
#
#  1. QoS = 1 ("almeno una volta"):
#     il broker conferma la ricezione di ogni messaggio (PUBACK).
#     Il sensore ritrasmette se non riceve conferma.
#
#  2. Sessione persistente (clean_session=False):
#     il broker ricorda questo client anche da disconnesso.
#     Quando il sensore torna online riceve i messaggi accodati.
#
#  3. Buffer locale:
#     se MQTT non è raggiungibile, i messaggi vengono salvati
#     in memoria e inviati al primo reconnect disponibile.
#
#  4. Reconnect esponenziale:
#     attende 1s, 2s, 4s... fino a 60s tra un tentativo e l'altro.
#
# ============================================================

import time
import random
import os
import socket
import json
import threading
import queue

import paho.mqtt.client as mqtt

# ============================================================
# CONFIGURAZIONE — tutti i parametri vengono da env variables
# così ogni container sensore ha valori diversi senza modificare
# il codice sorgente.
# ============================================================
BROKER      = "mosquitto"
PORT        = "1883"
INTERVAL    = "30"
SENSOR_ID   = os.getenv("SENSOR_ID","sensore01")

# QoS=1: "at least once" — il broker salva il messaggio finché
# non riceve ACK dal subscriber. Nessun messaggio viene perso.
QOS_LEVEL = 1

# Quanti messaggi tenere in memoria durante una disconnessione.
# 500 × 5s = 41 minuti di dati offline prima dello scarto.
LOCAL_BUFFER_MAX = 500

# Backoff esponenziale: aspetta 1s, poi 2s, 4s... max 60s
RECONNECT_MIN = 1
RECONNECT_MAX = 60

print(f"[BOOT] ========================================", flush=True)
print(f"[BOOT] Sensore ID  : {SENSOR_ID}", flush=True)
print(f"[BOOT] Broker MQTT : {BROKER}:{PORT}", flush=True)
print(f"[BOOT] Intervallo  : {INTERVAL}s", flush=True)
print(f"[BOOT] QoS         : {QOS_LEVEL}", flush=True)
print(f"[BOOT] Buffer max  : {LOCAL_BUFFER_MAX} messaggi", flush=True)
print(f"[BOOT] ========================================", flush=True)

# ============================================================
# STATO GLOBALE (thread-safe)
# ============================================================

# Event che segnala se il client MQTT è connesso.
# Thread principale lo controlla prima di ogni publish.
connected = threading.Event()

# Coda FIFO per i messaggi accumulati mentre MQTT è offline.
# maxsize previene overflow di memoria in caso di lunga disconnessione.
local_buffer: queue.Queue = queue.Queue(maxsize=LOCAL_BUFFER_MAX)

# Contatore messaggi persi per overflow del buffer locale
stats = {"sent": 0, "buffered": 0, "dropped": 0, "flushed": 0}


# ============================================================
# UTILITY
# ============================================================

def wait_for_broker(host: str, port: int) -> None:
    """
    Prova ripetutamente una connessione TCP al broker.
    Blocca finché il broker non risponde sul porto MQTT.
    Questo evita crash del sensore se mosquitto parte lentamente.
    """
    print("[WAIT] In attesa del broker MQTT...", flush=True)
    while True:
        try:
            with socket.create_connection((host, port), timeout=5):
                pass
            print("[WAIT] Broker raggiungibile, avvio connessione MQTT.", flush=True)
            return
        except OSError as exc:
            print(f"[WAIT] Broker non disponibile ({exc}), ritento in 2s...", flush=True)
            time.sleep(2)


def flush_local_buffer(client: mqtt.Client) -> None:
    """
    Invia tutti i messaggi accumulati nel buffer locale.
    Chiamata subito dopo ogni reconnessione riuscita.
    I messaggi vengono pubblicati con QoS=1 per garantire la consegna.
    """
    count = 0
    while not local_buffer.empty():
        try:
            topic, payload = local_buffer.get_nowait()
            client.publish(topic, payload, qos=QOS_LEVEL)
            count += 1
        except queue.Empty:
            break
        except Exception as exc:
            print(f"[BUFFER] Errore durante il flush: {exc}", flush=True)
            break

    if count > 0:
        stats["flushed"] += count
        print(f"[BUFFER] Inviati {count} messaggi in coda (totale flushed: {stats['flushed']})", flush=True)


# ============================================================
# CALLBACK MQTT (API v2 di paho-mqtt)
# ============================================================

def on_connect(client: mqtt.Client, userdata, flags, reason_code, properties) -> None:
    """
    Chiamata ogni volta che la connessione (o riconnessione) ha esito.

    flags.session_present = True  → il broker aveva già la sessione memorizzata.
      In questo caso, il broker ha in coda i messaggi QoS≥1 non ancora
      consegnati al sensore (es. comandi inviati mentre era offline).

    reason_code.is_failure = False → connessione accettata.
    """
    if not reason_code.is_failure:
        connected.set()
        session_msg = "sessione RIPRISTINATA" if flags.session_present else "NUOVA sessione"
        print(f"[MQTT] ✓ Connesso al broker ({session_msg})", flush=True)
        flush_local_buffer(client)
    else:
        print(f"[MQTT] ✗ Connessione rifiutata: {reason_code}", flush=True)


def on_disconnect(client: mqtt.Client, userdata, disconnect_flags, reason_code, properties) -> None:
    """
    Chiamata ad ogni disconnessione, volontaria o per errore di rete.
    Resetta il flag connected: da questo momento i messaggi vanno al buffer.
    Il loop interno di paho-mqtt gestirà automaticamente la riconnessione.
    """
    connected.clear()
    cause = "normale" if reason_code.value == 0 else f"errore rc={reason_code}"
    print(f"[MQTT] ✗ Disconnesso ({cause}). Buffer locale attivo.", flush=True)


def on_publish(client: mqtt.Client, userdata, mid, reason_code, properties) -> None:
    """
    Callback di conferma pubblicazione (solo per QoS≥1).
    Viene chiamata quando il broker invia il PUBACK.
    Questo garantisce che il broker ha ricevuto e memorizzato il messaggio.
    """
    # Silenzioso per non inondare i log; decommentare per debug avanzato.
    # print(f"[PUBACK] mid={mid}", flush=True)
    pass


# ============================================================
# SETUP CLIENT MQTT
# ============================================================

wait_for_broker(BROKER, PORT)

# client_id = SENSOR_ID → identificatore univoco e stabile per la sessione
# clean_session = False  → il broker MANTIENE la sessione tra disconnessioni:
#   - ricorda le sottoscrizioni attive
#   - mantiene in coda i messaggi QoS 1/2 non ancora consegnati
# Senza questa opzione, ogni riconnessione sarebbe come la prima volta.
client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id=SENSOR_ID,
    protocol=mqtt.MQTTv311,
    clean_session=False,
)

client.on_connect    = on_connect
client.on_disconnect = on_disconnect
client.on_publish    = on_publish

# Backoff esponenziale integrato in paho: aspetta 1s, 2s, 4s... max 60s
# tra un tentativo di reconnect e l'altro. Evita di bombardare il broker.
client.reconnect_delay_set(min_delay=RECONNECT_MIN, max_delay=RECONNECT_MAX)

# Avvia la connessione e il loop di rete in un thread di background.
# loop_start() è non-bloccante: il main thread continua normalmente.
# keepalive=10: il broker considera il client morto dopo 10×1.5=15s senza attività.
# Il sensore invia PINGREQ ogni 10s se non pubblica dati.
# Con INTERVAL=5s il publish stesso funge da heartbeat, ma abbassare
# il keepalive riduce il tempo di rilevamento da ~90s a ~15s in caso
# di network disconnect silenzioso (Docker rimuove le rotte senza RST).
# Tradeoff: keepalive basso = più traffico PINGREQ/PINGRESP su reti lente.
client.connect(BROKER, PORT, keepalive=10)
client.loop_start()

print("[MQTT] Loop di rete avviato, in attesa della prima connessione...", flush=True)

# Blocca fino a 30 secondi per la prima connessione prima di iniziare
if not connected.wait(timeout=30):
    print("[WARN] Timeout attesa connessione iniziale. Procedo con il buffer.", flush=True)


# ============================================================
# LOOP PRINCIPALE — generazione e invio dati sensore
# ============================================================

seq = 0  # numero di sequenza: permette di rilevare messaggi persi/duplicati

while True:

    # --- Genera la lettura del sensore ---
    # In un sensore reale qui si leggerebbe un GPIO o I2C (es. DHT22)
    temperatura = round(random.uniform(15.0, 35.0), 1)
    timestamp   = time.time()   # timestamp lato sensore (non lato server)
    seq        += 1

    payload_dict = {
        "sensor_id":  SENSOR_ID,
        "temperatura": temperatura,
        "timestamp":   timestamp,
        "seq":         seq,
    }
    payload_json = json.dumps(payload_dict)
    topic        = f"fridge"

    # --- Pubblica o bufferizza ---
    if connected.is_set():
        # Connesso: pubblica con QoS=1 e aspetta la conferma del broker
        t0 = time.time()
        result = client.publish(topic, payload_json, qos=QOS_LEVEL)

        try:
            result.wait_for_publish(timeout=5.0)
            latency_ms = round((time.time() - t0) * 1000, 1)
            stats["sent"] += 1
            print(
                f"[TX]     {SENSOR_ID} → {temperatura}°C"
                f" | seq={seq} | latency={latency_ms}ms"
                f" | sent_total={stats['sent']}",
                flush=True,
            )
        except RuntimeError:
            # Il messaggio è in coda nel client paho ma non ancora confermato.
            # Con QoS=1 verrà ritrasmesso automaticamente alla riconnessione.
            print(f"[TX-QUEUED] {SENSOR_ID} → {temperatura}°C | seq={seq} (in coda paho)", flush=True)

    else:
        # Disconnesso: salva nel buffer locale come fallback secondario.
        # Il buffer principale è il broker Mosquitto (QoS=1, persistent session),
        # questo buffer copre il caso in cui anche il broker non sia raggiungibile.
        try:
            local_buffer.put_nowait((topic, payload_json))
            stats["buffered"] += 1
            print(
                f"[OFFLINE] {SENSOR_ID} → {temperatura}°C"
                f" | seq={seq}"
                f" | buffer={local_buffer.qsize()}/{LOCAL_BUFFER_MAX}",
                flush=True,
            )
        except queue.Full:
            # Buffer locale pieno: strategia FIFO — rimuovi il più vecchio.
            # Preferiamo perdere un dato vecchio che uno recente.
            try:
                local_buffer.get_nowait()
                local_buffer.put_nowait((topic, payload_json))
                stats["dropped"] += 1
                print(
                    f"[DROP] Buffer pieno! Rimosso messaggio vecchio."
                    f" Dropped_total={stats['dropped']}",
                    flush=True,
                )
            except queue.Empty:
                pass

    time.sleep(INTERVAL)
