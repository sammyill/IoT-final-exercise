"""
sensor.py — Simulatore di sensore di temperatura IoT con CoAP
=============================================================
Progetto didattico ITS 3° anno — Protocollo CoAP (RFC 7252)

Differenze rispetto a MQTT:
- Non c'è un broker: il sensore è CLIENT CoAP, il gateway è SERVER
- UDP (non TCP): nessuna connessione persistente
- Messaggi CON (Confirmable): il server deve rispondere con ACK
  → se non risponde, aiocoap ritrasmette (max ~45s con 4 tentativi)
- Buffer locale: copre i periodi in cui il gateway non è raggiungibile

Flusso dati:
  sensore --[POST CON]--> coap-gateway:5683/iot/temperatura
"""

import asyncio
import aiocoap
import json
import random
import time
import os
import socket
from collections import deque

# ──────────────────────────────────────────────────────────────
# Configurazione da variabili d'ambiente
# ──────────────────────────────────────────────────────────────
COAP_SERVER   ="coap-gateway"
COAP_PORT     = "5683"
SEND_INTERVAL = "30"
SENSOR_ID     = os.getenv("SENSOR_ID", "sensor-01")

COAP_URI      = f"coap://{COAP_SERVER}:{COAP_PORT}/iot/temperatura"

# Timeout per messaggio CON:
# aiocoap ritrasmette fino a 4 volte (2+4+8+16=30s totali)
# Noi usciamo dopo 15s per non bloccare troppo il loop
COAP_TIMEOUT  = 15.0

# Buffer locale per i messaggi quando il gateway non risponde
LOCAL_BUFFER_MAX = 500
local_buffer = deque(maxlen=LOCAL_BUFFER_MAX)  # FIFO, scarta il più vecchio se pieno

# Statistiche operative
stats = {
    "sent":     0,   # messaggi consegnati con successo
    "buffered": 0,   # messaggi in attesa nel buffer locale
    "dropped":  0,   # messaggi persi (buffer pieno)
    "flushed":  0,   # messaggi recuperati dal buffer dopo reconnessione
}

seq = 0  # numero di sequenza progressivo


# ──────────────────────────────────────────────────────────────
# Generazione valore temperatura simulato
# ──────────────────────────────────────────────────────────────
def genera_temperatura() -> float:
    """
    Simula un sensore di temperatura con piccole variazioni casuali.
    Range normale: 18–28 °C. Occasionalmente produce valori anomali
    per testare il sistema di alerting.
    """
    base = 22.0
    rumore = random.gauss(0, 2.0)  # distribuzione normale, σ=2°C

    # 5% di probabilità di anomalia alta/bassa (per testare gli alert)
    if random.random() < 0.025:
        rumore += random.uniform(10, 15)   # picco alto
    elif random.random() < 0.025:
        rumore -= random.uniform(8, 12)    # picco basso

    return round(base + rumore, 2)


# ──────────────────────────────────────────────────────────────
# Invio singolo messaggio CON (Confirmable)
# ──────────────────────────────────────────────────────────────
async def try_send(protocol, uri: str, payload_json: str) -> bool:
    """
    Invia un POST CON al gateway CoAP.

    CON = Confirmable: il server DEVE rispondere con ACK.
    Se non risponde, aiocoap ritrasmette automaticamente.
    Noi aspettiamo al massimo COAP_TIMEOUT secondi.

    Restituisce True se il server ha risposto con 2.xx (successo).
    """
    request = aiocoap.Message(
        code=aiocoap.POST,
        uri=uri,
        payload=payload_json.encode("utf-8"),
        mtype=aiocoap.CON,   # Confirmable — richiede ACK dal server
    )
    try:
        response = await asyncio.wait_for(
            protocol.request(request).response,
            timeout=COAP_TIMEOUT
        )
        return response.code.is_successful()
    except asyncio.TimeoutError:
        return False
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────
# Attesa disponibilità DNS del gateway
# ──────────────────────────────────────────────────────────────
async def wait_for_server():
    """
    Aspetta che il nome host del gateway sia risolvibile via DNS.

    Nota: CoAP usa UDP, non TCP → non possiamo fare un "connect"
    per verificare la raggiungibilità. Verifichiamo solo il DNS,
    poi confidiamo nel meccanismo CON/ACK per la consegna effettiva.
    """
    print(f"[WAIT] Risoluzione DNS per {COAP_SERVER}:{COAP_PORT}...")
    while True:
        try:
            socket.getaddrinfo(COAP_SERVER, COAP_PORT)
            print(f"[WAIT] DNS ok → {COAP_SERVER} raggiungibile")
            return
        except socket.gaierror:
            print(f"[WAIT] DNS non ancora pronto, riprovo tra 3s...")
            await asyncio.sleep(3)


# ──────────────────────────────────────────────────────────────
# Loop principale del sensore
# ──────────────────────────────────────────────────────────────
async def main():
    global seq

    # Blocco di avvio (utile per il debug e il monitoraggio)
    print("=" * 55)
    print(f"[BOOT] Sensore CoAP avviato")
    print(f"[BOOT]   SENSOR_ID     = {SENSOR_ID}")
    print(f"[BOOT]   COAP_SERVER   = {COAP_SERVER}")
    print(f"[BOOT]   COAP_PORT     = {COAP_PORT}")
    print(f"[BOOT]   COAP_URI      = {COAP_URI}")
    print(f"[BOOT]   SEND_INTERVAL = {SEND_INTERVAL}s")
    print(f"[BOOT]   BUFFER_MAX    = {LOCAL_BUFFER_MAX} messaggi")
    print(f"[BOOT]   COAP_TIMEOUT  = {COAP_TIMEOUT}s")
    print(f"[BOOT]   Tipo messaggio: CON (Confirmable)")
    print("=" * 55)

    # Aspetta che il gateway sia raggiungibile
    await wait_for_server()

    # Crea il contesto CoAP (client UDP)
    protocol = await aiocoap.Context.create_client_context()

    # Flag per rilevare il ritorno online dopo un periodo offline
    server_era_offline = False

    print(f"[COAP] Contesto client CoAP creato, inizio trasmissione")

    while True:
        loop_start = time.monotonic()

        seq += 1
        temperatura = genera_temperatura()
        ts = time.time()

        # Payload JSON con tutti i metadati
        payload = {
            "sensor_id":   SENSOR_ID,
            "temperatura": temperatura,
            "ts":          ts,
            "seq":         seq,
            "buffered":    len(local_buffer),  # quanti messaggi in coda
        }
        payload_json = json.dumps(payload)

        # ── Tentativo di invio ──────────────────────────────
        ok = await try_send(protocol, COAP_URI, payload_json)

        if ok:
            stats["sent"] += 1

            # Se eravamo offline, prima svuotiamo il buffer
            if server_era_offline and len(local_buffer) > 0:
                buf_size = len(local_buffer)
                print(f"[BUFFER] Gateway tornato online! Flush di {buf_size} messaggi in coda...")
                flushed = 0
                while local_buffer:
                    old_payload = local_buffer.popleft()
                    old_json = json.dumps(old_payload)
                    flush_ok = await try_send(protocol, COAP_URI, old_json)
                    if flush_ok:
                        flushed += 1
                        stats["flushed"] += 1
                    else:
                        # Se fallisce di nuovo, rimette in cima (ma deque non ha appendleft limit issue)
                        local_buffer.appendleft(old_payload)
                        break
                    # Piccola pausa per non inondare il gateway
                    await asyncio.sleep(0.1)

                print(f"[BUFFER] Flush completato: {flushed}/{buf_size} messaggi recuperati")

            server_era_offline = False
            print(
                f"[TX] seq={seq:05d} | {SENSOR_ID} | {temperatura:6.2f}°C | "
                f"buf={len(local_buffer)} | sent={stats['sent']} | flushed={stats['flushed']}"
            )

        else:
            # Invio fallito → salva nel buffer locale
            server_era_offline = True
            buf_was_full = len(local_buffer) == LOCAL_BUFFER_MAX

            # deque con maxlen gestisce automaticamente l'overflow FIFO
            local_buffer.append(payload)

            if buf_was_full:
                stats["dropped"] += 1
                print(
                    f"[DROP] seq={seq:05d} | Buffer pieno ({LOCAL_BUFFER_MAX}), "
                    f"messaggio più vecchio scartato | dropped={stats['dropped']}"
                )
            else:
                stats["buffered"] += 1
                print(
                    f"[OFFLINE] seq={seq:05d} | Gateway non raggiungibile → "
                    f"buffer={len(local_buffer)}/{LOCAL_BUFFER_MAX} | "
                    f"buffered={stats['buffered']}"
                )

        # ── Attesa fino al prossimo ciclo ──────────────────
        elapsed = time.monotonic() - loop_start
        sleep_time = max(0.0, SEND_INTERVAL - elapsed)
        await asyncio.sleep(sleep_time)


# ──────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n[COAP] Sensore {SENSOR_ID} fermato")
        print(f"[COAP] Statistiche finali: {stats}")
