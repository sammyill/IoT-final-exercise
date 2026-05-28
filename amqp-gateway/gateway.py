#!/usr/bin/env python3
"""
AMQP Gateway — Consumer RabbitMQ → InfluxDB

Consuma la queue 'iot.temperature', accumula messaggi in batch,
poi scrive su InfluxDB in un'unica chiamata HTTP (batch write).

Garanzie:
- Manual ACK: il messaggio viene rimosso dalla queue SOLO dopo la scrittura.
  Se il gateway crasha, RabbitMQ ri-consegna i messaggi (at-least-once).
- Multiple ACK: basic_ack(multiple=True) acknowledges l'intero batch con
  un solo frame AMQP → riduce il traffico del 95%.
- NACK + requeue: se InfluxDB è down, i messaggi restano nella queue.
- JSON malformato → NACK senza requeue → va in DLQ (iot.temperature.dlq).
- Prefetch: RabbitMQ non invia più di PREFETCH_COUNT messaggi non-acked
  per volta → backpressure naturale.

Architettura:

  RabbitMQ 'iot.temperature'
       │ CONSUME (prefetch=50)
       ▼
  on_message() → pending[]
       │
       ├─ len >= BATCH_SIZE? → flush()
       │
  process_data_events(2s)
       │
       └─ timer scaduto + pending? → flush()

  flush():
    write_api.write(points) → InfluxDB
    OK  → basic_ack(last, multiple=True)
    ERR → basic_nack(last, multiple=True, requeue=True)
"""

import os
import json
import time
import signal

import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.write.point import Point

# ── Configurazione ─────────────────────────────────────────────────────────────
RABBITMQ_HOST   = os.getenv("RABBITMQ_HOST",   "rabbitmq")
RABBITMQ_PORT   = int(os.getenv("RABBITMQ_PORT",  "5672"))
RABBITMQ_USER   = os.getenv("RABBITMQ_USER",   "iot")
RABBITMQ_PASS   = os.getenv("RABBITMQ_PASS",   "iot-password")
QUEUE_NAME      = os.getenv("QUEUE_NAME",      "iot.temperature")
EXCHANGE_NAME   = os.getenv("EXCHANGE_NAME",   "iot.sensors")

INFLUX_URL      = os.getenv("INFLUX_URL",      "http://influxdb:8086")
INFLUX_TOKEN    = os.getenv("INFLUX_TOKEN",    "my-super-token")
INFLUX_ORG      = os.getenv("INFLUX_ORG",      "its")
INFLUX_BUCKET   = os.getenv("INFLUX_BUCKET",   "iot")

PREFETCH_COUNT  = int(os.getenv("PREFETCH_COUNT", "50"))
BATCH_SIZE      = int(os.getenv("BATCH_SIZE",     "20"))
BATCH_TIMEOUT   = float(os.getenv("BATCH_TIMEOUT", "2.0"))

# ── Stato globale ──────────────────────────────────────────────────────────────
pending    = []
last_flush = time.time()
write_api  = None
running    = True
stats      = {"received": 0, "written": 0, "errors": 0, "nacked": 0}


def log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


# ── InfluxDB ───────────────────────────────────────────────────────────────────

def init_influxdb() -> None:
    global write_api
    log(f"[INFLUX] Attendo InfluxDB su {INFLUX_URL}...")
    while True:
        try:
            client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
            if client.health().status == "pass":
                write_api = client.write_api(write_options=SYNCHRONOUS)
                log("[INFLUX] ✓ InfluxDB pronto!")
                return
        except Exception as e:
            log(f"[INFLUX] Non raggiungibile: {e}. Riprovo tra 5s...")
        time.sleep(5)


def build_point(data: dict) -> Point:
    p = (
        Point("temperatura")
        .tag("sensor_id", data.get("sensor_id", "unknown"))
        .field("value",   float(data["temperatura"]))
        .field("seq",     int(data.get("seq", 0)))
    )
    if "timestamp" in data:
        p = p.time(int(data["timestamp"] * 1_000_000_000))
    return p


def flush(ch) -> None:
    global pending, last_flush
    last_flush = time.time()
    if not pending:
        return

    batch   = pending[:]
    pending = []
    last_dt = batch[-1][0]
    points  = [build_point(d) for _, d in batch]

    try:
        write_api.write(bucket=INFLUX_BUCKET, record=points)
        ch.basic_ack(delivery_tag=last_dt, multiple=True)
        stats["written"] += len(batch)
        log(f"[INFLUX] ✓ Scritti {len(batch)} punti in batch | "
            f"totale={stats['written']} | buf={len(pending)}")
    except Exception as e:
        stats["errors"] += 1
        try:
            ch.basic_nack(delivery_tag=last_dt, multiple=True, requeue=True)
        except Exception:
            pass
        stats["nacked"] += len(batch)
        pending = batch + pending
        log(f"[ERR] Batch write fallito (#{stats['errors']}): {e}")
        log(f"[ERR] {len(batch)} msg rimandati in queue (nack+requeue)")
        time.sleep(2)


# ── Consumer ───────────────────────────────────────────────────────────────────

def on_message(ch, method, properties, body) -> None:
    global pending
    try:
        data = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as e:
        log(f"[ERR] JSON malformato: {e} | body={body[:60]!r}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    pending.append((method.delivery_tag, data))
    stats["received"] += 1

    if stats["received"] % 20 == 0:
        log(f"[RX] ricevuti={stats['received']} | scritti={stats['written']} | "
            f"buf={len(pending)} | nacked={stats['nacked']} | "
            f"ultimo: {data.get('sensor_id')} → {data.get('temperatura')}°C")

    if len(pending) >= BATCH_SIZE:
        flush(ch)


def init_rabbitmq():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host="/",
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=300,
    )
    while True:
        try:
            log(f"[AMQP] Connessione a {RABBITMQ_HOST}:{RABBITMQ_PORT}...")
            conn = pika.BlockingConnection(params)
            ch   = conn.channel()
            ch.basic_qos(prefetch_count=PREFETCH_COUNT)
            ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)
            ch.queue_declare(
                queue=QUEUE_NAME, durable=True,
                arguments={
                    "x-message-ttl": 3_600_000,
                    "x-max-length": 10_000,
                    "x-dead-letter-exchange": "iot.dlx",
                    "x-dead-letter-routing-key": "temperatura",
                },
            )
            ch.queue_bind(queue=QUEUE_NAME, exchange=EXCHANGE_NAME,
                          routing_key="sensor.#.temperatura")
            ch.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)
            log(f"[AMQP] ✓ Connesso! Consumer attivo su queue='{QUEUE_NAME}'")
            log(f"[AMQP]   prefetch={PREFETCH_COUNT} | batch={BATCH_SIZE} msg o {BATCH_TIMEOUT}s")
            return conn, ch
        except Exception as e:
            log(f"[AMQP] Connessione fallita: {e}. Riprovo tra 5s...")
            time.sleep(5)


def main() -> None:
    global running

    log("[BOOT] AMQP Gateway avviato")
    log(f"[BOOT]   RABBITMQ  = {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    log(f"[BOOT]   QUEUE     = {QUEUE_NAME}")
    log(f"[BOOT]   INFLUX    = {INFLUX_URL}")
    log(f"[BOOT]   PREFETCH  = {PREFETCH_COUNT}")
    log(f"[BOOT]   BATCH     = {BATCH_SIZE} msg / {BATCH_TIMEOUT}s")

    def shutdown(sig, frame):
        global running
        log("[BOOT] Shutdown...")
        running = False

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    init_influxdb()
    connection, channel = init_rabbitmq()

    while running:
        try:
            connection.process_data_events(time_limit=BATCH_TIMEOUT)
            if pending and (time.time() - last_flush >= BATCH_TIMEOUT):
                flush(channel)
        except (AMQPConnectionError, AMQPChannelError, Exception) as e:
            log(f"[AMQP] Connessione persa: {e}. Riconnessione...")
            try:
                connection.close()
            except Exception:
                pass
            pending.clear()
            time.sleep(3)
            connection, channel = init_rabbitmq()

    log("[BOOT] Gateway fermato.")


if __name__ == "__main__":
    main()
