"""
amqp-gateway — HTTP receiver + alert emailer
============================================
Riceve dati di temperatura via HTTP POST da due fonti distinte
e invia email a Mailhog quando la temperatura supera la soglia.

Endpoints:
  POST /fridge    ← chiamato da Telegraf (sensori MQTT)
  POST /factory   ← chiamato da factory-gateway (sensori CoAP)
  GET  /health    ← stato e statistiche

Architettura:
  HTTP request → asyncio.Queue per-source (fridge | factory)
                    │
                    ▼
              consumer task → threshold check → SMTP to Mailhog

Cooldown: dopo un'email per sensor_id X, sopprime ulteriori email
sullo stesso sensore per COOLDOWN_SECS (default 300s = 5 min).
Questo evita di inondare la inbox quando un sensore resta "caldo".
"""

import asyncio
import os
import smtplib
import time
from email.mime.text import MIMEText

from aiohttp import web

# ── Config (da variabili d'ambiente) ────────────────────────────
HTTP_PORT       = int(os.getenv("HTTP_PORT",      "8000"))
TEMP_THRESHOLD  = float(os.getenv("TEMP_THRESHOLD", "30.0"))
SMTP_HOST       = os.getenv("SMTP_HOST",          "mailhog")
SMTP_PORT       = int(os.getenv("SMTP_PORT",      "1025"))
ALERT_FROM      = os.getenv("ALERT_FROM",         "iot-gateway@example.com")
ALERT_TO        = os.getenv("ALERT_TO",           "ops@example.com")
COOLDOWN_SECS   = int(os.getenv("COOLDOWN_SECS",  "300"))
QUEUE_MAX       = int(os.getenv("QUEUE_MAX",      "1000"))

# Stato globale
fridge_queue:  asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX)
factory_queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX)
last_alert: dict = {}  # sensor_id → epoch dell'ultima email inviata
stats = {
    "rx_fridge":   0,
    "rx_factory":  0,
    "emails_sent": 0,
    "suppressed":  0,
    "errors":      0,
}


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ── Normalizzazione payload ────────────────────────────────────
# Telegraf invia in batch:
#   {"metrics":[{"name":"...","fields":{"value":25.4,"seq":1},
#                "tags":{"sensor_id":"sensor01","topic":"fridge"},
#                "timestamp":1700000000}]}
# Il factory-gateway invia il payload nativo del sensore:
#   {"sensor_id":"factory01","temperatura":25.4,"ts":...,"seq":1}
# Questa funzione restituisce sempre una lista di record normalizzati.

def _from_telegraf_metric(m: dict):
    fields = m.get("fields", {}) or {}
    tags   = m.get("tags",   {}) or {}
    sensor_id = tags.get("sensor_id") or fields.get("sensor_id")
    # Telegraf usa "value" come field nominale; fallback su "temperatura"
    temp = fields.get("value", fields.get("temperatura"))
    if sensor_id is None or temp is None:
        return None
    try:
        return {"sensor_id": str(sensor_id), "temperatura": float(temp)}
    except (TypeError, ValueError):
        return None


def _from_native(d: dict):
    sensor_id = d.get("sensor_id")
    temp = d.get("temperatura")
    if sensor_id is None or temp is None:
        return None
    try:
        return {"sensor_id": str(sensor_id), "temperatura": float(temp)}
    except (TypeError, ValueError):
        return None


def normalize(payload) -> list:
    if isinstance(payload, dict) and "metrics" in payload:
        return [r for r in (_from_telegraf_metric(m) for m in payload["metrics"]) if r]
    if isinstance(payload, list):
        return [r for r in (_from_native(d) for d in payload if isinstance(d, dict)) if r]
    if isinstance(payload, dict):
        single = _from_native(payload)
        return [single] if single else []
    return []


# ── SMTP (sincrono, eseguito in thread executor) ───────────────
def send_email(source: str, sensor_id: str, temp: float) -> bool:
    body = (
        f"ALERT: temperatura fuori soglia\n"
        f"Sorgente   : {source}\n"
        f"Sensor ID  : {sensor_id}\n"
        f"Temperatura: {temp:.2f}°C (soglia {TEMP_THRESHOLD}°C)\n"
        f"Timestamp  : {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    msg = MIMEText(body)
    msg["Subject"] = f"[IoT-Alert] {source}/{sensor_id} = {temp:.1f}°C"
    msg["From"] = ALERT_FROM
    msg["To"]   = ALERT_TO
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as s:
            s.send_message(msg)
        return True
    except Exception as e:
        log(f"[SMTP] ✗ Invio fallito: {e}")
        return False


# ── Consumer asincrono per ciascuna queue ──────────────────────
async def consumer(name: str, queue: asyncio.Queue):
    log(f"[CONSUMER] {name} avviato "
        f"(threshold={TEMP_THRESHOLD}°C, cooldown={COOLDOWN_SECS}s)")
    loop = asyncio.get_event_loop()
    while True:
        record = await queue.get()
        try:
            sensor_id = record["sensor_id"]
            temp      = record["temperatura"]

            if temp <= TEMP_THRESHOLD:
                continue

            now  = time.time()
            last = last_alert.get(sensor_id, 0.0)
            if now - last < COOLDOWN_SECS:
                stats["suppressed"] += 1
                remaining = int(COOLDOWN_SECS - (now - last))
                log(f"[ALERT] {name}/{sensor_id}={temp:.2f}°C — "
                    f"SOPPRESSA (cooldown {remaining}s)")
                continue

            # smtplib è bloccante: spostiamolo in un thread per non
            # fermare l'event loop di aiohttp.
            ok = await loop.run_in_executor(None, send_email, name, sensor_id, temp)
            if ok:
                last_alert[sensor_id] = now
                stats["emails_sent"] += 1
                log(f"[ALERT] ✉ {name}/{sensor_id}={temp:.2f}°C — "
                    f"email inviata (tot={stats['emails_sent']})")
            else:
                stats["errors"] += 1
        except Exception as e:
            stats["errors"] += 1
            log(f"[CONSUMER] {name} errore: {e}")
        finally:
            queue.task_done()


# ── Handler HTTP ───────────────────────────────────────────────
async def _ingest(request, source: str, queue: asyncio.Queue, stat_key: str):
    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"error": "invalid json"}, status=400)

    records = normalize(payload)
    accepted = 0
    for r in records:
        stats[stat_key] += 1
        try:
            queue.put_nowait(r)
            accepted += 1
        except asyncio.QueueFull:
            stats["errors"] += 1
            log(f"[{source}] Queue piena, scarto {r['sensor_id']}={r['temperatura']}")

    # Log periodico, non a ogni request
    if stats[stat_key] % 20 == 1:
        log(f"[RX] {source}: rx_tot={stats[stat_key]} | "
            f"emails={stats['emails_sent']} | suppressed={stats['suppressed']}")

    return web.json_response({"received": accepted})


async def handle_fridge(request):
    return await _ingest(request, "fridge", fridge_queue, "rx_fridge")


async def handle_factory(request):
    return await _ingest(request, "factory", factory_queue, "rx_factory")


async def handle_health(request):
    return web.json_response({
        "status": "ok",
        "threshold": TEMP_THRESHOLD,
        "cooldown_secs": COOLDOWN_SECS,
        "queues": {
            "fridge":  fridge_queue.qsize(),
            "factory": factory_queue.qsize(),
        },
        "stats": stats,
    })


# ── Main ───────────────────────────────────────────────────────
async def main():
    log("=" * 60)
    log("[BOOT] amqp-gateway (HTTP receiver + alert emailer)")
    log(f"[BOOT]   HTTP_PORT      = {HTTP_PORT}")
    log(f"[BOOT]   TEMP_THRESHOLD = {TEMP_THRESHOLD}°C")
    log(f"[BOOT]   SMTP           = {SMTP_HOST}:{SMTP_PORT}")
    log(f"[BOOT]   FROM → TO      = {ALERT_FROM} → {ALERT_TO}")
    log(f"[BOOT]   COOLDOWN       = {COOLDOWN_SECS}s per sensore")
    log(f"[BOOT]   QUEUE_MAX      = {QUEUE_MAX}")
    log("=" * 60)

    app = web.Application()
    app.router.add_post("/fridge",  handle_fridge)
    app.router.add_post("/factory", handle_factory)
    app.router.add_get ("/health",  handle_health)

    asyncio.create_task(consumer("fridge",  fridge_queue))
    asyncio.create_task(consumer("factory", factory_queue))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HTTP_PORT)
    await site.start()
    log(f"[BOOT] HTTP server in ascolto su 0.0.0.0:{HTTP_PORT}")
    log("[BOOT] Endpoints: POST /fridge, POST /factory, GET /health")

    # Loop infinito
    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log(f"[BOOT] Stop. Stats finali: {stats}")
