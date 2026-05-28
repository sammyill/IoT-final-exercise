# Cartella `amqp-gateway/` — Consumer AMQP → InfluxDB

## Cos'è questa cartella?

Qui vive il **gateway** che legge i messaggi dalla queue RabbitMQ e li scrive su InfluxDB.

```
MQTT:   Sensore → Mosquitto → Telegraf → InfluxDB
CoAP:   Sensore → coap-gateway          → InfluxDB
AMQP:   Sensore → RabbitMQ → amqp-gateway → InfluxDB
                  (broker)   (consumer)
```

Il gateway AMQP non tocca i sensori: si limita a consumare la queue e scrivere su InfluxDB. RabbitMQ si occupa di routing, persistenza, retry e DLQ.

---

## File nella cartella

```
amqp-gateway/
├── gateway.py         ← Consumer pika + batch writer InfluxDB
├── requirements.txt   ← pika + influxdb-client
├── Dockerfile         ← Immagine Python slim
└── README.md          ← Questo file
```

---

## `gateway.py` — Come funziona

### Architettura interna

```
RabbitMQ 'iot.temperature'
    │ CONSUME (prefetch=50)
    ▼
on_message() → pending[]
    │
    ├─ len(pending) >= BATCH_SIZE → flush()
    │
process_data_events(time_limit=2s)
    │
    └─ timer scaduto + pending → flush()

flush():
  write_api.write(points) → InfluxDB
  OK  → basic_ack(last_dt, multiple=True)
  ERR → basic_nack(last_dt, multiple=True, requeue=True)
  JSON ERR → basic_nack(requeue=False) → DLQ
```

### Prefetch + Manual ACK

```
RabbitMQ                    Gateway
   │── msg1 (unacked) ─────►│
   │── msg2 (unacked) ─────►│  pending = [msg1, msg2]
   │── msg3 (unacked) ─────►│  pending = [msg1..msg3]
   │  (prefetch=3: stop)    │
   │◄── basic_ack(msg3, multiple=True) ── flush() OK
   │── msg4 ───────────────►│  (libero slot per altri 3)
```

**Backpressure naturale**: il gateway non riceve più messaggi di quanti riesca a processare.

### Multiple ACK

Con 20 messaggi per batch, un solo `basic_ack(multiple=True)` acknowledges tutto il batch → riduce il traffico AMQP del 95% rispetto ad ack individuali.

### At-Least-Once

```
Scenario: gateway crasha durante flush()

1. msg1..msg20 ricevuti (non ancora acked)
2. flush() scrive su InfluxDB
3. Gateway crasha prima dell'ack
4. RabbitMQ: nessun ACK ricevuto → ri-consegna msg1..msg20

→ I dati arrivano due volte su InfluxDB.
  InfluxDB gestisce i duplicati tramite timestamp (same point = overwrite).
```

### JSON malformato → DLQ

```python
except json.JSONDecodeError:
    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    # → RabbitMQ invia a iot.dlx → iot.temperature.dlq
```

---

## Configurazione tramite variabili d'ambiente

| Variabile | Default | Descrizione |
|---|---|---|
| `RABBITMQ_HOST` | `rabbitmq` | Hostname broker |
| `RABBITMQ_PORT` | `5672` | Porta AMQP |
| `QUEUE_NAME` | `iot.temperature` | Queue da consumare |
| `EXCHANGE_NAME` | `iot.sensors` | Exchange (per dichiarazione) |
| `INFLUX_URL` | `http://influxdb:8086` | Endpoint InfluxDB |
| `INFLUX_TOKEN` | `my-super-token` | Token autenticazione |
| `INFLUX_ORG` | `its` | Organizzazione InfluxDB |
| `INFLUX_BUCKET` | `iot` | Bucket di destinazione |
| `PREFETCH_COUNT` | `50` | Max messaggi non-acked in RAM |
| `BATCH_SIZE` | `20` | Messaggi per batch write |
| `BATCH_TIMEOUT` | `2.0` | Secondi prima di flush forzato |

---

## Come leggere i log

```bash
docker compose logs -f amqp-gateway
```

| Prefisso | Significato |
|---|---|
| `[BOOT]` | Avvio: configurazione |
| `[INFLUX]` | Stato connessione InfluxDB |
| `[AMQP]` | Stato connessione RabbitMQ |
| `[RX]` | Riepilogo ogni 20 messaggi |
| `[INFLUX] ✓` | Batch scritto con successo |
| `[ERR]` | Errore batch write o JSON malformato |

Log sano:
```
[BOOT] AMQP Gateway avviato
[INFLUX] ✓ InfluxDB pronto!
[AMQP] ✓ Connesso! Consumer attivo su queue='iot.temperature'
[INFLUX] ✓ Scritti 20 punti in batch | totale=20 | buf=0
[RX] ricevuti=20 | scritti=20 | buf=0 | nacked=0 | ultimo: sensor-07 → 23.4°C
```

Log durante `drop_gateway` chaos:
```
[AMQP] Connessione persa: Connection refused. Riconnessione...
[AMQP] ✓ Connesso!
[INFLUX] ✓ Scritti 20 punti | totale=120 | buf=30   ← burst dalla queue
[INFLUX] ✓ Scritti 20 punti | totale=140 | buf=10
[INFLUX] ✓ Scritti 10 punti | totale=150 | buf=0
```

---

## Esempi pratici — Come modificare il gateway

### 1. Cambiare batch size e timeout

Nel `.env`:
```env
BATCH_SIZE=50        # batch più grande → meno write, più latenza
BATCH_TIMEOUT=5      # flush ogni 5s se batch non è pieno
```

**Tradeoff:**
- Batch più grande → meno chiamate HTTP a InfluxDB, dati in dashboard con più ritardo
- Batch più piccolo → più latenza in dashboard, più carico su InfluxDB

---

### 2. Aggiungere una measurement per l'umidità

In `gateway.py`:
```python
def build_point(data: dict) -> list:
    points = []
    p_temp = (
        Point("temperatura")
        .tag("sensor_id", data.get("sensor_id", "unknown"))
        .field("value", float(data["temperatura"]))
    )
    points.append(p_temp)
    if "umidita" in data:
        p_humi = (
            Point("umidita")
            .tag("sensor_id", data.get("sensor_id", "unknown"))
            .field("value", float(data["umidita"]))
        )
        points.append(p_humi)
    return points
```

Aggiorna `flush()` per appiattire la lista:
```python
points = []
for _, data in batch:
    points.extend(build_point(data))
write_api.write(bucket=INFLUX_BUCKET, record=points)
```

---

### 3. Aggiungere un consumer sulla DLQ

```python
def on_dlq_message(ch, method, properties, body):
    log(f"[DLQ] Messaggio malformato: {body[:100]!r}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Nel setup:
ch.basic_consume(
    queue="iot.temperature.dlq",
    on_message_callback=on_dlq_message,
    auto_ack=False,
)
```

---

### 4. Scrivere su due bucket InfluxDB

```python
def flush(ch):
    ...
    write_api.write(bucket=INFLUX_BUCKET, record=points)   # produzione
    if stats["written"] % (BATCH_SIZE * 10) == 0:
        write_api.write(bucket="iot-debug", record=points[:1])  # debug
```

---

### 5. Aumentare il prefetch per svuotare più velocemente una queue piena

Nel `.env`:
```env
PREFETCH_COUNT=200
```

Utile per drenare rapidamente una queue accumulata durante un chaos `drop_gateway`.

> **Attenzione:** con prefetch alto, il crash del gateway aumenta il numero di messaggi re-consegnati (potenziale duplicazione su InfluxDB).

---

### 6. Test manuale via Management API

```bash
# Invia un dato direttamente all'exchange
curl -s -u iot:iot-password \
  -X POST -H "Content-Type: application/json" \
  -d '{
    "properties": {"delivery_mode": 2},
    "routing_key": "sensor.test-manual.temperatura",
    "payload": "{\"sensor_id\":\"test-manual\",\"temperatura\":42.0,\"timestamp\":0,\"seq\":1}",
    "payload_encoding": "string"
  }' \
  http://localhost:15672/api/exchanges/%2F/iot.sensors/publish

# Verifica su InfluxDB
docker exec influxdb influx query \
  --org its --token my-super-token \
  'from(bucket:"iot") |> range(start:-1m) |> filter(fn:(r) => r.sensor_id == "test-manual")'
```

---

### 7. Esporre metriche Prometheus

```python
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            body = (
                f"gateway_received_total {stats['received']}\n"
                f"gateway_written_total {stats['written']}\n"
                f"gateway_errors_total {stats['errors']}\n"
                f"gateway_nacked_total {stats['nacked']}\n"
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(body)
    def log_message(self, *args): pass

server = HTTPServer(("0.0.0.0", 9090), MetricsHandler)
threading.Thread(target=server.serve_forever, daemon=True).start()
```

---

## Confronto con le altre versioni

| Aspetto | MQTT (Telegraf) | CoAP (gateway.py) | AMQP (gateway.py) |
|---|---|---|---|
| Input | MQTT subscribe | CoAP POST resource | **AMQP consume** |
| Garanzia | Best-effort | ACK sempre (buffer gw) | **Manual ACK** |
| Buffer offline | RAM Telegraf | RAM (deque) | **Queue broker** durable |
| Messaggi malformati | Ignorati | Ignorati | **DLQ** |
| Backpressure | Limitata | Nessuna | **Prefetch nativo** |
| Scalabilità | Un'istanza | Un'istanza | **Multi-istanza** (round-robin) |

---

## Domande frequenti

**Q: Cosa succede se avvio due istanze del gateway?**
A: RabbitMQ distribuisce i messaggi in **round-robin**: ogni messaggio va a un solo consumer. Scaling orizzontale automatico senza configurazione aggiuntiva.

**Q: I messaggi in `pending` vengono persi se il gateway si riavvia?**
A: Non definitivamente. Non erano stati acked → RabbitMQ li ri-consegna. L'unico rischio è una doppia scrittura su InfluxDB (gestita dal timestamp).

**Q: Con prefetch=50 e batch_size=20, quanti messaggi sono in RAM?**
A: Al massimo 50 (prefetch limit). Il flush avviene ogni 20 messaggi, quindi `pending` raramente supera 20 elementi in condizioni normali.
