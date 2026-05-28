# Cartella `coap-gateway/` — Il Server CoAP e Gateway verso InfluxDB

## Cos'è questa cartella?

Qui vive il **cuore del progetto CoAP**: il server che riceve i dati dai sensori e li scrive nel database.

Nella versione MQTT, questo ruolo era diviso in **due** componenti:
- **Mosquitto** (broker): riceveva i messaggi e li metteva in coda
- **Telegraf** (gateway): leggeva dal broker e scriveva su InfluxDB

Con CoAP, i due ruoli sono **unificati** in un unico processo Python:

```
MQTT:   Sensore → Mosquitto → Telegraf → InfluxDB
CoAP:   Sensore → coap-gateway          → InfluxDB
```

Il gateway fa tre cose in modo asincrono e indipendente:
1. **Ascolta** su UDP 5683 e risponde alle POST dei sensori (sempre con ACK)
2. **Bufferizza** i dati in RAM se InfluxDB è temporaneamente irraggiungibile
3. **Scrive** su InfluxDB in batch ogni secondo (una sola chiamata HTTP per molti punti)

---

## File nella cartella

```
coap-gateway/
├── gateway.py         ← Server CoAP + writer InfluxDB (tutto qui)
├── requirements.txt   ← aiocoap + influxdb-client
├── Dockerfile         ← Immagine Python slim
└── README.md          ← Questo file
```

---

## `gateway.py` — Come funziona

### Architettura interna

```
UDP :5683
    │
    │ pacchetti CoAP
    ▼
┌─────────────────────────────────────────────┐
│  aiocoap event loop (asyncio, single thread) │
│                                             │
│  TemperaturaResource.render_post()          │
│   └─ decodifica JSON                        │
│   └─ aggiunge a internal_buffer (deque)     │
│   └─ risponde immediatamente ACK 2.04       │
│                                             │
│  influx_writer_loop() [task background]     │
│   └─ ogni 1s: drena tutto internal_buffer   │
│   └─ chiama sync_write_batch() in executor  │
│   └─ in caso di errore: reinserisce batch   │
└─────────────────────────────────────────────┘
         │ ThreadPoolExecutor (1 worker)
         │ sync_write_batch() — BLOCCANTE
         ▼
    InfluxDB HTTP API :8086
```

### Perché il batch write?

Il problema con la scrittura punto per punto:

```
10 sensori × 1 msg/5s = 2 msg/s in ingresso
1 write ogni 2s        = 0.5 msg/s in uscita
Accumulo netto         = 1.5 msg/s → buffer pieno in ~22 min
```

Con il batch write:

```
Ogni secondo: scrivi TUTTI i punti accumulati → 1 chiamata HTTP
Throughput limitato solo dalla rete, non dalla frequenza del loop
Buffer rimane sempre vicino a 0 in condizioni normali
```

### Il principio "ACK sempre"

Il gateway risponde **immediatamente** con `2.04 CHANGED` al sensore, **anche se InfluxDB è down**. Questo significa:
- Il sensore sa che il gateway ha il dato → non lo ritiene nel buffer locale
- Il gateway si assume la responsabilità della persistenza (buffer interno)
- Se il gateway crasha, i dati nel buffer interno vengono persi (tradeoff accettabile per un lab)

### Le risorse CoAP esposte

```
coap://coap-gateway:5683/iot/temperatura   ← POST: ricevi dato sensore
coap://coap-gateway:5683/.well-known/core  ← GET: discovery delle risorse
```

La risorsa `.well-known/core` è standard CoAP (RFC 6690) e permette di scoprire quali risorse offre un server:
```bash
# Scopri le risorse del gateway
docker exec coap-gateway python3 -c "
import asyncio, aiocoap
async def d():
    ctx = await aiocoap.Context.create_client_context()
    req = aiocoap.Message(code=aiocoap.GET, uri='coap://localhost:5683/.well-known/core')
    resp = await ctx.request(req).response
    print(resp.payload.decode())
asyncio.run(d())
"
```

---

## `requirements.txt`

```
aiocoap>=0.4.7
influxdb-client>=1.36.0
```

- **aiocoap**: server e client CoAP per Python asyncio
- **influxdb-client**: client ufficiale InfluxDB v2 (API HTTP/REST)

---

## `Dockerfile`

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY gateway.py .
CMD ["python", "-u", "gateway.py"]
```

---

## Configurazione tramite variabili d'ambiente

| Variabile | Default | Descrizione |
|---|---|---|
| `INFLUX_URL` | `http://influxdb:8086` | Endpoint InfluxDB |
| `INFLUX_TOKEN` | `my-super-token` | Token di autenticazione |
| `INFLUX_ORG` | `its` | Organizzazione InfluxDB |
| `INFLUX_BUCKET` | `iot` | Bucket di destinazione |
| `COAP_PORT` | `5683` | Porta UDP in ascolto |

---

## Come leggere i log

```bash
docker compose logs -f coap-gateway
```

| Prefisso | Significato |
|---|---|
| `[BOOT]` | Avvio: stampa la configurazione |
| `[INFLUX]` | Stato della connessione a InfluxDB |
| `[RX]` | Riepilogo messaggi ricevuti (ogni 20) |
| `[INFLUX] ✓` | Batch scritto con successo su InfluxDB |
| `[BUFFER]` | Buffer interno pieno: dato vecchio scartato |
| `[ERR]` | Errore di scrittura su InfluxDB |

Log sano tipico:
```
[BOOT] CoAP Gateway avviato
[BOOT]   INFLUX_URL    = http://influxdb:8086
[INFLUX] Attendo InfluxDB su http://influxdb:8086/health...
[INFLUX] InfluxDB pronto! Status: pass
[BOOT] Server CoAP in ascolto su 0.0.0.0:5683/udp
[INFLUX] ✓ Scritti 20 punti in batch | totale=20 | buf=0
[RX] ricevuti=21 | scritti=20 | buf=0 | ultimo: sensor-07 → 23.4°C
[INFLUX] ✓ Scritti 11 punti in batch | totale=31 | buf=0
```

---

## Esempi pratici — Come modificare il gateway

### 1. Aumentare il buffer interno (più resilienza se InfluxDB è down)

In `gateway.py`:
```python
INTERNAL_BUFFER_MAX = 2000   # default

# Per reggere 30 minuti di InfluxDB down (10 sensori × 1/5s × 1800s):
INTERNAL_BUFFER_MAX = 3600
```

Ricostruisci:
```bash
docker compose build coap-gateway --no-cache
docker compose up -d --force-recreate coap-gateway
```

---

### 2. Aggiungere una nuova risorsa CoAP (es. umidità)

Aggiungere una risorsa separata per un tipo diverso di dato:

```python
# In gateway.py, dopo la classe TemperaturaResource, aggiungi:
class UmiditaResource(resource.Resource):
    """Risorsa CoAP per i dati di umidità."""

    async def render_post(self, request):
        try:
            data = json.loads(request.payload.decode("utf-8"))
            data["tipo"] = "umidita"
            data["gw_ts"] = time.time()
            internal_buffer.append(data)
        except Exception as e:
            print(f"[ERR] UmiditaResource: {e}")
        return aiocoap.Message(code=aiocoap.CHANGED)
```

Registra la risorsa in `main()`:
```python
root.add_resource(["iot", "temperatura"], TemperaturaResource())
root.add_resource(["iot", "umidita"],     UmiditaResource())   # ← aggiunto
```

E nel `sync_write_batch()` gestisci il campo `tipo`:
```python
for data in batch:
    measurement = data.get("tipo", "temperatura")
    p = (
        Point(measurement)             # ← usa il tipo come measurement
        .tag("sensor_id", data.get("sensor_id", "unknown"))
        .field("value", float(data.get(measurement, 0)))
        .field("seq", int(data.get("seq", 0)))
    )
    points.append(p)
```

---

### 3. Esporre un endpoint CoAP GET (lettura dati)

Puoi rendere il gateway anche un server che risponde a richieste GET con l'ultimo valore ricevuto:

```python
# Stato globale: ultimo valore per sensore
last_values = {}

class StatusResource(resource.Resource):
    """Risponde a GET con l'ultimo valore di tutti i sensori."""

    async def render_get(self, request):
        import json
        payload = json.dumps(last_values, indent=2).encode()
        return aiocoap.Message(code=aiocoap.CONTENT, payload=payload)

# In TemperaturaResource.render_post():
last_values[data["sensor_id"]] = data["temperatura"]  # aggiorna stato

# In main():
root.add_resource(["iot", "status"], StatusResource())
```

Test:
```bash
docker exec coap-gateway python3 -c "
import asyncio, aiocoap
async def g():
    ctx = await aiocoap.Context.create_client_context()
    req = aiocoap.Message(code=aiocoap.GET, uri='coap://localhost:5683/iot/status')
    resp = await ctx.request(req).response
    print(resp.payload.decode())
asyncio.run(g())
"
```

---

### 4. Aggiungere CoAP Observe (push dal gateway verso Node-RED)

L'estensione **Observe** (RFC 7641) permette a Node-RED di iscriversi al gateway e ricevere aggiornamenti automatici — simile al subscribe MQTT.

```python
from aiocoap.resource import ObservableResource

class TemperaturaObservableResource(ObservableResource):
    """Risorsa osservabile: i client ricevono aggiornamenti automatici."""

    def __init__(self):
        super().__init__()
        self.last_data = {}

    async def render_get(self, request):
        # Restituisce l'ultimo valore
        payload = json.dumps(self.last_data).encode()
        return aiocoap.Message(code=aiocoap.CONTENT, payload=payload)

    async def update_observation_count(self, count):
        pass  # non serve fare nulla

    def update_value(self, data):
        """Chiamato da render_post quando arriva un nuovo dato."""
        self.last_data = data
        self.updated_state()  # notifica tutti gli observer
```

Con Observe, Node-RED potrebbe usare `node-red-contrib-coap` per ricevere dati in push invece di fare polling su InfluxDB.

---

### 5. Cambiare il batch size massimo

Il batch è attualmente limitato a 500 punti per chiamata:
```python
while internal_buffer and len(batch) < 500:
    batch.append(internal_buffer.popleft())
```

Per carichi più elevati (es. 50 sensori):
```python
while internal_buffer and len(batch) < 2000:  # batch più grande
    batch.append(internal_buffer.popleft())
```

> **Nota:** InfluxDB v2 accetta batch fino a ~5000 punti per chiamata. Oltre, è consigliabile spezzare in più chiamate.

---

### 6. Scrivere su due bucket separati

Puoi scrivere contemporaneamente su due bucket (es. `iot` per produzione, `iot-debug` per debug):

```python
def sync_write_batch(batch: list):
    points = [...]  # costruisci i punti

    # Scrivi su bucket principale
    write_api.write(bucket=INFLUX_BUCKET, record=points)

    # Scrivi su bucket di debug (solo un campione ogni 10)
    if stats["written"] % 10 == 0:
        write_api.write(bucket="iot-debug", record=points[:1])
```

---

### 7. Test manuale: invia un dato al gateway

```bash
# Dal container stesso (localhost)
docker exec coap-gateway python3 -c "
import asyncio, aiocoap, json, time

async def test():
    ctx = await aiocoap.Context.create_client_context()
    payload = json.dumps({
        'sensor_id': 'test-manual',
        'temperatura': 42.0,
        'timestamp': time.time(),
        'seq': 1
    }).encode()
    req = aiocoap.Message(
        code=aiocoap.POST,
        uri='coap://localhost:5683/iot/temperatura',
        payload=payload,
        mtype=aiocoap.CON,
    )
    resp = await ctx.request(req).response
    print('Risposta gateway:', resp.code)

asyncio.run(test())
"

# Verifica in InfluxDB
docker exec influxdb influx query \
  --org its --token my-super-token \
  'from(bucket:"iot") |> range(start:-1m) |> filter(fn:(r) => r.sensor_id == "test-manual")'
```

---

## Differenza rispetto alla versione MQTT (Telegraf)

| Aspetto | MQTT (Telegraf) | CoAP (gateway.py) |
|---|---|---|
| Linguaggio | Configurazione TOML | **Python** (modificabile) |
| Input | MQTT subscribe | **CoAP POST** resource |
| Output | InfluxDB v2 line protocol | InfluxDB v2 Python client |
| Buffer offline | Interno a Telegraf | **deque in RAM** (gateway.py) |
| Persistenza buffer | RAM (Telegraf) | RAM (gateway.py) — stesse garanzie |
| Batch write | Automatico (Telegraf lo gestisce) | **Implementato manualmente** nel writer loop |
| Nuovi input | Modifica `telegraf.conf` | **Aggiungi una classe Resource** in Python |
| Flessibilità | Limitata ai plugin Telegraf | **Illimitata**: codice Python completo |

---

## Domande frequenti

**Q: Il gateway usa 1 solo ThreadPoolExecutor worker. Non è un bottleneck?**  
A: No, perché il batch write raggruppa tutti i punti disponibili in una singola chiamata HTTP. Il thread è bloccato per la durata di quella chiamata (tipicamente <100ms), poi è subito libero per il batch successivo.

**Q: Cosa succede se InfluxDB è down per 30 minuti?**  
A: I dati si accumulano nel buffer interno (max 2000 punti). Con 10 sensori a 5s = 2 msg/s, il buffer si riempie in `2000/2 = 1000s ≈ 16 minuti`. Dopo, i dati più vecchi vengono scartati. Per aumentare la finestra, aumenta `INTERNAL_BUFFER_MAX`.

**Q: Il gateway può scalare a 100 sensori?**  
A: Sì. UDP è molto leggero — aiocoap gestisce centinaia di connessioni simultanee su un singolo thread asyncio. Il vero bottleneck diventa InfluxDB. Con 100 sensori a 5s = 20 msg/s, il batch write gestisce facilmente (InfluxDB scrive migliaia di punti al secondo).
