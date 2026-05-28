# Cartella `sensor/` — Il Sensore IoT (versione CoAP)

## Cos'è questa cartella?

Qui vive il **simulatore di un sensore IoT che usa il protocollo CoAP** invece di MQTT.

La differenza fondamentale rispetto alla versione MQTT:

| MQTT | CoAP |
|---|---|
| Il sensore pubblica su un **broker** (Mosquitto) | Il sensore fa una **POST HTTP-like** direttamente al gateway |
| Il broker è un intermediario sempre acceso | Non c'è intermediario: se il gateway è spento, il sensore bufferizza |
| Protocollo **TCP** (connessione persistente) | Protocollo **UDP** (senza connessione) |
| `clean_session=False` per sessioni persistenti | Non esiste sessione: ogni messaggio è indipendente |

In un sistema reale, CoAP viene usato su microcontrollori con pochissima RAM (come Arduino o ESP32) perché il suo header è di soli **4 byte** contro i ~12 di MQTT.

---

## File nella cartella

```
sensor/
├── sensor.py          ← Tutto il comportamento del sensore
├── requirements.txt   ← aiocoap (libreria CoAP per Python)
├── Dockerfile         ← Immagine Python slim
└── README.md          ← Questo file
```

---

## `sensor.py` — Come funziona

### Il flusso principale

```
Loop ogni 5 secondi
    │
    ├─ Genera temperatura casuale (15–35°C)
    │
    ├─ Buffer locale non vuoto?
    │     SÌ → prova a svuotare il buffer prima (ordinato dal più vecchio)
    │
    ├─ Invia POST CON a coap://coap-gateway:5683/iot/temperatura
    │     Successo (ACK 2.04) → stampa [TX], aggiorna stats
    │     Timeout/errore    → salva nel buffer locale [OFFLINE]
    │
    └─ Dormi 5 secondi, ripeti
```

### Cos'è un messaggio CON (Confirmable)?

```
Sensore                     Gateway
   │                            │
   │──── POST CON ─────────────►│  Il gateway riceve
   │◄─── ACK 2.04 CHANGED ──────│  Conferma: "ricevuto"
   │                            │
```

Se l'ACK non arriva entro 2s, **aiocoap ritrasmette automaticamente** fino a 4 volte:
```
Tentativo 1: dopo 2s
Tentativo 2: dopo 4s
Tentativo 3: dopo 8s
Tentativo 4: dopo 16s
Totale: ~45s prima di dichiarare il fallimento
```

Noi impostiamo un timeout personale di **15s** per non bloccare troppo il loop principale.

### Gestione della disconnessione

```
Gateway irraggiungibile:
  → [OFFLINE] dato salvato in buffer locale (deque max 500)
  → Il deque scarta automaticamente il più vecchio se pieno [DROP]

Gateway torna raggiungibile:
  → [BUFFER] flush in ordine cronologico (popleft)
  → Poi invia il dato corrente
```

### Il payload JSON inviato

```json
{
    "sensor_id": "sensor-03",
    "temperatura": 22.4,
    "timestamp": 1748000000.123,
    "seq": 147
}
```

La URI di destinazione: `coap://coap-gateway:5683/iot/temperatura`

---

## `requirements.txt`

```
aiocoap>=0.4.7
```

**aiocoap** è la libreria Python più usata per CoAP. Gestisce automaticamente:
- Retransmission dei messaggi CON
- Deduplicazione (se lo stesso messaggio arriva due volte, viene ignorato)
- Il loop asyncio dei messaggi UDP

---

## `Dockerfile`

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY sensor.py .
CMD ["python", "-u", "sensor.py"]
```

---

## Configurazione tramite variabili d'ambiente

| Variabile | Default | Descrizione |
|---|---|---|
| `COAP_SERVER` | `coap-gateway` | Hostname del server CoAP |
| `COAP_PORT` | `5683` | Porta UDP del server |
| `SEND_INTERVAL` | `5` | Secondi tra una misura e l'altra |
| `SENSOR_ID` | `sensor-01` | Identificatore univoco del sensore |

---

## Come leggere i log

```bash
docker compose logs -f sensor03
```

| Prefisso | Significato |
|---|---|
| `[BOOT]` | Avvio: stampa la configurazione |
| `[WAIT]` | Aspetta che il gateway sia raggiungibile |
| `[COAP]` | Evento di connessione/disconnessione CoAP |
| `[TX]` | Messaggio inviato con successo (ACK ricevuto) |
| `[OFFLINE]` | Gateway non raggiungibile, dato nel buffer locale |
| `[BUFFER]` | Flush del buffer dopo ripristino connessione |
| `[DROP]` | Buffer pieno: dato vecchio scartato |

---

## Esempi pratici — Come modificare il sensore

### 1. Cambiare l'intervallo di invio

Nel `docker-compose.yml`:
```yaml
sensor03:
  environment:
    SEND_INTERVAL: "2"    # ← era "5", ora invia ogni 2 secondi
    SENSOR_ID: sensor-03
```

Riavvia solo quel sensore:
```bash
docker compose up -d --force-recreate sensor03
```

> **Attenzione:** con 10 sensori a 2s = 5 msg/s. Il gateway gestisce facilmente con il batch write, ma InfluxDB avrà più carico.

---

### 2. Cambiare il range di temperatura simulata

Apri `sensor.py` e trova:
```python
def genera_temperatura() -> float:
    return round(random.uniform(15.0, 35.0), 1)
```

Simulare un sensore esterno invernale:
```python
return round(random.uniform(-5.0, 10.0), 1)
```

Simulare un forno industriale:
```python
return round(random.uniform(180.0, 240.0), 1)
```

Dopo la modifica, ricostruisci:
```bash
docker compose build sensor03 --no-cache
docker compose up -d --force-recreate sensor03
```

---

### 3. Aggiungere l'umidità al payload

Nel `sensor.py`, modifica la funzione di generazione e il payload:

```python
import random

def genera_umidita() -> float:
    return round(random.uniform(30.0, 90.0), 1)

# Nel loop principale, aggiorna il payload:
payload = {
    "sensor_id":   SENSOR_ID,
    "temperatura": genera_temperatura(),
    "umidita":     genera_umidita(),     # ← aggiunto
    "timestamp":   time.time(),
    "seq":         seq,
}
```

Poi nel `gateway.py` aggiungi il campo `umidita` nel Point InfluxDB:
```python
p = (
    Point("temperatura")
    .tag("sensor_id", data.get("sensor_id", "unknown"))
    .field("value",   float(data["temperatura"]))
    .field("umidita", float(data.get("umidita", 0)))  # ← aggiunto
    .field("seq",     int(data.get("seq", 0)))
)
```

---

### 4. Aggiungere un undicesimo sensore

Nel `docker-compose.yml`:
```yaml
sensor11:
  <<: *sensor-base
  container_name: sensor-11
  environment:
    <<: *sensor-env
    SENSOR_ID: sensor-11
```

Avvia solo il nuovo sensore:
```bash
docker compose up -d sensor11
```

---

### 5. Aumentare il buffer locale (più resilienza offline)

In `sensor.py`:
```python
LOCAL_BUFFER_MAX = 500   # default: ~41 minuti a 5s/msg
# Per 4 ore di disconnessione:
LOCAL_BUFFER_MAX = 2880  # 2880 × 5s = 14400s = 4 ore
```

> **Nota:** a differenza di MQTT (buffer su disco nel broker), questo buffer è in **RAM**. Se il container del sensore si riavvia, i dati vengono persi.

---

### 6. Cambiare il timeout CON (più o meno paziente)

```python
COAP_TIMEOUT = 15.0   # default: 15 secondi

# Più aggressivo (scopre il problema prima, ma più retransmission):
COAP_TIMEOUT = 5.0

# Più paziente (aspetta l'intero ciclo aiocoap ~45s):
COAP_TIMEOUT = 50.0
```

---

### 7. Simulare un sensore difettoso

```python
# 5% di probabilità di valore anomalo
if random.random() < 0.05:
    temperatura = round(random.uniform(80.0, 100.0), 1)  # spike!
else:
    temperatura = genera_temperatura()
```

Usa per testare le soglie di alerting in Node-RED.

---

## Differenza rispetto alla versione MQTT

| Aspetto | MQTT (`sensor.py`) | CoAP (`sensor.py`) |
|---|---|---|
| Libreria | `paho-mqtt` | `aiocoap` |
| Programmazione | Sincrona con thread | **Asincrona** con asyncio |
| Connessione | `client.connect()` permanente | Nessuna connessione: ogni POST è indipendente |
| Conferma consegna | PUBACK dal broker | ACK 2.04 dal gateway |
| Sessione persistente | `clean_session=False` | **Non esiste** in CoAP |
| Retransmission | Gestita da paho-mqtt | Gestita da aiocoap (CON) |
| Buffer locale | `queue.Queue(500)` | `collections.deque(maxlen=500)` |

---

## Domande frequenti

**Q: Perché asyncio invece di threading come nella versione MQTT?**  
A: aiocoap è una libreria asyncio nativa. Non funziona bene con i thread tradizionali. asyncio gestisce la concorrenza con un singolo thread e coroutine — è il modello di programmazione standard per I/O non bloccante in Python moderno.

**Q: CoAP usa UDP. I pacchetti UDP non vengono persi?**  
A: Sì, UDP può perdere pacchetti. Per questo usiamo messaggi **CON (Confirmable)**: se il gateway non risponde, aiocoap ritrasmette automaticamente. È il protocollo stesso che garantisce la consegna, non TCP.

**Q: Cosa succede se il gateway riavvia mentre il sensore sta inviando?**  
A: Il gateway perde il buffer interno (RAM). I sensori hanno già ricevuto ACK, quindi credono che i dati siano stati salvati. I dati in transito (non ancora scritti su InfluxDB) vengono persi. Per evitarlo, il gateway usa il **batch write immediato** (non aspetta 30s come prima).
