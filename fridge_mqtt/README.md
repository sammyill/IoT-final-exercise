# Cartella `sensor/` — Il Sensore IoT

## Cos'è questa cartella?

Qui vive il **simulatore di un sensore di temperatura IoT**.  
In un sistema reale, al posto di questo codice Python ci sarebbe un microcontrollore fisico (es. un Raspberry Pi con un sensore DHT22 collegato al GPIO). Noi lo simuliamo via software per studiare il comportamento della rete senza dover comprare hardware.

Un **sensore IoT** fa tre cose:
1. **Misura** qualcosa (temperatura, umidità, pressione…)
2. **Impacchetta** il dato in un messaggio
3. **Lo invia** al broker MQTT tramite la rete

---

## File nella cartella

```
sensor/
├── sensor.py          ← Il codice del sensore (tutto il comportamento)
├── requirements.txt   ← Le librerie Python necessarie
├── Dockerfile         ← Come costruire l'immagine Docker
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
    ├─ Connesso a Mosquitto?
    │     SÌ → pubblica con QoS=1, aspetta conferma (PUBACK)
    │     NO → salva in buffer locale (max 500 messaggi)
    │
    └─ Dormi 5 secondi, ripeti
```

### Le garanzie di affidabilità

Il sensore è progettato per **non perdere mai un dato**, anche in caso di problemi di rete:

| Meccanismo | Cosa fa | Dove nel codice |
|---|---|---|
| **QoS=1** | Il broker conferma ogni messaggio ricevuto (PUBACK) | `client.publish(..., qos=1)` |
| **clean_session=False** | Il broker ricorda questo sensore anche da offline | `mqtt.Client(..., clean_session=False)` |
| **Buffer locale** | Se il broker non è raggiungibile, salva in memoria | `queue.Queue(maxsize=500)` |
| **Backoff esponenziale** | Riprova connessione: 1s, 2s, 4s… max 60s | `client.reconnect_delay_set(1, 60)` |
| **keepalive=10s** | Il broker si accorge che il sensore è offline in ~15s | `client.connect(..., keepalive=10)` |

### Il formato del messaggio JSON inviato

```json
{
    "sensor_id": "sensor03",
    "temperatura": 22.4,
    "timestamp": 1748000000.123,
    "seq": 147
}
```

- `sensor_id`: chi ha mandato il dato
- `temperatura`: la misura (gradi Celsius)
- `timestamp`: quando è stato misurato (Unix epoch, secondi)
- `seq`: numero di sequenza → permette di scoprire se si perde qualcosa

### Il topic MQTT usato

```
iot/aula/sensor03/temperatura
```

Struttura: `iot` / `aula` / `{sensor_id}` / `{tipo_misura}`

---

## `requirements.txt` — Le dipendenze

```
paho-mqtt>=2.0.0
```

**paho-mqtt** è la libreria ufficiale Eclipse per usare MQTT in Python.  
La versione 2.x cambia la firma dei callback (più pulita, type-safe).

---

## `Dockerfile` — Come si costruisce il container

```dockerfile
FROM python:3.11-slim          # immagine Python minimale (~150MB)
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt   # prima le dipendenze (cache Docker)
COPY sensor.py .
CMD ["python", "-u", "sensor.py"]    # -u = stdout non bufferizzato
```

L'ordine `requirements.txt` prima di `sensor.py` non è casuale:  
Docker usa una **cache per layer**. Se cambi solo `sensor.py`, non reinstalla le librerie — usa la cache. Risparmia minuti di build.

---

## Configurazione tramite variabili d'ambiente

Il sensore si configura senza toccare il codice, solo con variabili d'ambiente nel `docker-compose.yml`:

| Variabile | Default | Descrizione |
|---|---|---|
| `MQTT_BROKER` | `mosquitto` | Hostname del broker (nome servizio Docker) |
| `MQTT_PORT` | `1883` | Porta MQTT |
| `SEND_INTERVAL` | `5` | Secondi tra una misura e l'altra |
| `SENSOR_ID` | `sensor` | Identificatore univoco del sensore |

---

## Come leggere i log del sensore

```bash
# Segui i log in tempo reale del sensore03
docker compose logs -f sensor03

# Solo gli ultimi 20 log
docker compose logs sensor03 --tail 20
```

**Decodifica dei prefissi:**

```
[BOOT]    → avvio del sensore, stampa la configurazione
[WAIT]    → sta aspettando che il broker sia raggiungibile
[MQTT]    → evento di connessione o disconnessione
[TX]      → messaggio inviato con successo (QoS=1 confermato)
[TX-QUEUED] → messaggio in coda paho (non ancora confermato)
[OFFLINE] → broker non raggiungibile, dato salvato in buffer locale
[BUFFER]  → svuotamento buffer dopo riconnessione
[DROP]    → buffer pieno, dato vecchio scartato (dopo 500 messaggi offline)
```

---

## Esempi pratici — Come modificare il sensore

### 1. Cambiare l'intervallo di invio

Nel `docker-compose.yml`, trova il sensore che vuoi modificare e cambia `SEND_INTERVAL`:

```yaml
sensor01:
  environment:
    SEND_INTERVAL: "2"    # ← era "5", ora invia ogni 2 secondi
    SENSOR_ID: sensor01
```

Riavvia solo quel sensore:
```bash
docker compose up -d --force-recreate sensor01
```

> **Nota:** Con 10 sensori a 1s = 10 msg/s sul broker. Controlla che Mosquitto regga.

---

### 2. Cambiare il range di temperatura simulata

Apri `sensor.py` e trova la riga:

```python
temperatura = round(random.uniform(15.0, 35.0), 1)
```

Cambiala per simulare un ambiente esterno invernale:

```python
temperatura = round(random.uniform(-10.0, 5.0), 1)
```

Oppure un forno industriale:

```python
temperatura = round(random.uniform(180.0, 220.0), 1)
```

Ricostruisci l'immagine e riavvia:
```bash
docker compose build sensor01
docker compose up -d --force-recreate sensor01
```

---

### 3. Aggiungere un secondo valore: umidità

Nel `sensor.py`, modifica il payload per inviare due misure insieme:

```python
# Prima (solo temperatura):
payload_dict = {
    "sensor_id":  SENSOR_ID,
    "temperatura": temperatura,
    "timestamp":   timestamp,
    "seq":         seq,
}

# Dopo (temperatura + umidità):
umidita = round(random.uniform(30.0, 90.0), 1)
payload_dict = {
    "sensor_id":   SENSOR_ID,
    "temperatura": temperatura,
    "umidita":     umidita,
    "timestamp":   timestamp,
    "seq":         seq,
}
```

Poi in `telegraf.conf` aggiungi `umidita` ai campi numerici da salvare.

---

### 4. Aggiungere un undicesimo sensore

Nel `docker-compose.yml`, copia un blocco sensore esistente e cambia l'ID:

```yaml
sensor11:
  <<: *sensor-base
  container_name: sensor-11
  environment:
    <<: *sensor-env
    SENSOR_ID: sensor11
```

Avvia solo il nuovo sensore (gli altri non si riavviano):
```bash
docker compose up -d sensor11
```

---

### 5. Simulare un sensore difettoso (dati anomali casuali)

Modifica `sensor.py` per simulare un sensore che ogni tanto impazzisce:

```python
import random

# 5% di probabilità di dato anomalo (spike)
if random.random() < 0.05:
    temperatura = round(random.uniform(80.0, 100.0), 1)  # spike!
else:
    temperatura = round(random.uniform(15.0, 35.0), 1)   # normale
```

Questo è utile per testare le **regole di alerting** in Node-RED: l'email di allarme arriva davvero?

---

### 6. Aumentare il buffer locale (più resilienza offline)

Se vuoi che il sensore sopporti disconnessioni più lunghe senza perdere dati, aumenta `LOCAL_BUFFER_MAX`:

```python
# sensor.py, riga ~50
LOCAL_BUFFER_MAX = 500   # default: 41 minuti a 5s/msg

# Per sopportare 4 ore di disconnessione:
LOCAL_BUFFER_MAX = 2880  # 2880 × 5s = 14400s = 4 ore
```

> **Attenzione:** ogni messaggio occupa ~200 byte in RAM.  
> 2880 messaggi ≈ 576 KB per sensore × 10 sensori = ~5.7 MB totali. Accettabile.

---

## Domande frequenti

**Q: Perché il sensore non si ferma subito quando lo disconnetto dalla rete?**  
A: Il protocollo TCP non invia un segnale di "connessione persa" quando Docker rimuove una rotta di rete. Mosquitto si accorge della caduta solo dopo `keepalive × 1.5 = 15 secondi`. È un comportamento normale di TCP su reti dove i pacchetti spariscono silenziosamente.

**Q: Cosa succede ai dati inviati mentre il sensore era offline?**  
A: Vengono salvati nel buffer locale (`queue.Queue`). Appena il broker è di nuovo raggiungibile, `flush_local_buffer()` li invia tutti in ordine FIFO.

**Q: Perché uso `seq` (numero di sequenza)?**  
A: Per rilevare buchi. Se ricevi seq=100, poi seq=103, significa che i messaggi 101 e 102 sono stati persi. Con QoS=1 non dovrebbe succedere, ma il seq è il tuo strumento di verifica.
