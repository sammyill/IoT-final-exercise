# Cartella `telegraf/` — Il Gateway IoT

## Cos'è questa cartella?

Qui vive la configurazione di **Telegraf**, il gateway che fa da ponte tra il broker MQTT e il database InfluxDB.

Pensa a Telegraf come a un **traduttore e corriere**:
- Si **iscrive** al broker Mosquitto e riceve i messaggi dei sensori
- **Trasforma** il formato JSON dei sensori nel formato Line Protocol di InfluxDB
- **Scrive** i dati nel database in batch efficienti

Telegraf è un agente open-source sviluppato da InfluxData. È scritto in Go, quindi è estremamente leggero e veloce. Supporta centinaia di sorgenti dati (input) e decine di destinazioni (output) — MQTT e InfluxDB sono solo due delle sue possibilità.

---

## File nella cartella

```
telegraf/
├── telegraf.conf     ← L'unico file di configurazione del gateway
└── README.md         ← Questo file
```

> Non c'è un `Dockerfile` perché usiamo l'immagine ufficiale `telegraf:latest` senza modifiche. La configurazione viene montata come volume read-only.

---

## `telegraf.conf` — Spiegazione sezione per sezione

### `[agent]` — Comportamento generale

```toml
[agent]
  interval         = "10s"    # raccoglie/processa ogni 10 secondi
  round_interval   = true     # allinea agli intervalli esatti (:00, :10, :20…)
  metric_batch_size = 1000    # invia a InfluxDB in batch da max 1000 punti
  flush_interval   = "10s"   # scarica il buffer verso InfluxDB ogni 10s
  flush_jitter     = "2s"    # ±2s casuali per evitare burst sincronizzati
  hostname         = "telegraf-gateway"
```

Per l'input MQTT (event-driven), `interval` non ha effetto diretto: i messaggi arrivano quando i sensori li inviano. Quello che conta è `flush_interval`: i dati ricevuti vengono accumulati e scritti su InfluxDB ogni 10 secondi, non uno per uno.

### `[[inputs.mqtt_consumer]]` — Ricezione da MQTT

```toml
[[inputs.mqtt_consumer]]
  servers            = ["tcp://mosquitto:1883"]
  topics             = ["iot/aula/+/temperatura"]
  qos                = 1
  persistent_session = true
  client_id          = "telegraf-iot-gateway"
  data_format        = "json"
  json_string_fields = ["sensor_id"]
  tag_keys           = ["sensor_id"]
```

| Parametro | Valore | Perché |
|---|---|---|
| `servers` | `tcp://mosquitto:1883` | Nome servizio Docker del broker |
| `topics` | `iot/aula/+/temperatura` | `+` = wildcard per un livello (tutti i sensori) |
| `qos` | `1` | Almeno una consegna garantita (con PUBACK) |
| `persistent_session` | `true` | Mosquitto mette in coda i messaggi se Telegraf è offline |
| `client_id` | `telegraf-iot-gateway` | Deve essere **stabile e univoco** per recuperare la sessione |
| `data_format` | `json` | Il payload dei sensori è JSON |
| `json_string_fields` | `["sensor_id"]` | `sensor_id` è una stringa, non un numero |
| `tag_keys` | `["sensor_id"]` | Promuove `sensor_id` a tag InfluxDB (indicizzato) |

**Perché `persistent_session = true` e `client_id` stabile sono fondamentali:**  
Se Telegraf perde la connessione (es. chaos event `drop_gateway`), Mosquitto continua a ricevere dati dai sensori e li mette in coda per `telegraf-iot-gateway`. Al reconnect, Telegraf scarica tutta la coda e nessun punto viene perso in InfluxDB.

### `[[outputs.influxdb_v2]]` — Scrittura su InfluxDB

```toml
[[outputs.influxdb_v2]]
  urls         = ["http://influxdb:8086"]
  token        = "my-super-token"
  organization = "its"
  bucket       = "iot"
  timeout      = "5s"
```

Telegraf usa le API v2 di InfluxDB (HTTP REST). Il token deve coincidere con `DOCKER_INFLUXDB_INIT_ADMIN_TOKEN` nel `docker-compose.yml`.

---

## Il flusso dei dati

```
Sensore invia JSON ogni 5s
        │
        ▼
  Mosquitto (broker)
        │  QoS=1, sessione persistente
        ▼
  Telegraf [inputs.mqtt_consumer]
        │  riceve il JSON, estrae i campi
        │  accumula per flush_interval=10s
        ▼
  Telegraf [outputs.influxdb_v2]
        │  scrive in formato Line Protocol:
        │  temperatura,sensor_id=sensor03 value=22.4 1748000000000000000
        ▼
  InfluxDB bucket "iot"
```

**Formato Line Protocol** (quello che Telegraf invia a InfluxDB):
```
<measurement>,<tag_key>=<tag_value> <field_key>=<field_value> <timestamp_ns>
temperatura,sensor_id=sensor03 value=22.4,seq=147 1748000000000000000
```

---

## Come leggere i log di Telegraf

```bash
docker compose logs -f gateway
```

Log tipici:
```
# Avvio corretto
I! [agent] Config: Interval:10s, Quiet:false, Hostname:"telegraf-gateway"
I! [inputs.mqtt_consumer] Connected to broker: tcp://mosquitto:1883
I! [outputs.influxdb_v2] Connected to http://influxdb:8086

# Scrittura riuscita (silenzioso: compare solo in modalità debug)

# Errore connessione InfluxDB (transitorio, riprova automaticamente)
E! [outputs.influxdb_v2] Post "http://influxdb:8086/api/v2/write": dial tcp: ...

# Errore autenticazione InfluxDB
E! [outputs.influxdb_v2] When writing: 401 Unauthorized
```

---

## Esempi pratici — Come modificare Telegraf

### 1. Aggiungere un secondo topic (es. umidità)

Se i sensori iniziano a inviare anche dati di umidità su un nuovo topic:

```toml
# telegraf.conf
[[inputs.mqtt_consumer]]
  topics = [
    "iot/aula/+/temperatura",
    "iot/aula/+/umidita",      # ← aggiunto
  ]
```

Oppure con la wildcard multi-livello `#` (attenzione: cattura TUTTO):
```toml
  topics = ["iot/aula/#"]    # tutto quello che inizia con iot/aula/
```

Riavvia il gateway:
```bash
docker compose restart gateway
```

---

### 2. Ripristinare il timestamp originale del sensore

Per default la configurazione attuale usa il tempo di arrivo in Telegraf. Se vuoi usare il timestamp del sensore (quando il dato è stato misurato):

```toml
# telegraf.conf — sezione [[inputs.mqtt_consumer]]
# Decommenta queste due righe:
json_time_key    = "timestamp"
json_time_format = "unix"
```

⚠️ Effetto collaterale: dopo un chaos event, i messaggi in coda arriveranno con il timestamp di quando erano stati generati (qualche minuto fa). InfluxDB li salva nel passato — comportamento corretto ma può sorprendere.

---

### 3. Velocizzare il flush verso InfluxDB (dati più freschi)

Se vuoi che i dati appaiano in InfluxDB ogni 2 secondi invece di 10:

```toml
[agent]
  flush_interval = "2s"    # era "10s"
  flush_jitter   = "0s"    # rimuovi il jitter
```

> **Tradeoff:** più chiamate HTTP verso InfluxDB → più carico sul database e sulla rete. Con 10 sensori a 5s, non è necessario — i dati arrivano comunque entro 10s.

---

### 4. Aggiungere filtri: scarta dati fuori range

Telegraf supporta **processori** che trasformano o filtrano i dati in pipeline:

```toml
# Aggiungi questa sezione DOPO [[inputs.mqtt_consumer]]
[[processors.pivot]]
  # (solo se ristrutturi il payload)

# Oppure usa uno stencil per scartare valori anomali:
[[processors.override]]
  namepass = ["temperatura"]   # elabora solo la measurement "temperatura"

# Scarta misure con temperatura impossibile (< -50 o > 150)
[[aggregators.minmax]]
  period = "30s"
  drop_original = false
```

Per filtri più precisi usa `[[processors.starlark]]` con codice Python/Starlark:
```toml
[[processors.starlark]]
  source = '''
def apply(metric):
    temp = metric.fields.get("temperatura", 0)
    if temp < -50 or temp > 150:
        return None     # scarta il punto
    return metric
'''
```

---

### 5. Aggiungere un secondo output: file CSV (debug)

Puoi scrivere gli stessi dati su un file locale per ispezione manuale:

```toml
[[outputs.file]]
  files   = ["/tmp/debug.csv"]
  data_format = "csv"
```

Monta il volume nel `docker-compose.yml`:
```yaml
gateway:
  volumes:
    - ./telegraf/telegraf.conf:/etc/telegraf/telegraf.conf:ro
    - ./telegraf/debug:/tmp    # ← aggiunto
```

Il file `./telegraf/debug/debug.csv` si aggiornerà in tempo reale.

---

### 6. Monitorare le performance di Telegraf stesso

Telegraf può auto-monitorarsi e inviare le sue metriche interne a InfluxDB:

```toml
# Aggiungi in cima al telegraf.conf
[[inputs.internal]]
  collect_memstats = true
```

In InfluxDB troverai una measurement `internal_agent` con:
- `metrics_written`: quanti punti scritti su InfluxDB
- `metrics_dropped`: quanti punti scartati (buffer pieno)
- `gather_errors`: errori di raccolta

---

### 7. Cambiare il bucket di destinazione

Se vuoi scrivere in un bucket separato per test:

```toml
[[outputs.influxdb_v2]]
  bucket = "iot-test"    # ← era "iot"
```

Crea prima il bucket in InfluxDB (http://localhost:8086 → Load Data → Buckets → Create Bucket).

---

### 8. Aggiungere una seconda istanza di Telegraf per un secondo sito

Supponiamo tu voglia aggiungere un secondo laboratorio (`aula-b`) con altri sensori. Nel `docker-compose.yml`:

```yaml
gateway-b:
  image: telegraf:latest
  container_name: mqtt-gateway-b
  volumes:
    - ./telegraf/telegraf-b.conf:/etc/telegraf/telegraf.conf:ro
  networks:
    - core_net
```

Crea `telegraf/telegraf-b.conf` con:
```toml
[[inputs.mqtt_consumer]]
  topics    = ["iot/aula-b/+/temperatura"]
  client_id = "telegraf-iot-gateway-b"   # ← DIVERSO dall'altro!
  persistent_session = true

[[outputs.influxdb_v2]]
  bucket = "iot-aula-b"    # bucket separato
  token  = "my-super-token"
  urls   = ["http://influxdb:8086"]
  organization = "its"
```

---

## Domande frequenti

**Q: Perché `client_id` deve essere stabile e diverso per ogni istanza Telegraf?**  
A: Il `client_id` è come un "nome utente" per MQTT. Mosquitto lo usa per ricordare quale sessione persistente appartiene a chi. Se due istanze usano lo stesso `client_id`, si "rubano" la sessione a vicenda e perdono i messaggi in coda. Se il `client_id` cambia ad ogni avvio (random), Mosquitto crea sempre una sessione nuova e i messaggi accodati durante la disconnessione vengono persi.

**Q: Cosa succede ai dati se Telegraf è down per 30 minuti?**  
A: Mosquitto mette in coda fino a `max_queued_messages=1000` messaggi per Telegraf. Con 10 sensori a 5s → 2 msg/s → 1000 msg coprono 500 secondi (~8 minuti). Dopo 8 minuti, i messaggi più vecchi vengono scartati da Mosquitto (FIFO). Per estendere, aumenta `max_queued_messages` in `mosquitto.conf`.

**Q: L'errore "server misbehaving" su DNS compare nei log. Cosa significa?**  
A: Il Docker DNS (127.0.0.11) ha avuto un problema transitorio nel risolvere il nome `influxdb`. Solitamente si risolve da solo. Se persiste:
```bash
docker compose restart gateway
```

**Q: Telegraf e Node-RED scrivono entrambi su InfluxDB. Non si duplicano i dati?**  
A: No. Telegraf scrive dati grezzi dei sensori (measurement `temperatura`). Node-RED nella configurazione attuale **non** scrive su InfluxDB in parallelo — usa i dati MQTT solo per la dashboard e gli alert. Se entrambi scrivessero, vedresti punti duplicati. Puoi verificarlo con una Flux query su InfluxDB.
