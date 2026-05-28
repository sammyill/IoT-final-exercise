# Cartella `nodered/` — Dashboard e Alerting (versione AMQP)

## Cos'è questa cartella?

Qui vive la configurazione di **Node-RED** per la versione AMQP del progetto.

Come nella versione CoAP, Node-RED usa il **polling** verso InfluxDB per la dashboard. La differenza dalla versione MQTT è che non c'è un broker da cui sottoscriversi in modo passivo.

```
MQTT:   Sensore → Broker → [Node-RED riceve automaticamente] → Dashboard
AMQP:   Sensore → RabbitMQ → Gateway → InfluxDB ← [Node-RED polling ogni 5s] → Dashboard
CoAP:   Sensore → Gateway → InfluxDB ← [Node-RED polling ogni 5s] → Dashboard
```

> **Nota avanzata:** con AMQP si potrebbe aggiungere una queue dedicata (`iot.temperature.nodered`) legata allo stesso exchange e usare `node-red-contrib-amqp` per ricevere messaggi in real-time invece di fare polling. Vedi esempio 4.

---

## File nella cartella

```
nodered/
├── Dockerfile             ← Node-RED + plugin pre-installati
├── data/
│   ├── flows.json         ← I 3 flussi: Dashboard, Alerting, Chaos Events
│   ├── flows_cred.json    ← Token InfluxDB
│   └── settings.js        ← Configurazione Node-RED
└── README.md              ← Questo file
```

---

## `Dockerfile`

```dockerfile
FROM nodered/node-red:latest

USER root
RUN npm install --prefix /usr/src/node-red \
    node-red-contrib-influxdb \
    node-red-dashboard \
    node-red-node-email

USER node-red
```

---

## `data/flows.json` — I 3 flussi

### Tab 1: Dashboard (polling InfluxDB ogni 5s)

```
inject (ogni 5s)
    │
    ▼
influxdb-in (Flux query: ultimi 5 minuti)
    │
    ▼
function "Formatta per grafico"
    │  msg.topic = sensor_id
    │  msg.payload = float (numero puro!)
    │  msg.timestamp = ms epoch
    ▼
ui_chart (lineare, 10 serie colorate)
```

**Regola critica `ui_chart` con `node-red-dashboard@3.x`:**
```javascript
// ✅ FUNZIONA
node.send({ topic: "sensor-03", payload: 22.4, timestamp: 1748000000000 });

// ❌ NON FUNZIONA
node.send({ topic: "sensor-03", payload: { x: 1748000000000, y: 22.4 } });
```

### Tab 2: Alerting (ogni 30s)

```
inject (ogni 30s) → influxdb-in (ultimo valore 35s) → soglie → email Mailhog
```

Soglie: `> 30°C` → alert ALTA, `< 16°C` → alert BASSA.

### Tab 3: Chaos Events (HTTP)

```
http in (POST /chaos/events) → formatta email → [email + debug + http 200]
```

Riceve JSON dal `chaos.sh` via curl. La email spiega cosa osservare per ogni tipo di evento AMQP.

---

## Interfacce web

| URL | Credenziali | Descrizione |
|---|---|---|
| http://localhost:1880/ui | — | Dashboard |
| http://localhost:1880 | — | Editor Node-RED |
| http://localhost:15672 | iot / iot-password | RabbitMQ Management |
| http://localhost:8086 | admin / adminpassword | InfluxDB |
| http://localhost:5025 | — | Mailhog |

---

## Esempi pratici — Come modificare Node-RED

### 1. Cambiare l'intervallo di polling

Editor Node-RED → tab Dashboard → nodo `inject` → **Repeat** → cambia `5` a `2`.

O modifica direttamente `flows.json`:
```json
{"id": "amqp-inject-poll", "repeat": "2"}
```

```bash
docker cp nodered/data/flows.json node-red:/data/flows.json
docker restart node-red
```

---

### 2. Cambiare le soglie di alerting

Editor → tab Alerting → doppio click su **"Controlla soglie"**:
```javascript
var SOGLIA_ALTA  = 30;   // °C
var SOGLIA_BASSA = 16;   // °C
```
Deploy (pulsante rosso in alto a destra).

---

### 3. Aggiungere dati storici (30 minuti, aggregati)

Aggiungi un secondo flusso nel tab Dashboard:
```
inject (ogni 60s)
    │
    ▼
influxdb-in
    query: from(bucket: "iot")
           |> range(start: -30m)
           |> filter(fn: (r) => r._measurement == "temperatura" and r._field == "value")
           |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
    │
    ▼
function (stessa logica formato) → ui_chart-storico
```

---

### 4. Ricevere messaggi AMQP in real-time (senza polling)

Questa è la modalità "push native" di AMQP — aggiornamenti in < 1s invece di ogni 5s.

**Nel `Dockerfile`:**
```dockerfile
RUN npm install --prefix /usr/src/node-red \
    node-red-contrib-influxdb \
    node-red-dashboard \
    node-red-node-email \
    node-red-contrib-amqp          ← aggiunto
```

**In `rabbitmq/definitions.json`**, aggiungi queue dedicata:
```json
{
  "name": "iot.temperature.nodered",
  "vhost": "/",
  "durable": false,
  "auto_delete": true,
  "arguments": {}
}
```

Con binding `sensor.#.temperatura` → `iot.temperature.nodered`.

In Node-RED, aggiungi `amqp-consumer` → function → `ui_chart`.

```bash
docker compose build nodered --no-cache
docker compose up -d --force-recreate nodered rabbitmq
```

---

### 5. Widget counter: messaggi in queue RabbitMQ

```
inject (ogni 10s)
    │
    ▼
http request
    method: GET
    url: http://rabbitmq:15672/api/queues/%2F/iot.temperature
    auth: basic (iot / iot-password)
    │
    ▼
function "Estrai stats"
    │  msg.payload = risultato.messages_ready
    ▼
ui_text (label: "Queue iot.temperature", units: "msg")
```

Particolarmente spettacolare durante `drop_gateway`: si vede il contatore salire.

---

### 6. Notifiche Slack invece di email

Nella function **"Controlla soglie"**:
```javascript
msg.url     = "https://hooks.slack.com/services/T000/B000/XXX";
msg.payload = JSON.stringify({ text: "*[IoT AMQP]* " + alerts.join("\n") });
msg.headers = {"Content-Type": "application/json"};
return msg;
```

Collega a un nodo `http request` (POST) invece di `e-mail`.

---

### 7. Applicare flows.json senza riavviare

```bash
docker cp nodered/data/flows.json node-red:/data/flows.json
docker restart node-red
```

Oppure usa l'editor visuale — più sicuro perché valida il JSON prima del deploy.

---

### 8. Aggiungere gauge per ogni sensore

1. Editor → tab Dashboard
2. Trascina un nodo **ui_gauge** dalla palette
3. Collegalo all'uscita della function "Formatta per grafico"
4. Configura: Group = "Temperature Sensori", min=10, max=40
5. Deploy

---

## Troubleshooting

### Il grafico non mostra dati

1. InfluxDB ha dati? → http://localhost:8086 → Data Explorer → bucket `iot`
2. Debug panel Node-RED (icona 🐛): guarda l'output della function
3. Il config node `influxdb-cfg-amqp` è verde?
4. `docker compose restart node-red`

### "401 Unauthorized" da InfluxDB

Verifica `flows_cred.json`:
```json
{"influxdb-cfg-amqp": {"token": "my-super-token"}}
```

L'ID `influxdb-cfg-amqp` deve corrispondere esattamente all'ID nel `flows.json`.

### Email non arrivano in Mailhog

Verifica nodo `e-mail`:
- **Server**: `mailhog` (container name, non `localhost`)
- **Port**: `1025` (non `8025`)
- **Auth**: None

### Chaos Events non ricevuti

Test manuale:
```bash
docker exec network-chaos curl -s -X POST \
  -H "Content-Type: application/json" \
  -d '{"event_type":"test","container":"test","phase":"DROP","duration":5,"timestamp":0,"event_count":0}' \
  http://node-red:1880/chaos/events
```
