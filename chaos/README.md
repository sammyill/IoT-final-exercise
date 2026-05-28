# Cartella `chaos/` — Il Chaos Monkey (versione AMQP)

## Cos'è questa cartella?

Qui vive il **Chaos Monkey**: lo strumento che simula guasti di rete in modo controllato.

La versione AMQP introduce due eventi esclusivi rispetto a MQTT e CoAP:

| Evento | MQTT | CoAP | AMQP |
|---|---|---|---|
| Drop sensore | ✓ | ✓ | ✓ |
| Drop sensori multipli | ✓ | ✓ | ✓ |
| Drop broker | Mosquitto | — | **RabbitMQ** |
| Drop consumer | Telegraf | coap-gw | **amqp-gateway** (msgs in queue!) |
| Drop Node-RED | ✓ | ✓ | ✓ |

**Novità AMQP:**
- `drop_rabbitmq`: tutti i sensori perdono il broker → buffer locale in RAM
- `drop_gateway`: messaggi si accumulano nella **queue durable di RabbitMQ** — nessun dato si perde, visibile dalla Management UI

---

## File nella cartella

```
chaos/
├── chaos.sh       ← Script bash con la logica
├── Dockerfile     ← docker:cli + bash + curl
└── README.md      ← Questo file
```

---

## `chaos.sh` — Come funziona

### Il loop principale

```
Attesa 45s (stack si stabilizza)
└─ Loop infinito:
      ├─ Attesa casuale (~45–135s)
      ├─ Sceglie evento (0–9):
      │     0,1,2 (30%) → sensor_single
      │     3,4   (20%) → sensor_multi
      │     5,6   (20%) → rabbitmq     ← broker down
      │     7,8   (20%) → gateway      ← msgs in queue
      │     9     (10%) → nodered
      ├─ curl POST → node-red:1880/chaos/events (DROP)
      ├─ docker network disconnect
      ├─ sleep DROP_DURATION
      ├─ docker network connect
      └─ curl POST → node-red:1880/chaos/events (RESTORE)
```

### L'evento più didattico: `drop_gateway`

```
Sensori → RabbitMQ → [amqp-gateway OFFLINE] → InfluxDB

Durante il drop:
  I sensori continuano a pubblicare normalmente
  I messaggi si accumulano nella queue durable
  → http://localhost:15672 → Queues → iot.temperature → "Ready" sale

Al ripristino:
  Il gateway drena la queue → burst di batch write su InfluxDB
  → "Ready" scende a 0 in pochi secondi
```

Questo è impossibile in CoAP (no broker, no queue) e mostra il vantaggio di AMQP.

---

## Configurazione tramite variabili d'ambiente

| Variabile | Default | Descrizione |
|---|---|---|
| `CHAOS_ENABLED` | `true` | `false` = standby (nessun evento) |
| `DROP_INTERVAL` | `90` | Secondi medi tra eventi |
| `DROP_DURATION` | `20` | Durata della disconnessione |
| `SENSOR_NET` | `amqp_stack_amqp_sensor_net` | Rete sensori |
| `CORE_NET` | `amqp_stack_amqp_core_net` | Rete servizi |

---

## Come leggere i log

```bash
docker compose logs -f chaos
```

Esempio `drop_gateway`:
```
[CHAOS] Prossimo evento #5 tra 112s
[CHAOS] ════════════════════════════════════════
[CHAOS] EVENTO #5: gateway — amqp-gateway down
[CHAOS]   Effetto AMQP esclusivo: i sensori continuano a pubblicare
[CHAOS]   I messaggi si ACCUMULANO nella queue durable
[CHAOS]   → Vai su http://localhost:15672 → Queues → iot.temperature
[CHAOS]   [HTTP] Notifica DROP inviata a Node-RED
[CHAOS]   ✗ amqp-gateway disconnesso da amqp_core_net
[CHAOS]   Dopo 20s, queue avrà ~40 messaggi in attesa
[CHAOS]   ✓ amqp-gateway riconnesso
[CHAOS]   Osserva: il gateway drena la queue → burst batch write
[CHAOS] ════════════════════════════════════════
```

---

## Esempi pratici — Come modificare il Chaos Monkey

### 1. Disabilitare il chaos

Nel `.env`:
```env
CHAOS_ENABLED=false
```
```bash
docker compose up -d --force-recreate chaos
```

---

### 2. Simulare manualmente `drop_gateway`

```bash
docker network disconnect amqp_stack_amqp_core_net amqp-gateway

# Script per monitorare la queue in tempo reale:
while true; do
    r=$(curl -s -u iot:iot-password \
        http://localhost:15672/api/queues/%2F/iot.temperature)
    ready=$(echo "$r" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('messages_ready',0))")
    echo "[$(date '+%H:%M:%S')] ready=$ready"
    sleep 2
done

# Ripristina dopo 30s
docker network connect amqp_stack_amqp_core_net amqp-gateway
```

---

### 3. Simulare manualmente `drop_rabbitmq`

```bash
# Tutti i sensori perdono il broker
docker network disconnect amqp_stack_amqp_sensor_net rabbitmq

# Vedi i sensori loggare [OFFLINE]
docker compose logs sensor03 | tail -5

# Ripristina → burst da 10 sensori
docker network connect amqp_stack_amqp_sensor_net rabbitmq
docker compose logs -f amqp-gateway | head -20
```

---

### 4. Demo intensiva (eventi frequenti)

```env
CHAOS_DROP_INTERVAL=20    # evento ogni ~10–30s
CHAOS_DROP_DURATION=5     # disconnessione 5s
```
```bash
docker compose up -d --force-recreate chaos
```

---

### 5. Aggiungere evento "InfluxDB down"

In `chaos.sh`:
```bash
drop_influxdb() {
    log "EVENTO #${event_count}: influxdb down"
    publish_chaos_event "influxdb" "influxdb" "DROP"
    docker network disconnect "$CORE_NET" "influxdb" 2>/dev/null && log "  ✗ influxdb disconnesso"
    log "  Effetto: il gateway accumula in pending[], fa nack+requeue"
    log "  RabbitMQ: messaggi restano Unacked nella queue"
    sleep "$DROP_DURATION"
    docker network connect "$CORE_NET" "influxdb" 2>/dev/null && log "  ✓ influxdb riconnesso"
    publish_chaos_event "influxdb" "influxdb" "RESTORE"
}
```

Nel `case`:
```bash
# Cambia RANDOM % 10 in RANDOM % 11 e aggiungi:
10) drop_influxdb ;;
```

---

### 6. Escludere sensori specifici

In `chaos.sh`:
```bash
SENSORS=(
    # "sensor-01"   ← commentato: mai toccato
    "sensor-02" "sensor-03" "sensor-04" "sensor-05"
    "sensor-06" "sensor-07" "sensor-08" "sensor-09" "sensor-10"
)
```

---

### 7. Chaos solo in orari specifici

```bash
while true; do
    ora=$(date +%H)
    if [ "$ora" -lt 9 ] || [ "$ora" -ge 17 ]; then
        log "Fuori orario (${ora}:xx). Pausa 5 min."
        sleep 300
        continue
    fi
    event_count=$(( event_count + 1 ))
    # ... resto del loop
done
```

---

### 8. Ricostruire dopo modifiche a `chaos.sh`

```bash
docker compose build chaos --no-cache
docker compose up -d --force-recreate chaos
```

---

## Differenza rispetto alle versioni MQTT e CoAP

### Drop del consumer (gateway/Telegraf)

**MQTT (drop Telegraf):**
- Mosquitto accumula i messaggi nelle sue queue interne
- Al ripristino: Telegraf legge da dove aveva lasciato

**CoAP (drop coap-gateway):**
- Tutti i sensori vedono timeout CON
- Bufferizzano in RAM (deque locale, max 500 msg)
- Possibile perdita dati se il buffer si riempie

**AMQP (drop amqp-gateway):**
- I sensori non si accorgono del drop del consumer
- I messaggi si accumulano nella **queue durable di RabbitMQ**
- Nessun dato si perde (fino a `x-max-length: 10000`)
- Al ripristino: burst ordinato (FIFO)

### Notifiche eventi

Identico a CoAP: `curl` HTTP POST a Node-RED (non `mosquitto_pub` come in MQTT).

---

## Domande frequenti

**Q: Se il chaos container si riavvia durante un DROP?**
A: Il container rimane disconnesso (nessuno lo riconnette). Ripristina manualmente:
```bash
docker network connect amqp_stack_amqp_sensor_net rabbitmq
docker network connect amqp_stack_amqp_core_net amqp-gateway
```

**Q: Con DROP_DURATION=60 e drop_gateway, quanti messaggi si accumulano?**
A: `60 / 5 × 10 = 120 messaggi`. Al ripristino: `120 / 20 = 6 batch` scritti su InfluxDB in pochi secondi.

**Q: È sicuro dare accesso al Docker socket al chaos container?**
A: In un lab locale è accettabile. In produzione usa strumenti come **LitmusChaos** o **Chaos Toolkit** per ambienti Kubernetes.
