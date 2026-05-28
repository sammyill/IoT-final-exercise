# Cartella `mosquitto/` — Il Broker MQTT

## Cos'è questa cartella?

Qui si trova la configurazione di **Eclipse Mosquitto**, il broker MQTT del progetto.

Un **broker MQTT** è come un ufficio postale intelligente:
- I **sensori** imbucano lettere (messaggi) specificando l'indirizzo (topic)
- Il **broker** le smista e le recapita a chi si è iscritto (subscriber)
- Se un destinatario è assente, le **tiene in coda** finché non torna online

Mosquitto è il broker MQTT open-source più diffuso al mondo, usato in produzione da milioni di dispositivi IoT.

---

## File nella cartella

```
mosquitto/
├── mosquitto.conf     ← La configurazione completa del broker
├── data/              ← Persistenza su disco (messaggi in coda, sessioni)
│   └── .gitkeep       ← File vuoto che mantiene la cartella in git
├── log/               ← Log del broker
│   └── .gitkeep
└── README.md          ← Questo file
```

> **Nota:** Le cartelle `data/` e `log/` sono montate come volumi bind nel container. I file in `data/` sopravvivono anche a `docker compose down -v`.

---

## `mosquitto.conf` — Spiegazione riga per riga

### Listener (porta di ascolto)

```conf
listener 1883
```

Il broker ascolta sulla porta standard MQTT (**1883**).  
In produzione si usa anche la **8883** con TLS/SSL (connessione cifrata).  
Per questo laboratorio usiamo la porta non cifrata — OK in una rete locale isolata.

### Accesso anonimo

```conf
allow_anonymous true
```

Chiunque può connettersi senza username/password.  
**NON farlo mai in produzione.** Qui è accettabile perché i container sono in una rete Docker privata, inaccessibile dall'esterno.

### Persistenza dei messaggi

```conf
persistence true
persistence_location /mosquitto/data/
autosave_interval 300
autosave_on_changes true
```

Con la persistenza attiva, Mosquitto salva su disco:
- Le **sottoscrizioni** dei client con sessione persistente (`clean_session=false`)
- I **messaggi QoS 1/2** in coda per client offline
- I messaggi **Retained**

Senza questo, un riavvio del broker cancellerebbe tutto e i sensori perderebbero i dati non ancora consegnati.

`autosave_interval 300` → salva ogni 5 minuti anche se non ci sono cambiamenti  
`autosave_on_changes true` → salva anche immediatamente quando qualcosa cambia

### Coda per client offline

```conf
max_queued_messages 1000
queue_qos0_messages false
persistent_client_expiration 24h
```

- `max_queued_messages 1000`: Mosquitto tiene in coda fino a 1000 messaggi per ogni subscriber offline. Con 10 sensori a 1 msg/5s → 2 msg/s → ~8 minuti di buffer per subscriber.
- `queue_qos0_messages false`: i messaggi QoS=0 ("fire and forget") non vengono accodati per design — chi non c'è li perde.
- `persistent_client_expiration 24h`: se un client non si riconnette entro 24 ore, il broker dimentica la sua sessione e cancella i messaggi in coda.

### Logging

```conf
log_dest file /mosquitto/log/mosquitto.log
log_dest stdout
log_type warning
log_type error
log_type information
```

I log vanno sia sul file che su stdout (visibile con `docker compose logs`).

---

## Come leggere i log di Mosquitto

```bash
# Log in tempo reale
docker compose logs -f mosquitto

# Solo errori e warning
docker compose logs mosquitto | grep -E "WARNING|ERROR"

# Quante connessioni attive
docker compose logs mosquitto | grep "New connection"
```

**Log tipici durante l'esecuzione:**

```
1748000000: New connection from 172.23.0.5 on port 1883.
1748000001: New client connected from 172.23.0.5 as sensor03 (p2, c0, k10).
1748000050: Client sensor03 disconnected.
1748000065: Client sensor03 reconnected.
```

Decodifica di `(p2, c0, k10)`:
- `p2` = protocollo MQTT versione 3.1.1
- `c0` = `clean_session=false` (sessione persistente)
- `k10` = keepalive 10 secondi

---

## Esempi pratici — Come modificare Mosquitto

### 1. Aumentare i messaggi in coda (più buffer per disconnessioni lunghe)

Scenario: vuoi che Mosquitto tenga i messaggi per un'ora invece di 8 minuti.

```conf
# mosquitto.conf
max_queued_messages 5000   # era 1000
```

Calcolo: 10 sensori × 1 msg/5s × 3600s = 7200 msg. Con 5000 sopporti circa 40 minuti.

Riavvia Mosquitto:
```bash
docker compose restart mosquitto
```

---

### 2. Attivare l'autenticazione con username e password

In produzione, ogni client dovrebbe autenticarsi.

**Passo 1** — Crea il file delle password:
```bash
# Entra nel container e crea il file
docker exec -it mqtt-broker sh
mosquitto_passwd -c /mosquitto/config/passwd telegraf
mosquitto_passwd /mosquitto/config/passwd nodered
mosquitto_passwd /mosquitto/config/passwd sensor
exit
```

**Passo 2** — Modifica `mosquitto.conf`:
```conf
allow_anonymous false                              # ← era true
password_file /mosquitto/config/passwd
```

**Passo 3** — Aggiorna i client. In `telegraf.conf`:
```toml
[[inputs.mqtt_consumer]]
  username = "telegraf"
  password = "la-tua-password"
```

In `sensor.py`:
```python
client.username_pw_set("sensor", "la-tua-password")
# Metti questo PRIMA di client.connect()
```

---

### 3. Aggiungere la porta sicura TLS (HTTPS per MQTT)

```conf
# Mantieni la porta standard per uso interno Docker
listener 1883

# Aggiungi la porta sicura per connessioni esterne
listener 8883
cafile   /mosquitto/config/certs/ca.crt
certfile /mosquitto/config/certs/server.crt
keyfile  /mosquitto/config/certs/server.key
tls_version tlsv1.2
```

> Questo richiede di generare certificati SSL (argomento avanzato).

---

### 4. Abilitare il log di debug (per vedere ogni messaggio)

Utile durante lo sviluppo per vedere **ogni** messaggio che passa per il broker.

```conf
log_type debug        # ← aggiunge questo
log_type warning
log_type error
log_type information
```

⚠️ Attenzione: con 10 sensori a 5s, generano ~120 log/minuto. Usa solo per debug breve.

```bash
# Riavvia e segui i log
docker compose restart mosquitto
docker compose logs -f mosquitto
```

---

### 5. Cambiare la porta MQTT

Se la porta 1883 è già occupata sul tuo computer:

```conf
# mosquitto.conf
listener 1884    # ← nuova porta
```

Aggiorna anche `docker-compose.yml`:
```yaml
mosquitto:
  ports:
    - "1884:1884"   # ← era "1883:1883"
```

E tutte le configurazioni client (`telegraf.conf`, variabili sensori, Node-RED).

---

### 6. Ridurre il tempo di scadenza sessione (risparmio memoria)

Per un laboratorio che dura solo 2 ore:

```conf
persistent_client_expiration 2h    # era 24h
```

Se un sensore non si riconnette entro 2 ore, il broker libera la memoria.

---

### 7. Ispezionare i topic attivi in tempo reale

Puoi "spiare" tutti i messaggi che passano per il broker con `mosquitto_sub`:

```bash
# Iscriviti a TUTTI i topic (wildcard #)
docker exec -it mqtt-broker mosquitto_sub -h localhost -t "#" -v

# Solo i dati dei sensori
docker exec -it mqtt-broker mosquitto_sub -h localhost -t "iot/aula/+/temperatura" -v

# Solo il sensore03
docker exec -it mqtt-broker mosquitto_sub -h localhost -t "iot/aula/sensor03/temperatura" -v

# Solo gli eventi chaos
docker exec -it mqtt-broker mosquitto_sub -h localhost -t "iot/chaos/events" -v
```

Output tipico:
```
iot/aula/sensor03/temperatura {"sensor_id":"sensor03","temperatura":22.4,"timestamp":1748000000,"seq":147}
iot/aula/sensor07/temperatura {"sensor_id":"sensor07","temperatura":18.9,"timestamp":1748000001,"seq":203}
```

---

### 8. Pubblicare un messaggio manuale (test)

Puoi simulare un sensore dall'esterno per testare Node-RED o Telegraf:

```bash
docker exec -it mqtt-broker mosquitto_pub \
  -h localhost \
  -t "iot/aula/sensor99/temperatura" \
  -m '{"sensor_id":"sensor99","temperatura":99.9,"timestamp":1748000000,"seq":1}' \
  -q 1
```

Controlla se arriva in Node-RED e in InfluxDB!

---

## Domande frequenti

**Q: Cosa succede se cancello la cartella `data/`?**  
A: Mosquitto perde tutta la memoria persistente: code dei messaggi, sessioni dei client, messaggi retained. I client si riconnetteranno come se fosse la prima volta. Utile per un "reset pulito" del laboratorio:
```bash
docker compose down
rm -rf ./mosquitto/data/*
docker compose up -d
```

**Q: Quanta memoria usa Mosquitto?**  
A: Con 10 sensori e 1000 messaggi in coda per subscriber, circa 5-10 MB di RAM. Mosquitto è famoso per essere estremamente leggero — gira anche su microcontrollori con 256 KB di RAM.

**Q: Posso usare Mosquitto con più di 1000 client?**  
A: Sì. Mosquitto scala facilmente a decine di migliaia di client concorrenti su hardware modesto. Il limite di questo laboratorio è il tuo computer, non Mosquitto.
