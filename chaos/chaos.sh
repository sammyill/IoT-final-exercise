#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Chaos Monkey — versione AMQP
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

CHAOS_ENABLED="${CHAOS_ENABLED:-true}"
DROP_INTERVAL="${DROP_INTERVAL:-90}"
DROP_DURATION="${DROP_DURATION:-20}"
SENSOR_NET="${SENSOR_NET:-amqp_stack_amqp_sensor_net}"
CORE_NET="${CORE_NET:-amqp_stack_amqp_core_net}"

SENSORS=(
    "sensor-01" "sensor-02" "sensor-03" "sensor-04" "sensor-05"
    "sensor-06" "sensor-07" "sensor-08" "sensor-09" "sensor-10"
)
NUM_SENSORS=${#SENSORS[@]}
event_count=0

log() { echo "[CHAOS] $(date '+%Y-%m-%d %H:%M:%S') $*"; }

publish_chaos_event() {
    local event_type="$1" container="$2" phase="$3"
    local payload
    payload=$(printf \
        '{"event_type":"%s","container":"%s","phase":"%s","duration":%s,"timestamp":%s,"event_count":%s}' \
        "$event_type" "$container" "$phase" \
        "$DROP_DURATION" "$(date +%s)" "$event_count")
    curl -s -X POST -H "Content-Type: application/json" \
        -d "$payload" "http://node-red:1880/chaos/events" 2>/dev/null \
        && log "  [HTTP] Notifica $phase inviata a Node-RED" \
        || log "  [HTTP] Node-RED non raggiungibile (chaos continua)"
}

drop_single_sensor() {
    local idx=$(( RANDOM % NUM_SENSORS ))
    local sensor="${SENSORS[$idx]}"
    log "════════════════════════════════════════"
    log "EVENTO #${event_count}: sensor_single — ${sensor}"
    log "  Effetto: il sensore perde la connessione AMQP, bufferizza in RAM"
    publish_chaos_event "sensor_single" "$sensor" "DROP"
    docker network disconnect "$SENSOR_NET" "$sensor" 2>/dev/null \
        && log "  ✗ ${sensor} disconnesso" || log "  [WARN] Disconnect fallito"
    sleep "$DROP_DURATION"
    docker network connect "$SENSOR_NET" "$sensor" 2>/dev/null \
        && log "  ✓ ${sensor} riconnesso" || log "  [WARN] Reconnect fallito"
    publish_chaos_event "sensor_single" "$sensor" "RESTORE"
    log "  Osserva: flush buffer [BUFFER] nei log del sensore"
    log "════════════════════════════════════════"
}

drop_multi_sensor() {
    local idx1=$(( RANDOM % NUM_SENSORS ))
    local idx2=$(( (idx1 + 1 + RANDOM % (NUM_SENSORS-1)) % NUM_SENSORS ))
    local s1="${SENSORS[$idx1]}" s2="${SENSORS[$idx2]}"
    log "════════════════════════════════════════"
    log "EVENTO #${event_count}: sensor_multi — ${s1} e ${s2}"
    publish_chaos_event "sensor_multi" "${s1}+${s2}" "DROP"
    docker network disconnect "$SENSOR_NET" "$s1" 2>/dev/null && log "  ✗ ${s1} disconnesso"
    docker network disconnect "$SENSOR_NET" "$s2" 2>/dev/null && log "  ✗ ${s2} disconnesso"
    sleep "$DROP_DURATION"
    docker network connect "$SENSOR_NET" "$s1" 2>/dev/null && log "  ✓ ${s1} riconnesso"
    docker network connect "$SENSOR_NET" "$s2" 2>/dev/null && log "  ✓ ${s2} riconnesso"
    publish_chaos_event "sensor_multi" "${s1}+${s2}" "RESTORE"
    log "════════════════════════════════════════"
}

drop_rabbitmq() {
    log "════════════════════════════════════════"
    log "EVENTO #${event_count}: rabbitmq — broker down (evento AMQP più drammatico)"
    log "  Effetto: TUTTI i sensori perdono la connessione AMQP → buffer locale"
    publish_chaos_event "rabbitmq" "rabbitmq" "DROP"
    docker network disconnect "$SENSOR_NET" "rabbitmq" 2>/dev/null \
        && log "  ✗ rabbitmq disconnesso da ${SENSOR_NET}" || log "  [WARN] Disconnect fallito"
    log "  Osserva: tutti i sensori loggano [OFFLINE] entro ~60s"
    sleep "$DROP_DURATION"
    docker network connect "$SENSOR_NET" "rabbitmq" 2>/dev/null \
        && log "  ✓ rabbitmq riconnesso" || log "  [WARN] Reconnect fallito"
    publish_chaos_event "rabbitmq" "rabbitmq" "RESTORE"
    log "  Osserva: burst da 10 sensori simultaneamente"
    log "════════════════════════════════════════"
}

drop_gateway() {
    log "════════════════════════════════════════"
    log "EVENTO #${event_count}: gateway — amqp-gateway down"
    log "  Effetto AMQP esclusivo: i sensori continuano a pubblicare"
    log "  I messaggi si ACCUMULANO nella queue durable di RabbitMQ"
    log "  → Vai su http://localhost:15672 → Queues → iot.temperature"
    publish_chaos_event "gateway" "amqp-gateway" "DROP"
    docker network disconnect "$CORE_NET" "amqp-gateway" 2>/dev/null \
        && log "  ✗ amqp-gateway disconnesso da ${CORE_NET}" || log "  [WARN] Disconnect fallito"
    local msg_attesi=$(( DROP_DURATION / 5 * 10 ))
    log "  Dopo ${DROP_DURATION}s, queue avrà ~${msg_attesi} messaggi in attesa"
    sleep "$DROP_DURATION"
    docker network connect "$CORE_NET" "amqp-gateway" 2>/dev/null \
        && log "  ✓ amqp-gateway riconnesso" || log "  [WARN] Reconnect fallito"
    publish_chaos_event "gateway" "amqp-gateway" "RESTORE"
    log "  Osserva: il gateway drena la queue → burst batch write su InfluxDB"
    log "════════════════════════════════════════"
}

drop_nodered() {
    log "════════════════════════════════════════"
    log "EVENTO #${event_count}: nodered — dashboard offline"
    publish_chaos_event "nodered" "node-red" "DROP"
    docker network disconnect "$CORE_NET" "node-red" 2>/dev/null \
        && log "  ✗ node-red disconnesso" || log "  [WARN] Disconnect fallito"
    sleep "$DROP_DURATION"
    docker network connect "$CORE_NET" "node-red" 2>/dev/null \
        && log "  ✓ node-red riconnesso" || log "  [WARN] Reconnect fallito"
    publish_chaos_event "nodered" "node-red" "RESTORE"
    log "════════════════════════════════════════"
}

log "Chaos Monkey AMQP avviato"
log "  CHAOS_ENABLED=$CHAOS_ENABLED | DROP_INTERVAL=${DROP_INTERVAL}s | DROP_DURATION=${DROP_DURATION}s"

if [ "$CHAOS_ENABLED" != "true" ]; then
    log "Chaos DISABILITATO. Container in standby."
    while true; do sleep 3600; done
fi

log "Attesa iniziale 45s..."
sleep 45

while true; do
    wait_time=$(( DROP_INTERVAL / 2 + RANDOM % DROP_INTERVAL ))
    event_count=$(( event_count + 1 ))
    log "Prossimo evento #${event_count} tra ${wait_time}s"
    sleep "$wait_time"

    # 0,1,2→sensor_single(30%) | 3,4→multi(20%) | 5,6→rabbitmq(20%) | 7,8→gateway(20%) | 9→nodered(10%)
    event_type=$(( RANDOM % 10 ))
    case "$event_type" in
        0|1|2) drop_single_sensor ;;
        3|4)   drop_multi_sensor  ;;
        5|6)   drop_rabbitmq      ;;
        7|8)   drop_gateway       ;;
        9)     drop_nodered       ;;
    esac
done
