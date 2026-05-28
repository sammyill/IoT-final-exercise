#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Chaos Monkey — IoT-final
# ─────────────────────────────────────────────────────────────────────────────
# Ferma un container scelto a caso fra sensori e gateway, attende
# DROP_DURATION, poi lo riavvia. Esercita: buffer locali sui sensori,
# riconnessione automatica dei gateway, recupero dello storage.
#
# Usa `docker stop` / `docker start` (più semplice di network disconnect:
# nessuna risoluzione di nome di rete, stato osservabile in `docker ps`).
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

CHAOS_ENABLED="${CHAOS_ENABLED:-true}"
DROP_INTERVAL="${DROP_INTERVAL:-180}"
DROP_DURATION="${DROP_DURATION:-10}"

# Container target. I nomi devono corrispondere ai container_name in compose.
FRIDGE_SENSORS=(
    "sensor01" "sensor02" "sensor03" "sensor04" "sensor05"
    "sensor06" "sensor07" "sensor08" "sensor09" "sensor10"
)
FACTORY_SENSORS=(
    "factory01" "factory02" "factory03" "factory04" "factory05"
    "factory06" "factory07" "factory08" "factory09" "factory10"
)
GATEWAYS=("factory-gateway" "mqtt-gateway")
DATASTORE=("influxdb")

log() { echo "[CHAOS] $(date '+%Y-%m-%d %H:%M:%S') $*"; }

stop_then_start() {
    local container="$1"
    local label="$2"

    log "════════════════════════════════════════"
    log "EVENTO ${label}: STOP ${container}"

    if docker stop "$container" >/dev/null 2>&1; then
        log "  ✗ ${container} fermato (durata ${DROP_DURATION}s)"
    else
        log "  [WARN] stop di ${container} fallito — container non in esecuzione?"
        return
    fi

    sleep "$DROP_DURATION"

    if docker start "$container" >/dev/null 2>&1; then
        log "  ✓ ${container} ripartito"
    else
        log "  [WARN] start di ${container} fallito"
    fi
    log "════════════════════════════════════════"
}

pick_random() {
    local -n arr="$1"
    echo "${arr[$(( RANDOM % ${#arr[@]} ))]}"
}

drop_fridge_sensor() {
    local c; c="$(pick_random FRIDGE_SENSORS)"
    stop_then_start "$c" "sensor-fridge"
}

drop_factory_sensor() {
    local c; c="$(pick_random FACTORY_SENSORS)"
    stop_then_start "$c" "sensor-factory"
}

drop_gateway() {
    local c; c="$(pick_random GATEWAYS)"
    stop_then_start "$c" "gateway"
}

drop_influxdb() {
    stop_then_start "${DATASTORE[0]}" "influxdb"
}

log "Chaos Monkey avviato"
log "  CHAOS_ENABLED=$CHAOS_ENABLED | DROP_INTERVAL=${DROP_INTERVAL}s | DROP_DURATION=${DROP_DURATION}s"

if [ "$CHAOS_ENABLED" != "true" ]; then
    log "Chaos DISABILITATO. Container in standby."
    while true; do sleep 3600; done
fi

log "Attesa iniziale 45s (lascia partire l'infrastruttura)..."
sleep 45

event_count=0
while true; do
    wait_time=$(( DROP_INTERVAL / 2 + RANDOM % DROP_INTERVAL ))
    event_count=$(( event_count + 1 ))
    log "Prossimo evento #${event_count} fra ${wait_time}s"
    sleep "$wait_time"

    # Distribuzione:
    #   0..3 → fridge sensor (40%)
    #   4..6 → factory sensor (30%)
    #   7..8 → gateway (20%)
    #   9    → influxdb (10%)
    event_type=$(( RANDOM % 10 ))
    case "$event_type" in
        0|1|2|3) drop_fridge_sensor   ;;
        4|5|6)   drop_factory_sensor  ;;
        7|8)     drop_gateway         ;;
        9)       drop_influxdb        ;;
    esac
done
