#!/bin/bash

###############################################################################
# SAFE Experiment Script for CSE 511 PA2 – CSV Output, Robust Server Handling
# Uses HIGH PORTS (15000+) to avoid conflicts on shared CSE machines
###############################################################################

set -u  # error on undefined vars

SERVER_BIN_ABD=./abd/abd_server
SERVER_BIN_BLOCKING=./blocking/blocking_server
CLIENT_BIN=./workload/workload

SERVER_HOST=127.0.0.1

OPS_PER_CLIENT=2000
NUM_KEYS=10

CLIENT_SWEEP=(1 2 4 8 12 16 20 24 32)
WORKLOADS=("0.9" "0.1")

timestamp=$(date +"%Y%m%d_%H%M%S")
RESULT_DIR=results/$timestamp
mkdir -p "$RESULT_DIR"

CSV_FILE=$RESULT_DIR/results.csv
echo "protocol,N,clients,get_fraction,throughput,get_median,get_p95,put_median,put_p95,succ_get,succ_put,fail" \
    > "$CSV_FILE"

###############################################################################
# Port assignment (HIGH PORTS to avoid interference)
# N=1 -> 15000
# N=3 -> 15100–15102
# N=5 -> 15200–15204
###############################################################################
port_for() {
    local n=$1
    local idx=$2
    case "$n" in
        1)  echo $((15000 + idx)) ;;
        3)  echo $((15100 + idx)) ;;
        5)  echo $((15200 + idx)) ;;
        *)  echo "INVALID_N" >&2; exit 1 ;;
    esac
}

###############################################################################
# Server Health Check
###############################################################################
wait_for_server() {
    local port=$1
    for _ in {1..20}; do
        nc -z "$SERVER_HOST" "$port" 2>/dev/null && return 0
        sleep 0.2
    done
    echo "ERROR: Server on port $port did not start listening in time." >&2
    return 1
}

###############################################################################
# Launch N servers for a protocol
###############################################################################
launch_servers() {
    local protocol=$1
    local N=$2

    echo "Launching $N $protocol servers…"

    for ((i=0; i<N; i++)); do
        local port
        port=$(port_for "$N" "$i")
        local logf="$RESULT_DIR/${protocol}_N${N}_server${i}.log"

        if [[ "$protocol" == "abd" ]]; then
            "$SERVER_BIN_ABD" "$port" > "$logf" 2>&1 &
        else
            "$SERVER_BIN_BLOCKING" "$port" > "$logf" 2>&1 &
        fi

        sleep 0.2

        if grep -q "Address already in use" "$logf"; then
            echo "Server failed to bind on port $port" >&2
            echo "   Check: $logf"
            exit 1
        fi

        if ! wait_for_server "$port"; then
            echo "Server on port $port failed health check." >&2
            exit 1
        fi
    done

    echo "✓ Servers ready for $protocol N=$N."
}

###############################################################################
# Run workload
###############################################################################
run_workload() {
    local protocol=$1
    local clients=$2
    local get_frac=$3
    local N=$4

    local servers=""
    for ((i=0; i<N; i++)); do
        servers+=" ${SERVER_HOST}:$(port_for "$N" "$i")"
    done

    local logf="$RESULT_DIR/${protocol}_N${N}_C${clients}_GET${get_frac}.log"

    echo "Running workload: protocol=$protocol, N=$N, clients=$clients, GET=$get_frac"

    "$CLIENT_BIN" "$protocol" "$clients" "$OPS_PER_CLIENT" "$get_frac" "$NUM_KEYS" $servers \
        > "$logf" 2>&1

    local throughput succ_get succ_put fail get_med get_p95 put_med put_p95

    throughput=$(grep "Throughput:"    "$logf" | awk '{print $2}')
    succ_get=$(grep "GET success:"     "$logf" | awk '{print $3}')
    succ_put=$(grep "PUT success:"     "$logf" | awk '{print $3}')
    fail=$(grep "FAIL count:"          "$logf" | awk '{print $3}')
    get_med=$(grep "GET median:"       "$logf" | awk '{print $3}')
    get_p95=$(grep "GET p95:"          "$logf" | awk '{print $3}')
    put_med=$(grep "PUT median:"       "$logf" | awk '{print $3}')
    put_p95=$(grep "PUT p95:"          "$logf" | awk '{print $3}')

    echo "$protocol,$N,$clients,$get_frac,$throughput,$get_med,$get_p95,$put_med,$put_p95,$succ_get,$succ_put,$fail" \
        >> "$CSV_FILE"
}

###############################################################################
# MAIN EXPERIMENT LOOP
###############################################################################
for protocol in "abd" "blocking"; do
    echo "===== PROTOCOL: $protocol ====="

    echo "🔧 Cleaning ports & killing old servers before $protocol..."
    pkill -f abd_server       2>/dev/null || true
    pkill -f abd_serve        2>/dev/null || true
    pkill -f blocking_server  2>/dev/null || true

    # Free all high ports we use
    for p in {15000..15010} {15100..15110} {15200..15210}; do
        fuser -k ${p}/tcp 2>/dev/null || true
    done
    sleep 1

    for N in 1 3 5; do
        launch_servers "$protocol" "$N"

        for get_frac in "${WORKLOADS[@]}"; do
            for clients in "${CLIENT_SWEEP[@]}"; do
                run_workload "$protocol" "$clients" "$get_frac" "$N"
            done
        done
    done
done

echo ""
echo "================ EXPERIMENT COMPLETE ================"
echo "CSV results written to: $CSV_FILE"
