#!/bin/bash
set -eo pipefail

# Usage: ./bench_v2.sh [binary] [mode]
#   binary: path to dragonfly binary (default: ./build-opt/dragonfly)
#   mode:   throughput | fragmentation | all (default: all)
#
# Modes:
#   throughput     - 50 clients, 2KB SET, heavy saturation.
#                    Tests peak RPS and confirms V2 doesn't regress under load.
#   fragmentation  - 1 client, 2KB SET, max_busy_read_usec=50000.
#                    Exposes V2's per-fragment flush vs V1's busy-read batching.
#                    Mirrors test_reply_count conditions.
#   all            - Run both modes sequentially.

DFLY_BIN=${1:-"./build-opt/dragonfly"}
MODE=${2:-"all"}
PORT=6379
GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "nogit")
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="bench_v2_${MODE}_${GIT_SHA}_${TIMESTAMP}.log"

# Number of CPU cores available; pin dragonfly to first 2, memtier to next 2 if possible.
NUM_CPUS=$(nproc)
DFLY_CPUS="0,1"
MEMTIER_CPUS="2,3"
if [[ $NUM_CPUS -le 2 ]]; then
    DFLY_TASKSET=""
    MEMTIER_TASKSET=""
else
    DFLY_TASKSET="taskset -c ${DFLY_CPUS}"
    MEMTIER_TASKSET="taskset -c ${MEMTIER_CPUS}"
fi

wait_for_server() {
    local timeout=${1:-10}
    local elapsed=0
    while ! redis-cli -p $PORT ping > /dev/null 2>&1; do
        sleep 0.2
        elapsed=$(( elapsed + 1 ))
        if [[ $elapsed -ge $(( timeout * 5 )) ]]; then
            echo "[!] Error: Dragonfly did not become ready within ${timeout}s."
            return 1
        fi
    done
}

get_send_count() {
    local raw
    raw=$(curl -s "http://127.0.0.1:${PORT}/metrics" 2>/dev/null) || true
    local val
    val=$(echo "$raw" | grep '^dragonfly_reply_total' | awk '{sum += $2} END {print (sum ? sum : 0)}') || true
    echo "${val:-0}"
}

# run_bench <v2_flag> <label> <threads> <clients> <data_size> <pipelines...> <extra_dfly_flags...>
run_bench() {
    local v2_flag=$1; shift
    local label=$1; shift
    local threads=$1; shift
    local clients=$1; shift
    local data_size=$1; shift
    local mode_name=$1; shift
    # Remaining positional args: pipeline sizes
    local pipelines=()
    while [[ $# -gt 0 && "$1" != "--" ]]; do
        pipelines+=("$1"); shift
    done
    [[ "${1:-}" == "--" ]] && shift
    # Remaining: extra dragonfly flags
    local extra_flags=("$@")

    echo ""
    echo ">>> Starting Dragonfly [${label}] mode=${mode_name} (io_loop_v2=${v2_flag})"
    $DFLY_TASKSET "$DFLY_BIN" \
        --proactor_threads=2 \
        --experimental_io_loop_v2="${v2_flag}" \
        --port=$PORT \
        "${extra_flags[@]}" \
        > /dev/null 2>&1 &
    DFLY_PID=$!

    if ! wait_for_server 10; then
        kill "$DFLY_PID" 2>/dev/null || true
        exit 1
    fi

    redis-cli -p $PORT PING > /dev/null

    if ! command -v memtier_benchmark > /dev/null 2>&1; then
        echo "[!] memtier_benchmark not found in PATH."
        kill "$DFLY_PID" 2>/dev/null || true
        exit 1
    fi

    RESULTS_TMP=$(mktemp)
    echo -e "PIPELINE\tRPS\tAVG_LAT(ms)\tSEND_SYSCALLS" > "$RESULTS_TMP"

    for PIPELINE in "${pipelines[@]}"; do
        echo "  [+] pipeline=$PIPELINE ..."

        SENDS_BEFORE=$(get_send_count)

        OUTPUT=$($MEMTIER_TASKSET memtier_benchmark \
            -s 127.0.0.1 \
            -p $PORT \
            -t "$threads" \
            -c "$clients" \
            --pipeline="$PIPELINE" \
            --ratio=1:0 \
            -d "$data_size" \
            --key-pattern=R:R \
            --key-prefix=bench \
            -n 1000 \
            --hide-histogram 2>&1) || true

        SENDS_AFTER=$(get_send_count)
        SEND_DELTA=$(( SENDS_AFTER - SENDS_BEFORE ))

        RPS=$(echo "$OUTPUT"     | grep "^Totals" | awk '{print $2}')
        LATENCY=$(echo "$OUTPUT" | grep "^Totals" | awk '{print $5}')
        RPS=${RPS:-"Error"}
        LATENCY=${LATENCY:-"Error"}

        echo -e "$PIPELINE\t$RPS\t$LATENCY\t$SEND_DELTA" >> "$RESULTS_TMP"
    done

    kill "$DFLY_PID" 2>/dev/null || true
    wait "$DFLY_PID" 2>/dev/null || true

    echo ""
    echo "====================================================="
    printf "  %-4s  %s  (commit: %s)\n" "$label" "$mode_name" "$GIT_SHA"
    echo "====================================================="
    column -t -s $'\t' "$RESULTS_TMP"
    echo "====================================================="
    rm "$RESULTS_TMP"
}

# ---------- MODE: throughput ----------
# 50 concurrent clients saturate the server.  Both V1 and V2 batch well.
# Purpose: regression-guard for peak RPS; confirms V2 doesn't lose throughput.
run_throughput() {
    run_bench false "V1" 2 25 2048 "throughput" 1 10 100 500
    run_bench true  "V2" 2 25 2048 "throughput" 1 10 100 500
}

# ---------- MODE: fragmentation ----------
# 1 client, large payloads, max_busy_read_usec=50000.
# Mirrors test_reply_count: V1's read fiber spin-accumulates fragments for 50ms
# while V2 flushes after each partial read.
# Purpose: expose V2's per-fragment unconditional flush penalty.
run_fragmentation() {
    run_bench false "V1" 1 1 2048 "fragmentation" 1 10 100 500 -- --max_busy_read_usec=50000
    run_bench true  "V2" 1 1 2048 "fragmentation" 1 10 100 500 -- --max_busy_read_usec=50000
}

# ---------- Main ----------
case "$MODE" in
    throughput)
        { run_throughput; } 2>&1 | tee "$LOG_FILE"
        ;;
    fragmentation)
        { run_fragmentation; } 2>&1 | tee "$LOG_FILE"
        ;;
    all)
        { run_throughput; run_fragmentation; } 2>&1 | tee "$LOG_FILE"
        ;;
    *)
        echo "Unknown mode: $MODE"
        echo "Usage: $0 [binary] [throughput|fragmentation|all]"
        exit 1
        ;;
esac

echo ""
echo "Full log saved to: $LOG_FILE"
