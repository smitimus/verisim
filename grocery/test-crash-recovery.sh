#!/usr/bin/env bash
set -euo pipefail

# Crash/Recovery test runner for Verisim grocery (T11)
# - Executes 3 crash/recovery cycles using the local stand-alone grocery image
# - Follows protocol from test-cycles-final-report.md without modifying compose or entrypoints

REPO_DIR="/opt/verisim/grocery"
DOCKER_YAML="compose.test.yaml"
CONTAINER_NAME="verisim-grocery-test"
LOG_FILE="/opt/verisim/grocery/test-crash-recovery.log"
SUMMARY_CSV="/opt/verisim/grocery/test-crash-recovery.csv"

if [[ ! -d "$REPO_DIR" ]]; then
  echo "ERROR: expected repo at $REPO_DIR" >&2
  exit 1
fi
if [[ ! -f "$REPO_DIR/test-cycles-final-report.md" ]]; then
  echo "ERROR: missing $REPO_DIR/test-cycles-final-report.md" >&2
  exit 2
fi
if [[ ! -f "$REPO_DIR/compose.test.yaml" ]]; then
  echo "ERROR: missing $REPO_DIR/compose.test.yaml" >&2
  exit 3
fi
if [[ ! -f "$REPO_DIR/api/main.py" ]]; then
  echo "ERROR: missing $REPO_DIR/api/main.py" >&2
  exit 4
fi

cd "$REPO_DIR"

echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC%z")] Crash/recovery test started" | tee -a "$LOG_FILE"

# Clean up any previous runs that collide with this run
echo "[info] Cleaning up any existing test containers..." | tee -a "$LOG_FILE"
docker compose -f "$DOCKER_YAML" down >/dev/null 2>&1 || true

echo "cycle,cycle_start,cycle_end,prev_ticks,curr_ticks,result" > "$SUMMARY_CSV"

wait_for_health() {
  local timeout=${1:-120}
  local port=${2:-8010}
  local t=0
  while (( t < timeout )); do
    if curl -sSf http://localhost:"$port"/health >/dev/null; then
      return 0
    fi
    sleep 1
    t=$((t + 1))
  done
  return 1
}

get_status_ticks() {
  local port=${1:-8010}
  local status_json
  status_json=$(curl -s http://localhost:"$port"/grocery/generator/status || true)
  if [[ -z "$status_json" ]]; then
    echo "0 0"  # mode and ticks; 0 indicates not ready
    return 0
  fi
  local mode
  mode=$(python3 - <<'PY'
import json,sys
try:
  j=json.loads(sys.stdin.read())
  m=j.get('state',{}).get('mode','')
  t=j.get('today',{}).get('ticks_today',0)
  print(m, t)
except Exception:
  print('','0')
PY
 <<< "$status_json")
  local mode_ticks=($mode)
  echo "${mode_ticks[0]} ${mode_ticks[1]}"  # mode ticks_today
}

get_tick_count() {
  local port=${1:-8010}
  local status_json
  status_json=$(curl -s http://localhost:"$port"/grocery/generator/status || true)
  if [[ -n "$status_json" ]]; then
    python3 - <<'PY'
import json,sys
try:
  j=json.loads(sys.stdin.read())
  t=j.get('today',{}).get('ticks_today',0)
  print(t)
except Exception:
  print(0)
PY
    <<< "$status_json"
  else
    echo 0
  fi
}

get_transactions_count() {
  # Returns an integer count of transactions
  local port=${1:-8010}
  curl -s http://localhost:"$port"/grocery/transactions | python3 -c "import sys,json; data=json.load(sys.stdin); print(len(data))"
}

SECONDS_SINCE_START=0
start_time=""
end_time=""
prev_ticks=0
cycle=0

for cycle in 1 2 3; do
  echo "[cycle $cycle] Beginning cycle..." | tee -a "$LOG_FILE"
  start_time=$(date -u +"%Y-%m-%d %H:%M:%S UTC")
  if [[ "$cycle" -eq 1 ]]; then
    # Ensure clean start
    docker compose -f "$DOCKER_YAML" down >/dev/null 2>&1 || true
  fi

  # Start container if not already up (cycle 1; cycles 2-3 skip if already running)
  if ! docker compose -f "$DOCKER_YAML" ps "$CONTAINER_NAME" | grep -q Up; then
    docker compose -f "$DOCKER_YAML" up -d
  else
    echo "[info] Container already up; skipping restart for cycle $cycle" | tee -a "$LOG_FILE"
  fi

  # Wait for health (up to 120s)
  if ! wait_for_health 120; then
    echo "[error] Health check failed for cycle $cycle" | tee -a "$LOG_FILE"; exit 10
  fi

  # Wait for data generation to start (mode=realtime with ticks_today > 0)
  ready=0
  attempts=0
  while (( attempts < 120 )); do
    read mode ticks <<<"$(get_status_ticks)"
    if [[ "$mode" == "realtime" && "$ticks" -gt 0 ]]; then
      ready=1
      break
    fi
    sleep 1
    attempts=$((attempts + 1))
  done
  if [[ "$ready" -ne 1 ]]; then
    echo "[error] Data generation did not reach realtime/ticks>0 for cycle $cycle" | tee -a "$LOG_FILE"; exit 11
  fi

  # Fetch sample transaction count before crash
  prev_ticks=$(get_tick_count)
  echo "[cycle $cycle] Prev tick count: $prev_ticks" | tee -a "$LOG_FILE"

  # Simulate crash: kill container
  docker compose -f "$DOCKER_YAML" kill

  # Downtime
  sleep 60

  # Restart
  docker compose -f "$DOCKER_YAML" up -d

  # Wait for health after restart
  if ! wait_for_health 120; then
    echo "[error] Health after restart failed for cycle $cycle" | tee -a "$LOG_FILE"; exit 12
  fi

  # Verify: ensure transaction count did not regress
  end_ticks=$(get_tick_count)
  if [[ -z "$end_ticks" ]]; then end_ticks=0; fi
  end_time=$(date -u +"%Y-%m-%d %H:%M:%S UTC")
  result="PASS"
  if (( end_ticks < prev_ticks )); then
    result="FAIL"
  fi
  echo "$cycle,$start_time,$end_time,$prev_ticks,$end_ticks,$result" >> "$SUMMARY_CSV"
  echo "[cycle $cycle] End tick count: $end_ticks, Result: $result" | tee -a "$LOG_FILE"

  if [[ "$result" != "PASS" ]]; then
    echo "[diagnostic] Cycle $cycle failed: regression detected" | tee -a "$LOG_FILE"; exit 13
  fi
done

echo "[done] All 3 cycles completed. Summary:" | tee -a "$LOG_FILE"
column -t -s ',' "$SUMMARY_CSV" | sed 's/^[^,]*/Cycle/;1s/.*/Cycle,Start,End,PrevTicks,CurrTicks,Result/'
exit 0
