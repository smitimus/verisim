#!/bin/bash
# Manages Verisim industry-stack dev/test/release modes.
# Usage: ./switch.sh [dev|test|release|rebuild|status] [grocery|gas-station]
#
# dev     — Build from source, start self-contained dev stack
# test    — Build standalone image locally, run as single container
# release — Start production stack (pulls from Docker Hub)
# rebuild — Build the standalone image and restart the test stack (rebuild-and-restart workflow)
# status  — Show which mode is currently running (per industry)

set -e

VERISIM_DIR="$(cd "$(dirname "$0")" && pwd)"
INDUSTRY="${2:-grocery}"
STACKS_DIR="/opt/data-lab"

case "$INDUSTRY" in
  grocery)
    IND_DIR="$VERISIM_DIR/grocery"
    IMAGE=verisim-grocery
    DB_ENV_NAME=VERISIM_GROCERY_DB
    DB_ENV_DEFAULT=grocery
    POSTGRES_PORT_VAR=VERISIM_POSTGRES_PORT
    API_PORT_VAR=VERISIM_API_PORT
    UI_PORT_VAR=VERISIM_UI_PORT
    POSTGRES_PORT_DEFAULT=5499
    API_PORT_DEFAULT=8010
    UI_PORT_DEFAULT=8501
    RELEASE_STACK="$STACKS_DIR/verisim-grocery/compose.yaml"
    ;;
  gas-station)
    IND_DIR="$VERISIM_DIR/gas-station"
    IMAGE=verisim-gas-station
    DB_ENV_NAME=VERISIM_GAS_DB
    DB_ENV_DEFAULT=gas_station
    POSTGRES_PORT_VAR=VERISIM_GAS_POSTGRES_PORT
    API_PORT_VAR=VERISIM_GAS_API_PORT
    UI_PORT_VAR=VERISIM_GAS_UI_PORT
    POSTGRES_PORT_DEFAULT=5500
    API_PORT_DEFAULT=8011
    UI_PORT_DEFAULT=8502
    RELEASE_STACK=""   # no Docker Hub release stack yet for gas-station
    ;;
  *)
    echo "Unknown industry: $INDUSTRY (expected: grocery|gas-station)"
    exit 1
    ;;
esac

# Load env vars for IP/port display
source "$VERISIM_DIR/grocery/.env" 2>/dev/null || true

_stop_all() {
  echo "  Stopping $INDUSTRY dev stack..."
  docker compose -f "$IND_DIR/compose.yaml" down 2>/dev/null || true
  echo "  Stopping $INDUSTRY test stack..."
  docker compose -f "$IND_DIR/compose.test.yaml" down 2>/dev/null || true
  if [ -n "$RELEASE_STACK" ]; then
    echo "  Stopping $INDUSTRY release stack..."
    docker compose -f "$RELEASE_STACK" down 2>/dev/null || true
  fi
}

_env_prefix() {
  # Emit the env assignments shared by all modes for this industry.
  echo "VERISIM_POSTGRES_USER=${VERISIM_POSTGRES_USER:-verisim}"
  echo "VERISIM_POSTGRES_PASSWORD=${VERISIM_POSTGRES_PASSWORD:-verisim}"
  echo "$DB_ENV_NAME=${!DB_ENV_NAME:-$DB_ENV_DEFAULT}"
  echo "$POSTGRES_PORT_VAR=${!POSTGRES_PORT_VAR:-$POSTGRES_PORT_DEFAULT}"
  echo "$API_PORT_VAR=${!API_PORT_VAR:-$API_PORT_DEFAULT}"
  echo "$UI_PORT_VAR=${!UI_PORT_VAR:-$UI_PORT_DEFAULT}"
  echo "TZ=${TZ:-America/New_York}"
  echo "CONF=${CONF:-/opt/conf}"
  echo "IP=${IP:-localhost}"
  echo "HOMEPAGE_GROUP=${HOMEPAGE_GROUP:-Verisim}"
}

_run_compose() {
  # $1 = compose file, rest = extra args
  local compose_file="$1"; shift
  env $(_env_prefix) docker compose -f "$compose_file" "$@"
}

_urls() {
  local pg_port="${!POSTGRES_PORT_VAR:-$POSTGRES_PORT_DEFAULT}"
  local api_port="${!API_PORT_VAR:-$API_PORT_DEFAULT}"
  local ui_port="${!UI_PORT_VAR:-$UI_PORT_DEFAULT}"
  echo "  UI:  http://${IP:-localhost}:${ui_port}"
  echo "  API: http://${IP:-localhost}:${api_port}/docs"
  echo "  PG:  ${IP:-localhost}:${pg_port}"
}

case "$1" in

  dev)
    echo ""
    echo "=== Verisim $INDUSTRY: switching to dev mode ==="
    _stop_all
    echo "  Building and starting dev stack..."
    _run_compose "$IND_DIR/compose.yaml" up -d --build
    echo ""
    echo "Dev stack is up."
    _urls
    echo ""
    ;;

  test)
    echo ""
    echo "=== Verisim $INDUSTRY: building local standalone image ==="
    docker build \
      --platform linux/amd64 \
      -t "$IMAGE:local" \
      -f "$IND_DIR/standalone/Dockerfile" \
      "$VERISIM_DIR"
    echo ""
    echo "=== Verisim $INDUSTRY: switching to test mode ==="
    _stop_all
    _run_compose "$IND_DIR/compose.test.yaml" up -d
    echo ""
    echo "Test stack is up (local standalone image)."
    _urls
    echo ""
    ;;

  release)
    if [ -z "$RELEASE_STACK" ]; then
      echo "release mode is not available for $INDUSTRY yet (no Docker Hub deploy stack)."
      exit 1
    fi
    echo ""
    echo "=== Verisim $INDUSTRY: switching to release mode ==="
    _stop_all
    _run_compose "$RELEASE_STACK" up -d
    echo ""
    echo "Release stack is up (Docker Hub image)."
    _urls
    echo ""
    ;;

  rebuild)
    echo ""
    echo "=== Verisim $INDUSTRY: rebuild standalone image + restart test stack ==="
    _stop_all
    echo "  Building standalone image ($IMAGE:local)..."
    docker build --platform linux/amd64 \
      -t "$IMAGE:local" \
      -f "$IND_DIR/standalone/Dockerfile" \
      "$VERISIM_DIR"
    echo "  Starting test stack..."
    _run_compose "$IND_DIR/compose.test.yaml" up -d
    echo ""
    echo "Test stack is up (local standalone image)."
    _urls
    echo ""
    ;;

  status)
    echo ""
    echo "=== Verisim mode status ==="
    RUNNING_MODE=""
    if docker ps --format '{{.Names}}' | grep -q "verisim-grocery-dev"; then
      echo "  grocery: dev (multi-container from source)"
      RUNNING_MODE="dev"
    elif docker ps --format '{{.Names}}' | grep -q 'verisim-grocery-test'; then
      echo "  grocery: test (local standalone image)"
      RUNNING_MODE="test"
    elif docker ps --format '{{.Names}}' | grep -q '^verisim-grocery$'; then
      echo "  grocery: release (Docker Hub image)"
      RUNNING_MODE="release"
    else
      echo "  grocery: none"
    fi
    if docker ps --format '{{.Names}}' | grep -q 'verisim-gas-station-test'; then
      echo "  gas-station: test (local standalone image)"
      RUNNING_MODE="test"
    elif docker ps --format '{{.Names}}' | grep -q '^verisim-gas-station$'; then
      echo "  gas-station: dev/legacy stack running"
    else
      echo "  gas-station: none"
    fi
    _urls
    echo ""
    ;;

  *)
    echo ""
    echo "Usage: $0 [dev|test|release|rebuild|status] [grocery|gas-station]"
    echo ""
    echo "  dev     Build from source, start self-contained dev stack"
    echo "  test    Build standalone image locally, run as single container"
    echo "  release Start production stack (pulls from Docker Hub; grocery only)"
    echo "  rebuild Rebuild standalone image + restart test stack (rebuild-and-restart)"
    echo "  status  Show which mode is currently running"
    echo ""
    echo "  Industry defaults to grocery."
    echo ""
    ;;

esac
