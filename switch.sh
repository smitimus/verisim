#!/bin/bash
# Manages Verisim grocery dev/test/release modes.
# Usage: ./switch.sh [dev|test|release|status]
#
# dev     — Build from source, start self-contained grocery dev stack
# test    — Build standalone image locally (verisim-grocery:local), run as single container
# release — Start production stack (pulls from Docker Hub)
# rebuild — Build the standalone image and restart the test stack (rebuild-and-restart workflow)
# status  — Show which mode is currently running

set -e

VERISIM_DIR="$(cd "$(dirname "$0")" && pwd)"
GROCERY_DIR="$VERISIM_DIR/grocery"
STACKS_DIR="/opt/data-lab"

# Load env vars for IP/port display
source "$GROCERY_DIR/.env" 2>/dev/null || true

_stop_all() {
  echo "  Stopping dev stack..."
  docker compose -f "$GROCERY_DIR/compose.yaml" down 2>/dev/null || true
  echo "  Stopping test stack..."
  docker compose -f "$GROCERY_DIR/compose.test.yaml" down 2>/dev/null || true
  echo "  Stopping release stack..."
  docker compose -f "$STACKS_DIR/verisim-grocery/compose.yaml" down 2>/dev/null || true
}

_urls() {
  local mode="$1"
  local pg_port ui_port api_port
  case "$mode" in
    # All modes share the canonical ports so data-lab needs no repointing.
    *)  pg_port="${VERISIM_POSTGRES_PORT:-5499}"; api_port="${VERISIM_API_PORT:-8010}"; ui_port="${VERISIM_UI_PORT:-8501}" ;;
  esac
  echo "  UI:  http://${IP:-localhost}:${ui_port}"
  echo "  API: http://${IP:-localhost}:${api_port}/docs"
  echo "  PG:  ${IP:-localhost}:${pg_port}"
}

case "$1" in

  dev)
    echo ""
    echo "=== Verisim: switching to dev mode ==="
    _stop_all
    echo "  Building and starting dev stack (canonical ports 5499/8010/8501)..."
    VERISIM_POSTGRES_USER="${VERISIM_POSTGRES_USER:-verisim}" \
    VERISIM_POSTGRES_PASSWORD="${VERISIM_POSTGRES_PASSWORD:-verisim}" \
    VERISIM_GROCERY_DB="${VERISIM_GROCERY_DB:-grocery}" \
    VERISIM_POSTGRES_PORT="${VERISIM_POSTGRES_PORT:-5499}" \
    VERISIM_API_PORT="${VERISIM_API_PORT:-8010}" \
    VERISIM_UI_PORT="${VERISIM_UI_PORT:-8501}" \
    TZ="${TZ:-America/New_York}" \
    CONF="${CONF:-/config}" \
    IP="${IP:-localhost}" \
    HOMEPAGE_GROUP="${HOMEPAGE_GROUP:-Verisim}" \
    docker compose -f "$GROCERY_DIR/compose.yaml" up -d --build
    echo ""
    echo "Dev stack is up."
    _urls dev
    echo ""
    ;;

  test)
    echo ""
    echo "=== Verisim: building local standalone image ==="
    docker build \
      --platform linux/amd64 \
      -t verisim-grocery:local \
      -f "$GROCERY_DIR/standalone/Dockerfile" \
      "$VERISIM_DIR"
    echo ""
    echo "=== Verisim: switching to test mode ==="
    _stop_all
    VERISIM_POSTGRES_USER="${VERISIM_POSTGRES_USER:-verisim}" \
    VERISIM_POSTGRES_PASSWORD="${VERISIM_POSTGRES_PASSWORD:-verisim}" \
    VERISIM_GROCERY_DB="${VERISIM_GROCERY_DB:-grocery}" \
    VERISIM_POSTGRES_PORT="${VERISIM_POSTGRES_PORT:-5499}" \
    VERISIM_API_PORT="${VERISIM_API_PORT:-8010}" \
    VERISIM_UI_PORT="${VERISIM_UI_PORT:-8501}" \
    TZ="${TZ:-America/New_York}" \
    CONF="${CONF:-/config}" \
    IP="${IP:-localhost}" \
    HOMEPAGE_GROUP="${HOMEPAGE_GROUP:-Verisim}" \
    docker compose -f "$GROCERY_DIR/compose.test.yaml" up -d
    echo ""
    echo "Test stack is up (local standalone image)."
    _urls test
    echo ""
    ;;

  release)
    echo ""
    echo "=== Verisim: switching to release mode ==="
    _stop_all
    VERISIM_POSTGRES_USER="${VERISIM_POSTGRES_USER:-verisim}" \
    VERISIM_POSTGRES_PASSWORD="${VERISIM_POSTGRES_PASSWORD:-verisim}" \
    VERISIM_GROCERY_DB="${VERISIM_GROCERY_DB:-grocery}" \
    VERISIM_POSTGRES_PORT="${VERISIM_POSTGRES_PORT:-5499}" \
    VERISIM_API_PORT="${VERISIM_API_PORT:-8010}" \
    VERISIM_UI_PORT="${VERISIM_UI_PORT:-8501}" \
    TZ="${TZ:-America/New_York}" \
    CONF="${CONF:-/config}" \
    IP="${IP:-localhost}" \
    HOMEPAGE_GROUP="${HOMEPAGE_GROUP:-Verisim}" \
    docker compose -f "$STACKS_DIR/verisim-grocery/compose.yaml" up -d
    echo ""
    echo "Release stack is up (Docker Hub image)."
    _urls release
    echo ""
    ;;

  rebuild)
    echo ""
    echo "=== Verisim: rebuild standalone image + restart test stack ==="
    _stop_all
    echo "  Building standalone image (verisim-grocery:local)..."
    docker build --platform linux/amd64 \
      -t verisim-grocery:local \
      -f "$GROCERY_DIR/standalone/Dockerfile" \
      "$VERISIM_DIR"
    echo "  Starting test stack (canonical ports 5499/8010/8501)..."
    VERISIM_POSTGRES_USER="${VERISIM_POSTGRES_USER:-verisim}" \
    VERISIM_POSTGRES_PASSWORD="${VERISIM_POSTGRES_PASSWORD:-verisim}" \
    VERISIM_GROCERY_DB="${VERISIM_GROCERY_DB:-grocery}" \
    VERISIM_POSTGRES_PORT="${VERISIM_POSTGRES_PORT:-5499}" \
    VERISIM_API_PORT="${VERISIM_API_PORT:-8010}" \
    VERISIM_UI_PORT="${VERISIM_UI_PORT:-8501}" \
    TZ="${TZ:-America/New_York}" \
    CONF="${CONF:-/config}" \
    IP="${IP:-localhost}" \
    HOMEPAGE_GROUP="${HOMEPAGE_GROUP:-Verisim}" \
    docker compose -f "$GROCERY_DIR/compose.test.yaml" up -d
    echo ""
    echo "Test stack is up (local standalone image)."
    _urls test
    echo ""
    ;;

  status)
    echo ""
    echo "=== Verisim mode status ==="
    RUNNING_MODE=""
    if docker ps --format '{{.Names}}' | grep -q 'verisim-grocery-dev'; then
      echo "  Mode: dev (multi-container from source)"
      RUNNING_MODE="dev"
    elif docker ps --format '{{.Names}}' | grep -q 'verisim-grocery-test'; then
      echo "  Mode: test (local standalone image)"
      RUNNING_MODE="test"
    elif docker ps --format '{{.Names}}' | grep -q '^verisim-grocery$'; then
      echo "  Mode: release (Docker Hub image)"
      RUNNING_MODE="release"
    else
      echo "  Mode: none (no Verisim stack running)"
    fi
    _urls "$RUNNING_MODE"
    echo ""
    ;;

  *)
    echo ""
    echo "Usage: $0 [dev|test|release|status]"
    echo ""
    echo "  dev     Build from source, start self-contained grocery dev stack"
    echo "  test    Build standalone image locally, run as single container"
    echo "  release Start production stack (pulls from Docker Hub)"
    echo "  rebuild Rebuild standalone image + restart test stack (rebuild-and-restart)"
    echo "  status  Show which mode is currently running"
    echo ""
    ;;

esac
