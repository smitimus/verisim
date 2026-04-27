#!/bin/bash
# Manages Verisim grocery dev/test/release modes.
# Usage: ./switch.sh [dev|test|release|status]
#
# dev     — Build from source, start self-contained grocery dev stack
# test    — Build standalone image locally (verisim-grocery:local), run as single container
# release — Start production stack (pulls from Docker Hub)
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
  echo "  UI:  http://${IP:-localhost}:${VERISIM_UI_PORT:-8501}"
  echo "  API: http://${IP:-localhost}:${VERISIM_API_PORT:-8010}/docs"
  echo "  PG:  ${IP:-localhost}:${VERISIM_POSTGRES_PORT:-5499}"
}

case "$1" in

  dev)
    echo ""
    echo "=== Verisim: switching to dev mode ==="
    _stop_all
    echo "  Building and starting dev stack..."
    docker compose --env-file "$GROCERY_DIR/.env" \
      -f "$GROCERY_DIR/compose.yaml" \
      up -d --build
    echo ""
    echo "Dev stack is up."
    _urls
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
    docker compose --env-file "$GROCERY_DIR/.env" \
      -f "$GROCERY_DIR/compose.test.yaml" \
      up -d
    echo ""
    echo "Test stack is up (local standalone image)."
    _urls
    echo ""
    ;;

  release)
    echo ""
    echo "=== Verisim: switching to release mode ==="
    _stop_all
    docker compose --env-file "$STACKS_DIR/verisim-grocery/.env" \
      -f "$STACKS_DIR/verisim-grocery/compose.yaml" \
      up -d
    echo ""
    echo "Release stack is up (Docker Hub image)."
    _urls
    echo ""
    ;;

  status)
    echo ""
    echo "=== Verisim mode status ==="
    if docker ps --format '{{.Names}}' | grep -q 'verisim-grocery-dev'; then
      echo "  Mode: dev (multi-container from source)"
    elif docker ps --format '{{.Names}}' | grep -q 'verisim-grocery-test'; then
      echo "  Mode: test (local standalone image)"
    elif docker ps --format '{{.Names}}' | grep -q '^verisim-grocery$'; then
      echo "  Mode: release (Docker Hub image)"
    else
      echo "  Mode: none (no Verisim stack running)"
    fi
    _urls
    echo ""
    ;;

  *)
    echo ""
    echo "Usage: $0 [dev|test|release|status]"
    echo ""
    echo "  dev     Build from source, start self-contained grocery dev stack"
    echo "  test    Build standalone image locally, run as single container"
    echo "  release Start production stack (pulls from Docker Hub)"
    echo "  status  Show which mode is currently running"
    echo ""
    ;;

esac
