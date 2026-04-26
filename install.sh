#!/usr/bin/env bash
set -euo pipefail

# =============================================================
# Verisim Grocery — End-User Installer
# What this does:
#   1. Installs Docker Engine if not present
#   2. Pulls smiti/verisim-grocery from Docker Hub
#   3. Starts the container (postgres + api + ui + generator)
#
# Data generator auto-backfills 30 days of grocery data on
# first start, then switches to real-time simulation.
# =============================================================

VERISIM_IMAGE="smiti/verisim-grocery:latest"
CONTAINER_NAME="verisim-grocery"
DB_PORT=5499
API_PORT=8010
UI_PORT=8501

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BOLD='\033[1m'; NC='\033[0m'

banner() {
  echo ""
  echo -e "${BOLD}╔══════════════════════════════════════════╗${NC}"
  echo -e "${BOLD}║          Verisim Grocery Installer       ║${NC}"
  echo -e "${BOLD}╚══════════════════════════════════════════╝${NC}"
  echo ""
  echo "This script will:"
  echo "  • Install Docker Engine (if not already installed)"
  echo "  • Pull smiti/verisim-grocery from Docker Hub"
  echo "  • Start the Verisim Grocery container"
  echo ""
}

install_docker() {
  echo -e "${YELLOW}Installing Docker Engine...${NC}"
  apt-get update -qq
  apt-get install -y -qq ca-certificates curl gnupg lsb-release
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL "https://download.docker.com/linux/${ID:-debian}/gpg" \
    | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  chmod a+r /etc/apt/keyrings/docker.gpg
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
    https://download.docker.com/linux/${ID:-debian} \
    $(lsb_release -cs) stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -qq
  apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-compose-plugin
  systemctl enable docker --now
  usermod -aG docker "${SUDO_USER:-$USER}" || true
  echo -e "${GREEN}Docker installed.${NC}"
}

main() {
  [[ -t 0 ]] || { [[ -c /dev/tty ]] && exec </dev/tty; }

  banner

  echo -e "${BOLD}Press ENTER to continue or Ctrl+C to cancel...${NC}"
  read -r

  # OS check
  if [[ -f /etc/os-release ]]; then
    . /etc/os-release
    if [[ "$ID" != "debian" && "$ID" != "ubuntu" ]]; then
      echo -e "${YELLOW}Warning: This installer targets Debian/Ubuntu. Detected: $ID${NC}"
      echo "Continue anyway? (y/N)"
      read -r ans
      [[ "$ans" =~ ^[Yy]$ ]] || exit 1
    fi
  fi

  # Docker install
  if ! command -v docker &>/dev/null; then
    if [[ "$EUID" -ne 0 ]]; then
      echo -e "${RED}Docker not found. Please run this script with sudo to install Docker.${NC}"
      exit 1
    fi
    install_docker
  else
    echo -e "${GREEN}Docker found: $(docker --version)${NC}"
  fi

  # Remove existing container if stopped
  if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    STATUS=$(docker inspect -f '{{.State.Status}}' "$CONTAINER_NAME")
    if [[ "$STATUS" == "running" ]]; then
      echo -e "${GREEN}Verisim is already running. Services:${NC}"
    else
      echo "Removing stopped container..."
      docker rm "$CONTAINER_NAME"
    fi
  fi

  # Pull and run
  if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "${YELLOW}Pulling image...${NC}"
    docker pull "$VERISIM_IMAGE"
    echo -e "${YELLOW}Starting container...${NC}"
    docker run -d \
      --name "$CONTAINER_NAME" \
      --restart unless-stopped \
      -p "${DB_PORT}:5432" \
      -p "${API_PORT}:8000" \
      -p "${UI_PORT}:8501" \
      "$VERISIM_IMAGE"
  fi

  # Get server IP
  IP=$(ip route get 1.1.1.1 2>/dev/null | awk '{print $7; exit}') || IP="localhost"

  echo ""
  echo -e "${GREEN}${BOLD}Verisim Grocery is running!${NC}"
  echo ""
  echo "  Streamlit UI  → http://${IP}:${UI_PORT}"
  echo "  API docs      → http://${IP}:${API_PORT}/docs"
  echo "  PostgreSQL    → ${IP}:${DB_PORT}  (user: verisim / pass: verisim / db: grocery)"
  echo ""
  echo "The generator is backfilling 30 days of data — this takes a few minutes."
  echo "Watch progress in the Streamlit UI → Generator Control tab."
  echo ""
  echo "To stop:   docker stop $CONTAINER_NAME"
  echo "To remove: docker rm $CONTAINER_NAME"
}

main "$@"
