#!/bin/bash
# =============================================================================
# Build and push Verisim standalone images to Docker Hub.
# Run from the verisim/ directory (or anywhere — script is self-relocating).
#
# Usage:
#   bash build-and-push.sh                    # builds grocery (default), tags as latest
#   bash build-and-push.sh grocery 1.0.0      # grocery, versioned
#   bash build-and-push.sh gas-station        # gas station, latest
#   bash build-and-push.sh gas-station 1.0.0  # gas station, versioned
# =============================================================================
set -e

INDUSTRY=${1:-grocery}
VERSION=${2:-latest}

case "$INDUSTRY" in
  grocery)
    IMAGE=smiti/verisim-grocery
    DOCKERFILE=grocery/standalone/Dockerfile
    ;;
  gas-station)
    IMAGE=smiti/verisim-gas-station
    DOCKERFILE=gas-station/standalone/Dockerfile
    ;;
  *)
    echo "Unknown industry: $INDUSTRY"
    echo "Usage: bash build-and-push.sh [grocery|gas-station] [version]"
    exit 1
    ;;
esac

# Build context is verisim/ — both base/ and industry dirs must be accessible
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "Industry:      $INDUSTRY"
echo "Build context: $(pwd)"
echo "Image:         ${IMAGE}:${VERSION}"
echo ""

docker build \
  --platform linux/amd64 \
  --progress=plain \
  -t "${IMAGE}:${VERSION}" \
  -f "$DOCKERFILE" \
  .

# Smoke-test the built image by running a temporary container and hitting /health
echo "Running smoke test..."
CONTAINER_ID=$(docker run -d --rm "${IMAGE}:${VERSION}" 2>&1) || { echo "Smoke test: FAIL (container start failed)"; exit 1; }
# Wait for /health - up to ~120s (first-run backfill)
HEALTHY=false
for i in $(seq 1 24); do
  sleep 5
  HEALTH=$(docker run --rm --network container:"$CONTAINER_ID" appropriate/curl curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health 2>/dev/null || echo "000")
  if [ "$HEALTH" = "200" ]; then
    HEALTHY=true
    break
  fi
done
docker rm -f "$CONTAINER_ID" >/dev/null 2>&1 || true
if [ "$HEALTHY" = true ]; then
  echo "Smoke test: PASS"
else
  echo "Smoke test: FAIL (health check did not return 200)"
  exit 1
fi

if [ "$VERSION" != "latest" ]; then
  docker tag "${IMAGE}:${VERSION}" "${IMAGE}:latest"
  echo "Tagged ${IMAGE}:${VERSION} → ${IMAGE}:latest"
fi

echo ""
read -r -p "Push to Docker Hub? [y/N] " confirm
if [[ "$confirm" =~ ^[Yy]$ ]]; then
  docker push "${IMAGE}:${VERSION}"
  [ "$VERSION" != "latest" ] && docker push "${IMAGE}:latest"
  echo "Pushed ${IMAGE}:${VERSION}"
else
  echo "Skipped push. Image available locally as ${IMAGE}:${VERSION}"
fi
