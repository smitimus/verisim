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
