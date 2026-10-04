#!/bin/bash
# =============================================================================
# Build and push Verisim standalone images to Docker Hub.
# Run from the verisim/ directory (or anywhere — script is self-relocating).
#
# The version tag comes from the VERSION file at the repo root unless you name
# one explicitly, and it must be MAJOR.MINOR.PATCH — a commit SHA is refused
# (t_4b05829c). `latest` is always retagged alongside it.
#
# Usage:
#   bash build-and-push.sh                    # grocery, version from VERSION
#   bash build-and-push.sh grocery 1.3.4      # grocery, that version
#   bash build-and-push.sh gas-station        # gas station, version from VERSION
#   bash build-and-push.sh gas-station 1.3.4  # gas station, that version
# =============================================================================
set -e

INDUSTRY=${1:-grocery}
VERSION=${2:-}

# A hand-run push is a publish too, so it obeys the same convention as CI: the
# tag is the tracked VERSION file, never a commit SHA (t_4b05829c). Reading it
# here means a hand push and a CI push of the same commit name the same version.
#
# An explicit argument still wins, but it must BE a version — `bash
# build-and-push.sh grocery $(git rev-parse HEAD)` is exactly how an unreadable
# tag gets back in, so it is refused rather than honoured.
if [ -z "${VERSION}" ]; then
  VERSION="$(tr -d '[:space:]' < "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/VERSION")"
  echo "Version:       ${VERSION} (from VERSION)"
elif ! echo "${VERSION}" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$'; then
  echo "Refusing to push '${VERSION}': a published tag must be the tracked version"
  echo "(MAJOR.MINOR.PATCH, from the VERSION file), not a commit SHA."
  echo ""
  echo "To release a version: bump VERSION and pyproject.toml in one commit, then"
  echo "  bash build-and-push.sh ${INDUSTRY}          # uses VERSION"
  echo "  bash build-and-push.sh ${INDUSTRY} 1.3.4    # or name it explicitly"
  exit 1
fi

case "$INDUSTRY" in
  grocery)
    IMAGE=smiti/verisim-grocery
    DOCKERFILE=grocery/standalone/Dockerfile
    ;;
  gas-station)
    IMAGE=smiti/verisim-gas-station
    DOCKERFILE=gas-station/standalone/Dockerfile
    ;;
  support)
    IMAGE=smiti/verisim-support
    DOCKERFILE=support/standalone/Dockerfile
    ;;
  *)
    echo "Unknown industry: $INDUSTRY"
    echo "Usage: bash build-and-push.sh [grocery|gas-station|support] [version]"
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

# `latest` always moves with the release. The old form guarded this with
# `[ "$VERSION" != "latest" ]`, which is now always true — VERSION is always a
# version number — and under `set -e` that guard was also a trap: a caller who
# passed `latest` explicitly made the `&&` chain the last command in the branch
# and exited non-zero on success. Unconditional is both correct and simpler.
docker tag "${IMAGE}:${VERSION}" "${IMAGE}:latest"
echo "Tagged ${IMAGE}:${VERSION} → ${IMAGE}:latest"

echo ""
read -r -p "Push to Docker Hub? [y/N] " confirm
if [[ "$confirm" =~ ^[Yy]$ ]]; then
  docker push "${IMAGE}:${VERSION}"
  docker push "${IMAGE}:latest"
  echo "Pushed ${IMAGE}:${VERSION} and ${IMAGE}:latest"
else
  echo "Skipped push. Image available locally as ${IMAGE}:${VERSION}"
fi
