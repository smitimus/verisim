#!/bin/bash
# =============================================================================
# Build and push the verisim-grocery standalone image to Docker Hub.
# Run from /opt/stacks/:
#   bash verisim-grocery/build-and-push.sh          # tags as latest
#   bash verisim-grocery/build-and-push.sh 1.0.0    # tags as 1.0.0 + latest
# =============================================================================
set -e

IMAGE=smiti/verisim-grocery
VERSION=${1:-latest}

# Must run from /opt/stacks/ so both verisim-base/ and verisim-grocery/ are
# available in the build context.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "Build context: $(pwd)"
echo "Image:         ${IMAGE}:${VERSION}"
echo ""

docker build \
  --platform linux/amd64 \
  --progress=plain \
  -t "${IMAGE}:${VERSION}" \
  -f verisim-grocery/standalone/Dockerfile \
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
  echo "Skipped push. Image is available locally as ${IMAGE}:${VERSION}"
fi
