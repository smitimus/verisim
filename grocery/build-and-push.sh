#!/bin/bash
# =============================================================================
# Build and push the verisim-grocery standalone image to Docker Hub.
# Run from /opt/stacks/:
#   bash verisim-grocery/build-and-push.sh          # version from the VERSION file
#   bash verisim-grocery/build-and-push.sh 1.3.4    # that version
#
# Legacy sibling of the repo-root build-and-push.sh. Both obey the same rule
# (t_4b05829c): a published tag is the tracked version, never a commit SHA, and
# `latest` always moves with it.
# =============================================================================
set -e

IMAGE=smiti/verisim-grocery
VERSION=${1:-}

# Must run from /opt/stacks/ so both verisim-base/ and verisim-grocery/ are
# available in the build context.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

# The VERSION file ships with the repo; this stack layout puts the repo at
# ../verisim/, but fall back to a sibling copy rather than guessing wrong. A
# missing one is a hard failure — this script must never tag with a SHA again.
VERSION_FILE=""
for candidate in "${SCRIPT_DIR}/../verisim/VERSION" "${SCRIPT_DIR}/../VERSION"; do
  if [ -f "${candidate}" ]; then VERSION_FILE="${candidate}"; break; fi
done

if [ -z "${VERSION}" ]; then
  if [ -z "${VERSION_FILE}" ]; then
    echo "No VERSION file found (looked in ${SCRIPT_DIR}/../verisim/VERSION and"
    echo "${SCRIPT_DIR}/../VERSION), and no version was given. A published tag must"
    echo "be the tracked version — pass one explicitly:"
    echo "  bash verisim-grocery/build-and-push.sh 1.3.4"
    exit 1
  fi
  VERSION="$(tr -d '[:space:]' < "${VERSION_FILE}")"
  echo "Version:       ${VERSION} (from ${VERSION_FILE})"
elif ! echo "${VERSION}" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$'; then
  echo "Refusing to push '${VERSION}': a published tag must be the tracked"
  echo "version (MAJOR.MINOR.PATCH), not a commit SHA."
  exit 1
fi

echo "Build context: $(pwd)"
echo "Image:         ${IMAGE}:${VERSION}"
echo ""

docker build \
  --platform linux/amd64 \
  --progress=plain \
  -t "${IMAGE}:${VERSION}" \
  -f verisim-grocery/standalone/Dockerfile \
  .

# Unconditional, and no `&&` chain as a trailing command — `latest` always moves
# with the release, and under `set -e` the old `[ ... ] && docker push` form made
# a successful push exit non-zero (t_4b05829c).
docker tag "${IMAGE}:${VERSION}" "${IMAGE}:latest"
echo "Tagged ${IMAGE}:${VERSION} → ${IMAGE}:latest"

echo ""
read -r -p "Push to Docker Hub? [y/N] " confirm
if [[ "$confirm" =~ ^[Yy]$ ]]; then
  docker push "${IMAGE}:${VERSION}"
  docker push "${IMAGE}:latest"
  echo "Pushed ${IMAGE}:${VERSION} and ${IMAGE}:latest"
else
  echo "Skipped push. Image is available locally as ${IMAGE}:${VERSION}"
fi
