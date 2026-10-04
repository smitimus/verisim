#!/bin/bash
# =============================================================================
# Compute the Docker tags a publish pushes — ONE place, so the convention
# cannot drift between the grocery and gas-station workflows (t_4b05829c).
#
# Prints `key=value` lines on stdout. In CI the caller appends them to
# $GITHUB_OUTPUT; locally a test can read stdout and assert on them, which is
# the only reason the output is not written straight into $GITHUB_OUTPUT here
# (a script that writes to the file cannot be executed by a test at all).
#
# Usage:
#   REPO_ROOT=<checkout> IMAGE_NAME=smiti/verisim-grocery \
#   GITHUB_REF=refs/heads/main GITHUB_REF_NAME=main GITHUB_SHA=abc123 \
#   bash .github/scripts/compute-push-tags.sh
#
# Inputs (all required except the git ones, which default to the real repo):
#   REPO_ROOT        checkout root; defaults to the repo this script lives in
#   IMAGE_NAME       full image path, e.g. smiti/verisim-grocery
#   GITHUB_REF       refs/heads/main | refs/tags/v<version>
#   GITHUB_REF_NAME  main | v<version>
#   GITHUB_SHA       the commit being published
#
# Output:
#   tags=<comma-separated refs for docker/build-push-action>
#   version=<the tracked version, e.g. 1.3.4>
#   release=true|false  (true when the push was a v* tag push)
#   dev_tag=dev-<YYYY-MM-DD-HHMM> UTC
#   version_ref=<image>:<version>
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="${REPO_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"

die() {
  # GitHub renders ::error:: as a red annotation, so this is loud in the UI and
  # not just in the log scrollback.
  echo "::error title=Bad publish tag::${1}" >&2
  echo "${1}" >&2
  exit 1
}

# ── 1. The tracked version ──────────────────────────────────────────────────
# This is the fix for the card: the version is a file in the repo, so it is
# reviewable in the diff and cannot be a 40-character SHA. Before this, every
# main push published :${GITHUB_SHA} and Docker Hub filled up with opaque tags
# (18 of 28 tags on smiti/verisim-grocery were raw SHAs on 2026-10-04).
VERSION_FILE="${REPO_ROOT}/VERSION"
[ -f "${VERSION_FILE}" ] || die \
  "No VERSION file at ${VERSION_FILE}. The tracked version is the source of truth for the published tag — add one (plain text, e.g. 1.3.4) and commit it."

VERSION="$(tr -d '[:space:]' < "${VERSION_FILE}")"

# Semver core, optional pre-release/build suffix. Deliberately strict: a typo'd
# version becomes a Docker tag nobody will ever pin again.
echo "${VERSION}" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$' || die \
  "VERSION reads '${VERSION}', which is not a version number. Write it as MAJOR.MINOR.PATCH (e.g. 1.3.4), optionally with a -suffix."

# pyproject carries the same number. Two sources that must agree is still two
# sources, so the disagreement is a hard failure rather than a preference
# (tools/check_publish_tags.py asserts it, so a drift fails the build).
PYPROJECT="${REPO_ROOT}/pyproject.toml"
if [ -f "${PYPROJECT}" ]; then
  PYPROJECT_VERSION="$(sed -n 's/^version *= *"\([^"]*\)".*/\1/p' "${PYPROJECT}" | head -1)"
  if [ "${PYPROJECT_VERSION}" != "${VERSION}" ]; then
    die "VERSION says '${VERSION}' but pyproject.toml says '${PYPROJECT_VERSION}'. They name the same release — update both in one commit."
  fi
fi

# ── 2. What kind of push is this? ───────────────────────────────────────────
: "${IMAGE_NAME:?IMAGE_NAME is required (e.g. smiti/verisim-grocery)}"
: "${GITHUB_REF:?GITHUB_REF is required (e.g. refs/heads/main)}"
: "${GITHUB_SHA:?GITHUB_SHA is required}"
GITHUB_REF_NAME="${GITHUB_REF_NAME:-$(basename "${GITHUB_REF}")}"

RELEASE=false
if [ "${GITHUB_REF}" != "refs/heads/main" ]; then
  # Only a v* tag pushes, so the pushed ref names a release. If it disagrees
  # with the tracked version, one of them is wrong and shipping it would put a
  # version in the registry that the repo does not claim.
  case "${GITHUB_REF}" in
    refs/tags/v*) ;;
    *) die "Refusing to publish '${GITHUB_REF}'. Only refs/heads/main and refs/tags/v<version> are publishable." ;;
  esac
  RELEASE=true
  if [ "${GITHUB_REF_NAME}" != "v${VERSION}" ]; then
    die "Pushing git tag '${GITHUB_REF_NAME}' but VERSION says '${VERSION}'. Either the tag or the VERSION file is wrong; releasing a tag the repo does not claim puts an untracked version in the registry."
  fi
fi

# ── 3. The tags ─────────────────────────────────────────────────────────────
# Always three things:
#   latest                 moving pointer (what `switch.sh release` pulls)
#   <version>              THE tracked version — the human-readable tag this card asks for
#   <sha>                  full traceability back to the commit, and the ref
#                          t_ff5a70ec's dual-registry verifier keys off
# A release tag push adds its own git tag name as well, so the historic
# `v1.3.3` shape keeps working alongside the bare `1.3.3`.
tags="${IMAGE_NAME}:latest,${IMAGE_NAME}:${VERSION},${IMAGE_NAME}:${GITHUB_SHA}"
if [ "${RELEASE}" = true ]; then
  tags="${tags},${IMAGE_NAME}:${GITHUB_REF_NAME}"
fi

# Named for the moment of the push, in UTC, matching the estate's existing
# dev-<YYYY-MM-DD>-<HHMM> convention. Never re-pointed: a minute collision
# overwrites a tag nobody has pinned yet, which is the acceptable case; the
# version tag above is the one that must stay meaningful.
DEV_TAG="dev-$(date -u +%Y-%m-%d-%H%M)"

echo "tags=${tags}"
echo "version=${VERSION}"
echo "release=${RELEASE}"
echo "dev_tag=${DEV_TAG}"
echo "version_ref=${IMAGE_NAME}:${VERSION}"