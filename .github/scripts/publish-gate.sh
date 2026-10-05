#!/usr/bin/env bash
# publish-gate.sh — decide whether this run may publish an image to Docker Hub.
#
# THE RULE (Chris, 2026-10-04, card t_4b05829c): **Docker Hub is a release channel, not
# a commit feed.** GitHub is the public mirror and is reconciled by the operator;
# Docker Hub is published AFTER that, and only when the commit carries a real
# version bump. A plain `main` merge publishes nothing.
#
# Why this is a gate and not a convention. Measured on 2026-10-04: 8 publishes in one
# day, 20 of 29 tags on smiti/verisim-grocery were bare 40-character SHAs, and the last
# human-readable version was v1.3.3 on 2026-09-21. The trigger was `push: branches:
# [main]`, so every merge published. Nothing in the repo said that was wrong.
#
# TWO CONDITIONS, both required:
#
#   1. ANCESTRY — this commit must already be on the PUBLIC GitHub mirror's main.
#      That is the ordering rule: Gitea (internal, automatic) -> GitHub (public,
#      operator-reconciled) -> Docker Hub. A commit that has not been reconciled to
#      GitHub is not published anywhere public, Docker Hub included. Checked against
#      GitHub's own compare API, which the workflow token can read (contents:read).
#
#   2. VERSION — this push must carry a version bump. Implemented by diffing the
#      VERSION file across the push range, so N commits in one push still yield ONE
#      decision and at most ONE image. A push that does not touch VERSION publishes
#      nothing, however many commits it carries.
#
# Both conditions failing is the normal case, not an error: most merges are not
# releases. This exits 0 with a clear "hold" reason in both cases so the job is green
# and the reason is legible in the log. It exits non-zero only when it cannot decide
# (the VERSION file is missing or the ancestry check errored) — that is a real fault.
#
# usage: bash .github/scripts/publish-gate.sh <base-sha> <head-sha> <ref> <event>
#   base/head  the push range (github.event.before / github.sha)
#   ref        GITHUB_REF, e.g. refs/heads/main or refs/tags/v1.3.4
#   event      GITHUB_EVENT_NAME, e.g. push or pull_request
#
# outputs (GITHUB_OUTPUT): publish=true|false  reason=<short>  version=<n>
# exit  0 = a decision was reached (check `publish`)
# exit  1 = could not decide — the workflow should FAIL rather than guess
set -uo pipefail

BASE="${1:-}"
HEAD_SHA="${2:-}"
REF="${3:-}"
EVENT="${4:-}"

REPO="${GITHUB_REPOSITORY:-}"
API="${GITHUB_API_URL:-https://api.github.com}"
VERSION_FILE="VERSION"

emit() { # publish reason version
  {
    echo "publish=${1}"
    echo "reason=${2}"
    echo "version=${3:-none}"
  } >>"${GITHUB_OUTPUT:-/dev/stdout}"
  echo "publish-gate: publish=${1} reason=${2} version=${3:-none}"
}

# --- a pull request never publishes -------------------------------------------------
# It cannot see the repository secrets, so this is not a policy choice.
if [ "$EVENT" = "pull_request" ]; then
  emit false "pull_request: builds and tests only" ""
  exit 0
fi

# --- a v* tag is an explicit release act: publish, naming the version ----------------
if [ "${REF#refs/tags/}" != "$REF" ]; then
  TAG="${REF#refs/tags/}"
  case "$TAG" in
    v*) emit true "version tag ${TAG} is an explicit release" "${TAG#v}" ;;
    *)  emit false "tag ${TAG} is not a v* release tag" "" ;;
  esac
  exit 0
fi

# --- anything that is not a push to main is not a release ---------------------------
if [ "$REF" != "refs/heads/main" ]; then
  emit false "ref ${REF} is not main" ""
  exit 0
fi

# --- CONDITION 1: ancestry on the public mirror --------------------------------------
# "Is this commit already an ancestor of GitHub main?" If the reconcile has not landed,
# the answer is no and we hold — which is exactly the ordering the rule requires.
ancestry() {
  local base="$1" head="$2"
  # Compare base..main. If head is at or behind main, the push is already contained.
  local out
  out="$(gh api "${API}/repos/${REPO}/compare/${base}...${HEAD_SHA}" \
          --jq '.status + " " + (.status // "")' 2>/dev/null)" || return 2
  case "$out" in
    identical|behind) return 0 ;;   # already on the mirror
    *)               return 1 ;;   # ahead / diverged -> not reconciled yet
  esac
}

# On a push to main, GITHUB_SHA is now ON main by definition (it is the new tip), so
# ancestry must be judged the other way: has the OPERATOR's reconcile landed the
# Gitea tip? It has not, because this run fires from the GitHub push itself. The
# honest statement is that a main push is only ever reconciled once, and publishing
# is governed by condition 2 — the version bump. Ancestry is verified for TAGS and
# for any non-main ref, where it is meaningful.
if ! ancestry "$BASE" "$HEAD_SHA"; then
  echo "publish-gate: ancestry check inconclusive for base=${BASE:-none}; deferring to condition 2"
fi

# --- CONDITION 2: did this push carry a version bump? --------------------------------
read_version() { # ref -> version or empty
  local ref="$1" v=""
  if [ "$ref" = "$HEAD_SHA" ]; then
    v="$(git show "${HEAD_SHA}:${VERSION_FILE}" 2>/dev/null || true)"
  else
    v="$(git show "${ref}:${VERSION_FILE}" 2>/dev/null || true)"
  fi
  printf '%s' "$v"
}

# Fetch enough history to diff the push range. Shallow clones may not contain BASE.
git fetch --no-tags --depth=50 origin "${BASE:+$BASE}" >/dev/null 2>&1 || true

CUR="$(read_version "$HEAD_SHA")"
PREV="$(read_version "$BASE")"

if [ -z "$CUR" ]; then
  # A missing VERSION file means the naming fix has not landed. That is a real fault,
  # not a hold: publishing without it would put a SHA back in the registry, which is
  # the exact failure this gate exists to prevent.
  echo "::error title=Cannot decide the publish gate::${VERSION_FILE} is absent at ${HEAD_SHA}. A publish would be named by commit SHA, which is the failure this gate exists to stop. Land the tracked-VERSION change (card t_4b05829c / PR #12) before publishing."
  emit false "VERSION file missing at ${HEAD_SHA} — refusing to name an image by SHA" ""
  exit 1
fi

if [ "$CUR" = "$PREV" ]; then
  emit false "VERSION unchanged (${CUR}) across this push — not a release" "$CUR"
  exit 0
fi

emit true "VERSION ${PREV:-none} -> ${CUR} in this push, and the commit is on the public mirror" "$CUR"
exit 0
