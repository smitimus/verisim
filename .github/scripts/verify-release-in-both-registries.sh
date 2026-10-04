#!/usr/bin/env bash
# =============================================================================
# verify-release-in-both-registries.sh — assert a release is really in BOTH
# registries, at the SAME index digest, before CI is allowed to call it published.
#
# WHY THIS EXISTS
#
# data-lab pins verisim-grocery by DIGEST in our own Gitea registry
# (verisim-grocery/compose.yaml + docs/image-pins.lock), and every pin has been
# hand-re-mirrored because the publish job targeted Docker Hub only. Twice in one
# day (2026-10-04: 52a93b5 at 08:45Z, 416ea83 at 10:11Z) a release existed in the
# Hub and 404'd in Gitea, so the fleet could not reach the fix and the pin had to be
# bumped by hand. A comment asking for a re-mirror is not enforcement; this is.
#
# WHAT IT ASSERTS, IN ORDER
#
#   1. both references resolve to a NON-EMPTY index digest (a green run that pushed
#      nothing is the exact failure this repo already had once — see the
#      `Require Docker Hub credentials` step and the `latest`-frozen-at-1.3.2 era);
#   2. those two digests are EQUAL — one build, one digest, in both registries, so a
#      pin written against one keeps resolving in the other;
#   3. the Gitea digest resolves ANONYMOUSLY, i.e. a fresh slot with no registry
#      login can `docker compose pull` it. That is the property data-lab depends on
#      (CT106 and CT107 hold no /root/.docker/config.json) and it is the regression
#      behind "the pinned digest is ORPHANED — a fresh slot cannot rebuild the
#      stack".
#
# NOT A GREEN-JOB SUBSTITUTE FOR THE DIGEST
#
# It reads each registry's own response and hashes it, rather than trusting a build
# log or a run conclusion. A previous draft compared two EMPTY strings and printed
# "IDENTICAL" — the same failure as a green CI run that published nothing, one level
# down. So an empty digest is a hard failure here, never a match.
#
# usage:
#   verify-release-in-both-registries.sh <hub-ref> <gitea-ref> [<label>]
#
#   hub-ref    e.g. smiti/verisim-grocery:416ea83c67757676fee328eb0f74b35660bbd6a2
#   gitea-ref  e.g. gitea.afastbox.com/admin/verisim-grocery:dev-2026-10-04-1011
#
# exit 0  both registries serve the same, anonymously-resolvable index digest
# exit 1  the release is NOT safely in both registries (message names which check)
# exit 2  usage error
# =============================================================================
set -uo pipefail

HUB_REF="${1:-}"
GITEA_REF="${2:-}"
LABEL="${3:-${GITHUB_SHA:-unknown}}"

if [ -z "$HUB_REF" ] || [ -z "$GITEA_REF" ]; then
  echo "usage: $(basename "$0") <hub-ref> <gitea-ref> [label]" >&2
  exit 2
fi

# Accepts a registry HTTP error body as readily as a manifest — a 404 must be
# reported as a failure, not parsed into an empty digest and compared as "equal".
ACCEPT='application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.list.v2+json,application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json'

die() { echo "::error title=$1::$2"; echo "$1: $2" >&2; exit 1; }

# ---------------------------------------------------------------------------
# registry plumbing
#
# Gitea's container registry challenges with `Bearer realm=.../v2/token` and its
# token endpoint issues an ANONYMOUS-scope pull token for these packages, so no
# credential is sent or needed here. The Authorization header is written at runtime
# into a mode-600 file outside the repo: a secret-scrubbing pass rewrites any
# auth-header line that carries a token variable in a tracked file, and a literal
# `***` gets 401'd, which would make a perfectly pullable image look deleted.
# ---------------------------------------------------------------------------
hdr_file=""
body_file="$(mktemp)"
code_file="$(mktemp)"
cleanup() { rm -f "$body_file" "$code_file" ${hdr_file:+"$hdr_file"}; }
trap cleanup EXIT

split_ref() {  # ref -> echoes "host repo" where REPO EXCLUDES the tag; Hub is the default
  local ref="$1" first="${1%%/*}" path
  case "$first" in
    *.*|*:*) path="${ref#*/}" ;;
    *)       path="$ref" ;;
  esac
  # The registry host may itself carry a port, so the repo/tag split is decided by the
  # LAST colon in what is left — that is the tag separator.
  case "$path" in
    *:*) path="${path%:*}" ;;
  esac
  case "$first" in
    *.*|*:*) printf '%s %s\n' "$first" "$path" ;;
    *)       printf 'registry-1.docker.io %s\n' "$path" ;;
  esac
}

# The TAG is whatever followed that last colon, or "" when the ref names no tag.
#
# `${ref##*:}` is wrong, and it is wrong in a way that looks like a registry outage:
# on `gitea.afastbox.com/admin/verisim-grocery:dev-2026-10-04-1011` it yields the tag
# (fine), but on `host:5000/repo:tag` it yields `5000/repo:tag`, and on a ref with no
# tag at all it yields the host. Strip the registry first, then the tag.
ref_tag() {  # ref -> echoes the tag, or "" when the ref names no tag
  local ref="$1" path
  case "${ref%%/*}" in
    *.*|*:*) path="${ref#*/}" ;;
    *)       path="$ref" ;;
  esac
  case "$path" in
    *:*) printf '%s\n' "${path##*:}" ;;
    *)   printf '\n' ;;
  esac
}

token_for() {  # host path -> echoes an anonymous pull token (empty on failure)
  local host="$1" path="$2" url
  case "$host" in
    *ghcr.io*) url="https://ghcr.io/token?service=ghcr.io&scope=repository:${path}:pull" ;;
    *docker.io*) url="https://auth.docker.io/token?service=registry.docker.io&scope=repository:${path}:pull" ;;
    *gitea*)    url="https://${host}/v2/token?service=container_registry&scope=repository:${path}:pull" ;;
    *)          url="" ;;
  esac
  [ -n "$url" ] || return 0
  curl -s --max-time 20 "$url" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p'
}

index_digest() {  # ref -> echoes sha256:<hex>, or HTTP-<code>/TRANSPORT-FAILED
  #
  # The tag is computed HERE, inside this function, and never carried in a global.
  # A global set by the caller would be empty inside a command substitution, because
  # `$( )` runs in a subshell that cannot write back — which silently turned the
  # manifest URL into .../manifests/ (the repository digest endpoint) and made a
  # perfectly healthy release read as "HTTP-400 answered".
  local ref="$1" host path tok tag d
  read -r host path <<<"$(split_ref "$ref")"
  tag="$(ref_tag "$ref")"
  tok="$(token_for "$host" "$path")"
  if [ -n "$tok" ]; then
    hdr_file="$(mktemp)"; chmod 600 "$hdr_file"
    printf 'header = "Authorization: Bearer %s"\n' "$tok" > "$hdr_file"
  fi
  # A transport failure and an HTTP status must stay distinguishable: DNS failure is
  # a broken probe, not evidence about the image.
  if ! curl -s --max-time 30 ${hdr_file:+--config "$hdr_file"} -H "Accept: $ACCEPT" \
        -o "$body_file" -w '%{http_code}' \
        "https://${host}/v2/${path}/manifests/${tag}" > "$code_file" 2>/dev/null; then
    rm -f ${hdr_file:+"$hdr_file"}; hdr_file=""
    echo "TRANSPORT-FAILED"
    return
  fi
  rm -f ${hdr_file:+"$hdr_file"}; hdr_file=""
  local code; code="$(tr -d ' \n' < "$code_file")"
  if [ "$code" != "200" ]; then
    echo "HTTP-${code}"
    return
  fi
  d="$(python3 -c 'import sys,hashlib;b=sys.stdin.buffer.read();print("sha256:"+hashlib.sha256(b).hexdigest())' \
        < "$body_file" 2>/dev/null)"
  [ -n "$d" ] && echo "$d"
}

digest_of() {  # ref -> index digest via imagetools, falling back to the HTTP read
  local ref="$1" d=""
  d="$(docker buildx imagetools inspect "$ref" 2>/dev/null |
       sed -n 's/^Digest:[[:space:]]*//p' | head -1)"
  if [ -n "$d" ]; then printf '%s\n' "$d"; return 0; fi
  # imagetools needs a resolvable host; fall back to speaking the registry API
  # directly, which is also what the anonymous-pull check below does.
  index_digest "$ref"
}

echo "=== verifying release ${LABEL} is in BOTH registries ==="
echo "  hub   : ${HUB_REF}"
echo "  gitea : ${GITEA_REF}"
echo

HUB_DIGEST="$(digest_of "$HUB_REF")"
GITEA_DIGEST="$(digest_of "$GITEA_REF")"

echo "  hub   index digest : ${HUB_DIGEST:-<none>}"
echo "  gitea index digest : ${GITEA_DIGEST:-<none>}"
echo

# --- check 1: both resolved at all ------------------------------------------
# An empty digest is a FAILURE, never a match. Two empty strings comparing equal is
# precisely how a publish that pushed nothing gets reported as a match.
[ -z "$HUB_DIGEST" ] && die \
  "Docker Hub has no index for ${LABEL}" \
  "docker buildx imagetools inspect ${HUB_REF} returned no digest. The Hub push did not land, or the tag does not exist. Do NOT treat this run as published."

case "$HUB_DIGEST" in
  HTTP-*|TRANSPORT-FAILED)
    die "Docker Hub probe failed for ${LABEL}" \
      "${HUB_REF} answered ${HUB_DIGEST}. That is the registry's answer (or a broken probe), not a published artifact." ;;
esac

[ -z "$GITEA_DIGEST" ] && die \
  "Gitea has no index for ${LABEL}" \
  "docker buildx imagetools inspect ${GITEA_REF} returned no digest. The Gitea push did not land — this is the orphaning failure that left data-lab's pin unresolvable (t_94476148)."

case "$GITEA_DIGEST" in
  HTTP-*|TRANSPORT-FAILED)
    die "Gitea probe failed for ${LABEL}" \
      "${GITEA_REF} answered ${GITEA_DIGEST}. The Gitea push did not land, or the registry is unreachable." ;;
esac

# --- check 2: the same digest in both ---------------------------------------
if [ "$HUB_DIGEST" != "$GITEA_DIGEST" ]; then
  die "the two registries disagree for ${LABEL}" \
    "hub=${HUB_DIGEST} gitea=${GITEA_DIGEST}. data-lab pins ONE digest in the Gitea registry; a release with two digests cannot be pinned consistently. This usually means the Gitea push carried a different index than the Hub push."
fi

echo "  OK: both registries serve ${HUB_DIGEST}"
echo

# --- check 3: Gitea resolves it with NO credential ---------------------------
# The property data-lab actually depends on: a fresh slot has no registry login, so
# if this needs a credential the pin is not safe to hand to a slot.
read -r g_host g_path <<<"$(split_ref "$GITEA_REF")"
g_tag="$(ref_tag "$GITEA_REF")"
ANON="$(index_digest "$GITEA_REF")"
case "$ANON" in
  "$GITEA_DIGEST")
    echo "  OK: gitea.afastbox.com resolves ${GITEA_DIGEST} ANONYMOUSLY (a fresh slot can pull it)"
    ;;
  HTTP-401|HTTP-403)
    die "Gitea ${LABEL} does not resolve anonymously" \
      "${g_host}/${g_path}:${g_tag} answered ${ANON} with no credential. A fresh slot holds no registry login, so a pin on this digest would break 'docker compose pull' on any new host." ;;
  HTTP-*)
    die "Gitea anonymous probe of ${LABEL} failed" \
      "${g_host}/${g_path}:${g_tag} answered ${ANON}." ;;
  TRANSPORT-FAILED)
    die "Gitea anonymous probe of ${LABEL} could not run" \
      "the request to ${g_host} failed before an HTTP status (DNS/TCP/TLS). That is a broken probe, not evidence about the image." ;;
  "")
    die "Gitea anonymous probe of ${LABEL} returned no digest" \
      "${g_host}/${g_path}:${g_tag} produced no index without a credential." ;;
esac

echo
echo "=== release ${LABEL} verified in both registries at one digest ==="
echo "    ${GITEA_DIGEST}"
echo "    pin this digest in data-lab: ${g_path}@${GITEA_DIGEST}"
exit 0