#!/usr/bin/env bash
# =============================================================================
# negative-tests.sh — prove verify-release-in-both-registries.sh actually FAILS when a
# release is not safely in both registries.
#
# A verifier that only ever passes proves nothing. These cases are the real failure
# modes, and they run against the LIVE registries rather than a mock, because the
# behaviours being tested are registry behaviours (404 on a missing tag, an anonymous
# token exchange, digest computation).
#
# The cases are DISCOVERED from each registry's own tag list, never hard-coded to a
# release that will eventually be garbage-collected. That matters: the draft this
# replaces pinned `dev-2026-10-04-1011` and `dev-2026-10-04-0845`, so once Gitea prunes
# either tag the suite would start failing for a reason that has nothing to do with the
# code under test. A harness that rots is worse than no harness.
#
# Both sides need no credential: Gitea issues an anonymous-scope pull token for these
# packages (verified — CT106 and CT107 hold no registry login and pull fine).
#
# usage: bash .github/scripts/negative-tests.sh
# exit  0  every case behaved as required
# exit  1  at least one case did not (the verifier is wrong)
# exit 77  skipped (a registry was unreachable, so nothing could be asserted)
# =============================================================================
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
V="$HERE/verify-release-in-both-registries.sh"

HUB_REPO="smiti/verisim-grocery"
REG_REPO="gitea.afastbox.com/admin/verisim-grocery"

pass=0; fail=0
note() { printf '\n=== %s ===\n' "$1"; }

result() { # label expected cmd...
  local label="$1" want="$2"; shift 2
  local out rc
  out="$("$@" 2>&1)"; rc=$?
  if [ "$rc" = "$want" ]; then
    printf '  PASS  %-56s exit=%s (wanted %s)\n' "$label" "$rc" "$want"
    pass=$((pass+1)); return 0
  fi
  printf '  FAIL  %-56s exit=%s (wanted %s)\n' "$label" "$rc" "$want"
  printf '%s\n' "$out" | tail -5 | sed 's/^/          | /'
  fail=$((fail+1)); return 1
}

digest_of() { docker buildx imagetools inspect "$1" 2>/dev/null |
              sed -n 's/^Digest:[[:space:]]*//p' | head -1; }

list_tags() { # repo -> one tag per line
  local repo="$1" host path tok url hdr=""
  case "${repo%%/*}" in
    *.*|*:*) host="${repo%%/*}"; path="${repo#*/}" ;;
    *)       host="registry-1.docker.io"; path="$repo" ;;
  esac
  case "$host" in
    *docker.io*) url="https://auth.docker.io/token?service=registry.docker.io&scope=repository:${path}:pull" ;;
    *gitea*)     url="https://${host}/v2/token?service=container_registry&scope=repository:${path}:pull" ;;
    *)           url="" ;;
  esac
  if [ -n "$url" ]; then
    hdr="$(mktemp)"; chmod 600 "$hdr"
    tok="$(curl -s --max-time 20 "$url" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')"
    [ -n "$tok" ] && printf 'header = "Authorization: Bearer %s"\n' "$tok" > "$hdr"
  fi
  curl -s --max-time 25 ${hdr:+--config "$hdr"} \
    "https://${host}/v2/${path}/tags/list?n=1000" \
    | tr ',' '\n' | sed -n 's/.*"\([A-Za-z0-9._-]\{1,\}\)".*/\1/p' \
    | grep -Ev '^(name|tags)$' | sort -u
  [ -n "$hdr" ] && rm -f "$hdr"
  return 0
}

echo "verifier under test : $V"
echo "discovering live tags from both registries..."
HUB_TAGS="$(list_tags "$HUB_REPO" | grep -E '^[0-9a-f]{40}$' | sort -r | head -20)"
REG_TAGS="$(list_tags "$REG_REPO" | grep -E '^dev-' | sort -r | head -20)"

if [ -z "$HUB_TAGS" ] || [ -z "$REG_TAGS" ]; then
  echo "  SKIP: could not list tags (hub=$(printf '%s\n' "$HUB_TAGS" | grep -c . || true)"
  echo "        reg=$(printf '%s\n' "$REG_TAGS" | grep -c . || true)) — nothing to assert."
  exit 77
fi
echo "  hub commit tags : $(printf '%s ' $HUB_TAGS)"
echo "  gitea dev tags  : $(printf '%s ' $REG_TAGS)"

# Find a pair that genuinely names ONE index in both registries. Discovered, not
# assumed: after this card lands CI publishes both, so the newest release qualifies
# automatically; before that, a hand-mirror (t_94476148) also qualifies.
GOOD_HUB=""; GOOD_REG=""
for ht in $HUB_TAGS; do
  hd="$(digest_of "${HUB_REPO}:${ht}")"
  [ -n "$hd" ] || continue
  for rt in $REG_TAGS; do
    [ "$(digest_of "${REG_REPO}:${rt}")" = "$hd" ] || continue
    GOOD_HUB="$ht"; GOOD_REG="$rt"; break 2
  done
done
echo "  release in both : hub=${GOOD_HUB:-<none>} gitea=${GOOD_REG:-<none>}"

# --- T1 usage error -----------------------------------------------------------
note "T1 usage error"
result "no arguments -> exit 2" 2 bash "$V"

# --- T2 absent on both sides --------------------------------------------------
# The important one: an absent digest must NEVER compare equal to another absent
# digest. Two empty strings are the "green job that published nothing" failure mode.
note "T2 a tag in NEITHER registry must not read as 'both empty = equal'"
MISSING="no-such-tag-t_ff5a70ec-$$"
result "missing on both sides" 1 bash "$V" "${HUB_REPO}:${MISSING}" "${REG_REPO}:${MISSING}" t2-nowhere

# --- T3 a real Gitea release against a Hub tag that was never pushed -----------
note "T3 a real Gitea release paired with a Hub tag that was never pushed"
result "hub tag absent" 1 bash "$V" "${HUB_REPO}:${MISSING}" "${REG_REPO}:${GOOD_REG:-$MISSING}" t3-hub-absent

# --- T4 two DIFFERENT real indexes must not read as in sync --------------------
note "T4 two different real indexes must disagree"
MISMATCH_REG=""
if [ -n "$GOOD_HUB" ]; then
  hd="$(digest_of "${HUB_REPO}:${GOOD_HUB}")"
  for rt in $REG_TAGS; do
    rd="$(digest_of "${REG_REPO}:${rt}")"
    if [ -n "$rd" ] && [ "$rd" != "$hd" ]; then MISMATCH_REG="$rt"; break; fi
  done
fi
if [ -n "$MISMATCH_REG" ]; then
  echo "  (hub ${GOOD_HUB} vs gitea ${MISMATCH_REG} — genuinely different indexes)"
  result "hub release vs a DIFFERENT gitea index" 1 bash "$V" \
    "${HUB_REPO}:${GOOD_HUB}" "${REG_REPO}:${MISMATCH_REG}" t4-mismatch
else
  echo "  SKIP: no Gitea dev- tag currently names a different index from ${GOOD_HUB:-the hub release}"
fi

# --- T5 a release in both registries must PASS ---------------------------------
note "T5 a release in both registries must PASS (no false alarm)"
if [ -n "$GOOD_HUB" ] && [ -n "$GOOD_REG" ]; then
  result "discovered release present in both" 0 bash "$V" \
    "${HUB_REPO}:${GOOD_HUB}" "${REG_REPO}:${GOOD_REG}" t5-good
else
  echo "  SKIP: no release is currently present in both registries — expected before the"
  echo "        first CI dual publish; the Hub releases so far were mirrored by hand."
fi

note "summary"
printf '  %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" = "0" ] || exit 1
exit 0