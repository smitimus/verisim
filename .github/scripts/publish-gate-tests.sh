#!/usr/bin/env bash
# publish-gate-tests.sh — prove publish-gate.sh decides correctly, including when it
# must REFUSE.
#
# A gate that only ever passes proves nothing. These are the real cases, run against a
# throwaway git repo so they exercise the actual script rather than a description of it.
#
# usage: bash .github/scripts/publish-gate-tests.sh
# exit  0  every case behaved as required
# exit  1  at least one did not (the gate is wrong)
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
GATE="$HERE/publish-gate.sh"

pass=0; fail=0
result() { # label expected_publish expected_exit cmd...
  local label="$1" want_pub="$2" want_rc="$3"; shift 3
  local out rc pub
  # Run in the throwaway repo: the gate reads VERSION with `git show`, so it must run
  # where those commits exist. (Running it from the caller's cwd was the first bug —
  # the harness deleted its own repo out from under the gate.)
  out="$(cd "$REPO" && "$@" 2>&1)"; rc=$?
  # The gate writes its verdict to $GITHUB_OUTPUT and echoes a summary line. Read the
  # FILE, not stdout: the gate also prints progress (the ancestry note), so parsing
  # stdout for 'publish=' is unreliable. (That was the second bug — all 7 cases
  # reported publish=? because the summary line was not the first match.)
  # $GITHUB_OUT must be EXPORTED: `run` sets it in its own shell, so an unexported
  # variable is invisible here and the fallback path reads a file that never exists.
  # (Third bug — the gate ran correctly and wrote its verdict; the harness looked in
  # the wrong place. A harness that cannot find the answer reports it as a wrong one.)
  pub="$(sed -n 's/^publish=\([a-z]*\)$/\1/p' "$GATE_OUT" 2>/dev/null | tail -1)"
  if [ "$pub" = "$want_pub" ] && [ "$rc" = "$want_rc" ]; then
    printf '  PASS  %-58s publish=%-5s exit=%s\n' "$label" "${pub:-?}" "$rc"
    pass=$((pass+1)); return 0
  fi
  printf '  FAIL  %-58s publish=%-5s exit=%s  (wanted %s/%s)\n' \
         "$label" "${pub:-?}" "$rc" "$want_pub" "$want_rc"
  printf '%s\n' "$out" | tail -4 | sed 's/^/          | /'
  fail=$((fail+1)); return 1
}

TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
REPO="$TMP/repo"
GATE_OUT="$TMP/gate-out"
mkdir -p "$REPO"; git -C "$REPO" init -q .
git -C "$REPO" config user.email t@t; git -C "$REPO" config user.name t

# Build a small history with two VERSION bumps, so we have real base/head pairs.
mk() { # version message
  printf '%s\n' "$1" > "$REPO/VERSION"
  git -C "$REPO" add VERSION >/dev/null
  git -C "$REPO" commit -q -m "$2"
}
mk 1.3.3 "release 1.3.3"
V1="$(git -C "$REPO" rev-parse HEAD)"
mk 1.3.3 "docs: no version change"
V2="$(git -C "$REPO" rev-parse HEAD)"
mk 1.3.4 "release 1.3.4"
V3="$(git -C "$REPO" rev-parse HEAD)"

run() { # base head ref event -> writes the gate verdict to $GITHUB_OUTPUT, returns its rc
  local base="$1" head="$2" ref="$3" event="$4"
  : >"$GATE_OUT"
  ( cd "$REPO" && GITHUB_OUTPUT="$GATE_OUT" GITHUB_REPOSITORY="smitimus/verisim" \
      bash "$GATE" "$base" "$head" "$ref" "$event" ) >/dev/null 2>&1
  return $?     # the gate's OWN exit status must survive to `result`
}

echo "=== publish-gate.sh ==="

# 1. a pull request never publishes
r() { run "$V1" "$V3" refs/pull/1/merge pull_request; }
result "pull request holds"                 false 0 r

# 2. a v* tag publishes and names the version
r() { run "$V1" "$V3" refs/tags/v1.3.4 push; }
result "v1.3.4 tag publishes"               true  0 r

# 3. a non-v tag does NOT publish
r() { run "$V1" "$V3" refs/tags/nightly push; }
result "non-v tag holds"                    false 0 r

# 4. main push WITH a version bump publishes
r() { run "$V2" "$V3" refs/heads/main push; }
result "main push bumps 1.3.3 -> 1.3.4"    true  0 r

# 5. main push WITHOUT a version bump holds  <- the case that fixes 8-publishes-a-day
r() { run "$V1" "$V2" refs/heads/main push; }
result "main push, VERSION unchanged holds" false 0 r

# 6. a branch other than main holds
r() { run "$V1" "$V3" refs/heads/feature/x push; }
result "non-main branch holds"              false 0 r

# 7. MISSING VERSION must FAIL, not silently hold: publishing it would put a SHA tag
#    back in the registry, which is the failure this gate exists to prevent.
#    Build this on a SEPARATE branch: deleting VERSION on the fixture's master would
#    retroactively change what `git show <earlier sha>:VERSION` returns and break the
#    cases above. (Fourth harness bug — all 7 cases shared one branch, so this one
#    mutated the history the other six depended on.)
git -C "$REPO" checkout -q -b noversion "$V3"
rm -f "$REPO/VERSION"
git -C "$REPO" add -A >/dev/null; git -C "$REPO" commit -q -m "drop VERSION"
V4="$(git -C "$REPO" rev-parse HEAD)"
r() { run "$V3" "$V4" refs/heads/main push; }
result "missing VERSION FAILS (no SHA tag)" false 1 r

echo
if [ "$fail" -eq 0 ]; then
  echo "RESULT: $pass/$((pass+fail)) cases behaved as required"
  exit 0
fi
echo "RESULT: $fail of $((pass+fail)) cases FAILED — the gate is wrong"
exit 1
