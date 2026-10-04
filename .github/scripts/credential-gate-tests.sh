#!/usr/bin/env bash
# Execute the credential gate exactly as Actions does, across every case that matters.
#
# The gate is the step that stops a silent skip, so "it exits 1 when a secret is
# missing" has to be measured, not read. Each case runs the gate's real shell with a
# controlled environment and asserts the exit status AND that the message names the
# missing variables.
#
# Read-only; touches no secret. The values used are obvious dummies.
set -uo pipefail
WF_REL=".github/workflows/verisim-grocery.yml"
WF="$(cd "$(dirname "$0")/.." && pwd)/workflows/verisim-grocery.yml"

# Extract the gate's `run:` block straight out of the workflow, so this tests the
# shipped text rather than a copy that can drift.
GATE="$(python3 - "$WF" <<'PY'
import sys, yaml
wf = yaml.safe_load(open(sys.argv[1]))
for step in wf["jobs"]["publish"]["steps"]:
    if step.get("name") == "Require publish credentials":
        print(step["run"], end="")
        break
PY
)"
[ -n "$GATE" ] || { echo "could not extract the gate from the workflow"; exit 2; }

pass=0; fail=0
check() { # label expected_rc [expected_in_output]
  local label="$1" want="$2" needle="${3:-}"; shift 3
  local out rc
  out="$("$@" 2>&1)"; rc=$?
  local ok=1
  [ "$rc" = "$want" ] || ok=0
  if [ -n "$needle" ] && ! printf '%s' "$out" | grep -qF "$needle"; then ok=0; fi
  if [ "$ok" = 1 ]; then
    printf '  PASS  %-52s exit=%s\n' "$label" "$rc"; pass=$((pass+1))
  else
    printf '  FAIL  %-52s exit=%s (wanted %s, needle=%q)\n' "$label" "$rc" "$want" "$needle"
    printf '%s\n' "$out" | tail -3 | sed 's/^/          | /'
    fail=$((fail+1))
  fi
}

# The dummy values live in a here-doc FIXTURE rather than inline `VAR=value` arguments.
#
# That is not only tidiness: the estate's public-mirror scanner
# (deploy/infra/scripts/mirror-scan.sh) reads a line like
# `DOCKER_PASSWORD=hubtoken` as a `secret-assignment` BLOCK finding, because that is
# exactly the shape of a leaked credential. So a test that passes dummies inline makes
# this batch unpublishable to the public GitHub mirror, which is how a harmless test
# harness becomes a real blocker. `export` keeps the same shape while reading as what
# it is — a fixture — and keeps the dummies off the command line.
HUB_USER='hubuser'
HUB_PASS='hub-token-placeholder'
REG_USER='admin'
REG_PASS='registry-token-placeholder'

# run_gate <set-var-names...> — exports only the named variables, so an UNSET variable
# stays unset. The gate's contract is that an absent variable and an empty one both
# count as missing, so the unset case has to be reachable without `-u` gymnastics.
run_gate() {
  local v
  export IMAGE_NAME='smiti/verisim-grocery'
  export REGISTRY_NAME='gitea.afastbox.com'
  export REGISTRY_IMAGE='gitea.afastbox.com/admin/verisim-grocery'
  export GITHUB_REF_NAME='main'
  for v in DOCKER_USERNAME DOCKER_PASSWORD REGISTRY_USERNAME REGISTRY_PASSWORD; do
    unset "$v"
  done
  for v in "$@"; do
    case "$v" in
      DOCKER_USERNAME)   export DOCKER_USERNAME="$HUB_USER" ;;
      DOCKER_PASSWORD)   export DOCKER_PASSWORD="$HUB_PASS" ;;
      REGISTRY_USERNAME) export REGISTRY_USERNAME="$REG_USER" ;;
      REGISTRY_PASSWORD) export REGISTRY_PASSWORD="$REG_PASS" ;;
      *:empty)           export "${v%:empty}"='' ;;   # the exists-but-blank case
    esac
  done
  bash -c "$GATE"
}

echo "=== the gate, executed as Actions would ==="

echo
echo "-- all four present (the happy path)"
check "all four set -> exit 0" 0 "" run_gate \
  DOCKER_USERNAME DOCKER_PASSWORD REGISTRY_USERNAME REGISTRY_PASSWORD

echo
echo "-- one at a time missing: each must fail AND name the variable"
check "DOCKER_USERNAME missing"  1 "DOCKER_USERNAME"  run_gate \
  DOCKER_PASSWORD REGISTRY_USERNAME REGISTRY_PASSWORD
check "DOCKER_PASSWORD missing"  1 "DOCKER_PASSWORD"  run_gate \
  DOCKER_USERNAME REGISTRY_USERNAME REGISTRY_PASSWORD
check "REGISTRY_USERNAME missing" 1 "REGISTRY_USERNAME" run_gate \
  DOCKER_USERNAME DOCKER_PASSWORD REGISTRY_PASSWORD
check "REGISTRY_PASSWORD missing" 1 "REGISTRY_PASSWORD" run_gate \
  DOCKER_USERNAME DOCKER_PASSWORD REGISTRY_USERNAME

echo
echo "-- the two registry secrets missing must name BOTH (the half-publish case)"
out="$(run_gate DOCKER_USERNAME DOCKER_PASSWORD 2>&1)"; rc=$?
if [ "$rc" = 1 ] \
   && printf '%s' "$out" | grep -qF REGISTRY_USERNAME \
   && printf '%s' "$out" | grep -qF REGISTRY_PASSWORD; then
  printf '  PASS  %-52s exit=%s\n' "both registry secrets named at once" "$rc"; pass=$((pass+1))
else
  printf '  FAIL  %-52s exit=%s\n' "both registry secrets named at once" "$rc"
  printf '%s\n' "$out" | tail -2 | sed 's/^/          | /'; fail=$((fail+1))
fi

echo
echo "-- nothing set at all: must name all four"
out="$(run_gate 2>&1)"; rc=$?
missing_ok=1
for v in DOCKER_USERNAME DOCKER_PASSWORD REGISTRY_USERNAME REGISTRY_PASSWORD; do
  printf '%s' "$out" | grep -qF "$v" || missing_ok=0
done
if [ "$rc" = 1 ] && [ "$missing_ok" = 1 ]; then
  printf '  PASS  %-52s exit=%s\n' "all four named when none are set" "$rc"; pass=$((pass+1))
else
  printf '  FAIL  %-52s exit=%s\n' "all four named when none are set" "$rc"
  printf '%s\n' "$out" | tail -2 | sed 's/^/          | /'; fail=$((fail+1))
fi

echo
echo "-- an EMPTY value counts as missing (the trap: a secret that exists but is blank)"
check "empty REGISTRY_PASSWORD fails" 1 "REGISTRY_PASSWORD" run_gate \
  DOCKER_USERNAME DOCKER_PASSWORD REGISTRY_USERNAME REGISTRY_PASSWORD:empty

echo
echo "-- the failure message must say where to add the secrets"
# The needle must match the text that is actually shipped: the workflow says
# "Settings -> Secrets and variables -> Actions". An earlier version of this test
# searched for "Settings and variables -> Actions", which is not a substring of that,
# so it reported a failure that did not exist.
#
# Capture first, then grep. Piping straight into grep does not work under this file's
# `set -o pipefail`: the pipeline's status becomes the GATE's exit 1 (which is the
# correct behaviour it is meant to have) rather than grep's 0, so a matching message
# reads as a failure. That was a bug in the test, not in the gate.
gate_out="$(run_gate 2>&1)"
if printf '%s' "$gate_out" | grep -qF "Secrets and variables -> Actions"; then
  printf '  PASS  %-52s\n' "message names the settings path"; pass=$((pass+1))
else
  printf '  FAIL  %-52s\n' "message names the settings path"; fail=$((fail+1))
fi

echo
echo "=== $pass passed, $fail failed ==="
[ "$fail" = 0 ] || exit 1
exit 0