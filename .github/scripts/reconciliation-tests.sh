#!/usr/bin/env bash
# =============================================================================
# The reconciliation gate for t_e1b1de67.
#
# PR #9 (t_ff5a70ec) and PR #11 (t_4b05829c) each rewrote the publish job's tag
# and push steps in .github/workflows/verisim-grocery.yml. They were COMPLEMENTARY
# — #9 decided WHERE the image goes, #11 decided WHAT the tags are called — but
# they occupied the same hunk, and both bodies were valid YAML. So a merge of the
# second would have silently discarded half of the first: either the version tag
# (a main push back to :${GITHUB_SHA}, which is what t_4b05829c exists to fix) or
# the second registry (Docker Hub only, which is what left data-lab's digest pin
# unresolvable for hours in t_3f078e61).
#
# A merge-conflict resolution would NOT have surfaced that: both bodies parse. So
# this asserts the SHAPE of the merged result — the behaviour of BOTH cards — by
# running the shipped workflow and the shipped scripts, not by reading the diff.
#
# Each section is a claim either PR made. If a future edit drops one, this fails.
#
# Usage: bash .github/scripts/reconciliation-tests.sh [checkout]
# Exit:  0 = every claim holds, 1 = a claim is broken.
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO="${1:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
WF="${REPO}/.github/workflows/verisim-grocery.yml"
COMPUTE="${SCRIPT_DIR}/compute-push-tags.sh"
VERIFY="${SCRIPT_DIR}/verify-release-in-both-registries.sh"
GATE_TESTS="${SCRIPT_DIR}/credential-gate-tests.sh"

PASS=0
FAIL=0
SHA=0123456789abcdef0123456789abcdef01234567
REG='gitea.afastbox.com/admin/verisim-grocery'

ok()  { PASS=$((PASS + 1)); printf '  PASS  %s\n' "$1"; return 0; }
bad() { FAIL=$((FAIL + 1)); printf '  FAIL  %s\n' "$1"; [ $# -gt 1 ] && printf '        %s\n' "$2"; return 0; }

TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

# Pull the publish job's steps out of the SHIPPED workflow, so this tests the
# text that will actually run rather than a copy that can drift.
STEPS="$(python3 - "${WF}" <<'PY'
import sys, yaml
wf = yaml.safe_load(open(sys.argv[1]))
for step in wf["jobs"]["publish"]["steps"]:
    print("===STEP=== " + str(step.get("name", "<unnamed>")))
    if "run" in step:
        print(step["run"])
    if "uses" in step:
        print("USES: " + step["uses"])
    if "with" in step:
        print("WITH: " + yaml.safe_dump(step["with"], default_flow_style=True))
PY
)"

step_block() { printf '%s\n' "${STEPS}" | awk -v want="$1" '
  /^===STEP=== / { inblk = (substr($0, 12) == want) }
  inblk { print }
'; }

has_step() { printf '%s\n' "${STEPS}" | grep -qx "===STEP=== $1"; }

echo "reconciliation gate — both PRs' behaviour survives the merge (t_e1b1de67)"
echo

# ── Claim 1 (t_4b05829c): a main push publishes the TRACKED VERSION ─────────
if grep -q "compute-push-tags.sh" "${WF}"; then
  ok "the workflow computes tags through the shared script"
else
  bad "the workflow computes tags through the shared script" \
      "no compute-push-tags.sh reference — tags are being computed inline again"
fi

run_compute() {  # run_compute <ref> <ref_name> [extra env assignments...]
  local ref="$1" name="$2"; shift 2
  env REPO_ROOT="${REPO}" IMAGE_NAME=smiti/verisim-grocery \
      GITHUB_REF="${ref}" GITHUB_REF_NAME="${name}" GITHUB_SHA="${SHA}" \
      "$@" bash "${COMPUTE}" 2>"${TMP}/err"
}

MAIN_OUT="$(run_compute refs/heads/main main REGISTRY_IMAGE="${REG}" REQUIRE_SECOND_REGISTRY=true)"
field() { printf '%s\n' "${MAIN_OUT}" | grep "^$1=" | head -1 | cut -d= -f2-; }

# Membership in a COMMA-separated list, tested by exact element comparison rather
# than by substring. Two earlier attempts were wrong in instructive ways:
#
#   * matching against the whole stdout passes on `version_ref=…:1.3.4` even when
#     the tag was deleted from the push — it hides the very mutation it must
#     catch; and
#   * wrapping the list in `:` delimiters and globbing `*":needle,"*` never
#     matches, because the character before an element is a COMMA, not a colon.
#
# So: split on commas and ask whether one element equals the needle.
has_tag() {  # has_tag <full-ref>
  printf '%s' "$(field tags)" | tr ',' '\n' | grep -qxF "$1"
}

if [ -n "$(field version)" ]; then
  ok "a main push emits a tracked version ($(field version))"
else
  bad "a main push emits a tracked version" "no version= field; main is back to SHA-only tags"
fi

# The version tag must be on BOTH registries. Before the merge it existed only on
# the Hub; after it, the vocabulary is identical on both sides.
if has_tag "smiti/verisim-grocery:$(field version)"; then
  ok "the version tag is on Docker Hub"
else
  bad "the version tag is on Docker Hub" "not in the pushed tag list: $(field tags)"
fi
if has_tag "${REG}:$(field version)"; then
  ok "the version tag is ALSO on our registry (the merge's whole point)"
else
  bad "the version tag is ALSO on our registry" \
      "the second registry has no version tag — the two PRs' tag schemes did not merge. tags=$(field tags)"
fi

# `latest` must survive on the Hub: `switch.sh release` pulls it, and it is what
# was frozen at 1.3.2 during the era when a green job published nothing.
if has_tag "smiti/verisim-grocery:latest"; then
  ok "latest is still published on Docker Hub"
else
  bad "latest is still published on Docker Hub" "not in the pushed tag list: $(field tags)"
fi

# The commit tag rides on both sides: the Hub ref is what the in-CI verifier keys
# off, and the Gitea one is the tag a data-lab re-pin names.
if has_tag "smiti/verisim-grocery:${SHA}" && has_tag "${REG}:${SHA}"; then
  ok "the commit tag is on both registries (traceability, and the re-pin name)"
else
  bad "the commit tag is on both registries" "missing from: $(field tags)"
fi

# ── Claim 2 (t_ff5a70ec): the push reaches BOTH registries ─────────────────
if grep -q "REGISTRY_IMAGE" "${WF}" && grep -q "${REG%/*}" "${WF}"; then
  ok "the workflow names our registry as a destination"
else
  bad "the workflow names our registry as a destination" "REGISTRY_IMAGE is gone"
fi

if has_step "Log in to our registry"; then
  ok "the workflow logs in to our registry"
else
  bad "the workflow logs in to our registry" "no second login step — a push there cannot authenticate"
fi

if [ -n "$(field registry_tags)" ]; then
  ok "the script emits the second registry's refs"
else
  bad "the script emits the second registry's refs" "no registry_tags= field"
fi

# One buildx push carrying every tag is what makes both registries hold the SAME
# index digest. Two pushes would produce two indexes and break every digest pin.
push_step="$(step_block "Push to both registries")"
if printf '%s' "${push_step}" | grep -q "steps.meta.outputs.tags" \
   && printf '%s' "${push_step}" | grep -q "push: true"; then
  ok "ONE push carries all the tags (one index digest in both registries)"
else
  bad "ONE push carries all the tags" "the push step no longer takes the combined tag list"
fi

# ── Claim 3 (t_ff5a70ec): the FOUR-secret credential gate survives ──────────
if has_step "Require publish credentials"; then
  ok "the four-secret credential gate is present"
else
  bad "the four-secret credential gate is present" "the gate step was renamed or dropped"
fi

gate="$(step_block "Require publish credentials")"
missing_from_gate=""
for v in DOCKER_USERNAME DOCKER_PASSWORD REGISTRY_USERNAME REGISTRY_PASSWORD; do
  printf '%s' "${gate}" | grep -q "$v" || missing_from_gate="${missing_from_gate} ${v}"
done
if [ -z "${missing_from_gate}" ]; then
  ok "the gate names all four secrets (not a DOCKER_*-only gate)"
else
  bad "the gate names all four secrets" "absent from the gate:${missing_from_gate}"
fi

# A two-secret gate is the exact regression that would let a half-publish through,
# so the harness that proves the gate's behaviour runs here too.
if [ -x "${GATE_TESTS}" ] || [ -f "${GATE_TESTS}" ]; then
  if bash "${GATE_TESTS}" >"${TMP}/gate.log" 2>&1; then
    ok "the gate harness passes ($(grep -oE '[0-9]+ passed, [0-9]+ failed' "${TMP}/gate.log" | tail -1))"
  else
    bad "the gate harness passes" "$(tail -3 "${TMP}/gate.log")"
  fi
fi

# ── Claim 4 (t_ff5a70ec): the in-CI verification step survives ────────────
if has_step "Verify the release is in both registries at one digest"; then
  ok "the in-CI digest verification step survives"
else
  bad "the in-CI digest verification step survives" "the verification step was dropped — a green job would no longer prove anything"
fi

if [ -f "${VERIFY}" ]; then
  ok "verify-release-in-both-registries.sh ships ($(wc -l < "${VERIFY}") lines)"
else
  bad "verify-release-in-both-registries.sh ships" "the file is missing"
fi

# The verifier reads back what the tag step emitted. If those keys were renamed
# on one side only, the step would run with empty refs and pass vacuously — the
# same class of bug as a green publish that published nothing.
verify_step="$(step_block "Verify the release is in both registries at one digest")"
missing_refs=""
for k in hub_ref registry_ref; do
  printf '%s' "${verify_step}" | grep -q "steps.meta.outputs.${k}" \
    || missing_refs="${missing_refs} ${k}"
done
if [ -z "${missing_refs}" ]; then
  ok "the verifier reads back the refs the tag step emits"
else
  bad "the verifier reads back the refs the tag step emits" "not consumed:${missing_refs}"
fi

for k in hub_ref registry_ref; do
  if printf '%s\n' "${MAIN_OUT}" | grep -q "^${k}="; then
    ok "the script emits ${k}"
  else
    bad "the script emits ${k}" "the verification step would pass an empty ref"
  fi
done

# ── Claim 5 (this card): no tag computation is left in the workflow ────────
# The reason this card exists. Any line in the workflow that builds a tag from
# the ref or the sha is a second owner of a tag name, which is how the two PRs
# came to overwrite each other in the first place.
inline="$(grep -nE '^\s*(hub_tags|registry_tags|tags)=' "${WF}" \
          | grep -vE 'compute-push-tags|^\s*#' || true)"
if [ -z "${inline}" ]; then
  ok "no inline tag computation remains in the workflow"
else
  bad "no inline tag computation remains in the workflow" "${inline}"
fi

# And the guard that makes a lost REGISTRY_IMAGE loud rather than a Hub-only push.
#
# Asserted by EXIT CODE, twice. Reading stdout instead would be satisfied by a
# script that printed the tag list and *then* died, and by one that died with the
# old tags already on stdout — so a removal of the guard reads as a pass. The
# check is: it fails when the registry is REQUIRED, and it succeeds when the image
# is Hub-only. Both halves matter, because a guard that always fires would break
# gas-station.
guarded() {  # guarded -> 0 when the script REFUSES an unset REGISTRY_IMAGE
  REGISTRY_IMAGE= REQUIRE_SECOND_REGISTRY=true \
    REPO_ROOT="${REPO}" IMAGE_NAME=smiti/verisim-grocery \
    GITHUB_REF=refs/heads/main GITHUB_REF_NAME=main GITHUB_SHA="${SHA}" \
    bash "${COMPUTE}" >"${TMP}/guarded.out" 2>"${TMP}/guarded.err"
}

if guarded; then
  bad "an unset REGISTRY_IMAGE is refused when the registry is required" \
      "the script exited 0 and would have published to Docker Hub only — the four-secret credential gate would still have passed, so a lost env var becomes a silent half-publish"
else
  if grep -q "REGISTRY_IMAGE" "${TMP}/guarded.err"; then
    ok "an unset REGISTRY_IMAGE is refused, naming REGISTRY_IMAGE"
  else
    bad "an unset REGISTRY_IMAGE is refused, naming REGISTRY_IMAGE" \
        "it failed without saying which variable was missing"
  fi
  # A refusal must not also leak a Hub-only tag list — a caller that ignores the
  # exit code would then publish exactly what the guard exists to prevent.
  if [ -n "$(grep '^tags=' "${TMP}/guarded.out")" ]; then
    bad "a refused publish emits no tag list" \
        "stdout still carried $(grep '^tags=' "${TMP}/guarded.out")"
  else
    ok "a refused publish emits no tag list (nothing to publish by accident)"
  fi
fi

# A Hub-only image (gas-station) must still publish: the guard is opt-in.
if REGISTRY_IMAGE= REQUIRE_SECOND_REGISTRY=false \
   REPO_ROOT="${REPO}" IMAGE_NAME=smiti/verisim-gas-station \
   GITHUB_REF=refs/heads/main GITHUB_REF_NAME=main GITHUB_SHA="${SHA}" \
   bash "${COMPUTE}" >"${TMP}/hubonly.out" 2>/dev/null \
   && grep -q '^tags=' "${TMP}/hubonly.out"; then
  ok "a Hub-only image still publishes (gas-station is unaffected)"
else
  bad "a Hub-only image still publishes" "the second-registry guard leaked into the Hub-only path"
fi

echo
echo "reconciliation-tests: ${PASS} passed, ${FAIL} failed"
[ "${FAIL}" -eq 0 ]