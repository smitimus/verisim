#!/bin/bash
# =============================================================================
# The publish-tag convention, asserted by executing it (t_4b05829c).
#
# compute-push-tags.sh is the only thing that decides what a release is called
# in the registry. This harness runs it for real, against temp checkouts, for
# every branch of the contract:
#
#   * a main push publishes the TRACKED VERSION, never a bare 40-char SHA
#   * no tag is ever a 40-character SHA standing in for a version
#   * a v* tag push must agree with VERSION, or it fails
#   * a malformed / missing / disagreeing VERSION fails loudly
#   * `latest` is always present (switch.sh release pulls it)
#
# It runs against real files rather than mocks: the whole class of bug here is
# a script that exits 0 while emitting the wrong string, and a mocked
# computation would have passed forever.
#
# Usage: bash .github/scripts/publish-tag-tests.sh [checkout]
# Exit:  0 = every case passed, 1 = a case failed.
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO="${1:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
COMPUTE="${SCRIPT_DIR}/compute-push-tags.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

PASS=0
FAIL=0
SHA=0123456789abcdef0123456789abcdef01234567

ok()  { PASS=$((PASS + 1)); echo "  PASS  ${1}"; return 0; }
bad() { FAIL=$((FAIL + 1)); echo "  FAIL  ${1}"; [ $# -gt 1 ] && echo "        ${2}"; return 0; }

# A throwaway checkout holding just what the script reads: the script, a VERSION
# and a pyproject.toml. Deliberately not a copy of the real repo — these cases
# must not be able to pass because of something unrelated in the tree.
fixture() {
  local dir="${TMP}/$1" version="$2" pyproject_version="$3"
  mkdir -p "${dir}/.github/scripts"
  cp "${COMPUTE}" "${dir}/.github/scripts/"
  printf '%s' "${version}" > "${dir}/VERSION"
  if [ -n "${pyproject_version}" ]; then
    printf '[project]\nname = "verisim"\nversion = "%s"\n' "${pyproject_version}" \
      > "${dir}/pyproject.toml"
  fi
  echo "${dir}"
}

# run <fixture-dir> <ref> <ref_name>
#   stdout -> ${TMP}/out, stderr -> ${TMP}/err, exit code -> ${TMP}/rc
#
# The exit code goes to a FILE, not a shell variable: every caller needs
# `$(run ...)`'s stdout, and a command substitution is a subshell, so an
# `RC=$?` set in here would be discarded before the caller could read it —
# `RC: unbound variable` under `set -u`. That is not a hypothetical: it is the
# same subshell trap that made verify-release-in-both-registries.sh request
# `.../manifests/` with an always-empty tag (t_ff5a70ec), and the first draft
# of this harness hit it the same way.
#
# EVERY input is pinned here, INCLUDING the ones this harness does not vary.
# `REGISTRY_IMAGE` is the one that bites: it is exported at the WORKFLOW level
# in verisim-grocery.yml, so the grocery test job's pytest process inherits it
# and every child bash does too. Left alone, compute-push-tags.sh would append
# the second registry's refs and three of these cases would fail on tag COUNTS
# (6 tags, not 3) — green locally, red in CI, for a reason that has nothing to
# do with the convention under test. A harness must not read the ambient
# environment; the case 19 below asserts exactly that.
run() {
  local dir="$1" ref="$2" ref_name="$3"
  REPO_ROOT="${dir}" IMAGE_NAME=smiti/verisim-grocery \
    REGISTRY_IMAGE="" REQUIRE_SECOND_REGISTRY=false \
    GITHUB_REF="${ref}" GITHUB_REF_NAME="${ref_name}" GITHUB_SHA="${SHA}" \
    bash "${COMPUTE}" > "${TMP}/out" 2> "${TMP}/err"
  echo $? > "${TMP}/rc"
}

out()   { cat "${TMP}/out"; }
err()   { cat "${TMP}/err"; }
rc()    { cat "${TMP}/rc"; }
field() { grep "^${1}=" "${TMP}/out" | head -1 | cut -d= -f2-; }
has_tag() { echo "$(field tags)" | tr ',' '\n' | grep -qx "$1"; }
# A version tag, not an opaque identifier. This is the card's actual complaint:
# Docker Hub filled up with c6280a084ecc6068…-style tags.
is_version_tag() {
  echo "$1" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$'
}

echo "compute-push-tags.sh — tag convention (t_4b05829c)"
echo

# ── 1. A main push ──────────────────────────────────────────────────────────
d="$(fixture main 1.3.4 1.3.4)"
run "${d}" refs/heads/main main

[ "$(rc)" -eq 0 ] \
  && ok "main push succeeds" \
  || bad "main push succeeds" "$(err)"

tags="$(field tags)"
version_tag="$(echo "${tags}" | tr ',' '\n' | grep -v ':latest$' | grep -v ":${SHA}\$" | head -1 | cut -d: -f2-)"

is_version_tag "${version_tag}" \
  && ok "main push publishes a semver tag (${version_tag})" \
  || bad "main push publishes a semver tag" "the non-latest, non-sha tag is '${version_tag}' out of tags=${tags}"

[ "$(field version)" = "1.3.4" ] \
  && ok "version comes from the VERSION file, not the ref" \
  || bad "version comes from the VERSION file, not the ref" "version=$(field version)"

has_tag "smiti/verisim-grocery:1.3.4" \
  && ok "the tracked version tag is in the push" \
  || bad "the tracked version tag is in the push" "tags=${tags}"

has_tag "smiti/verisim-grocery:latest" \
  && ok "latest is present (switch.sh release pulls it)" \
  || bad "latest is present" "tags=${tags}"

has_tag "smiti/verisim-grocery:${SHA}" \
  && ok "the sha tag is kept for traceability" \
  || bad "the sha tag is kept for traceability" "tags=${tags}"

[ "$(echo "${tags}" | tr ',' '\n' | wc -l)" -eq 3 ] \
  && ok "main pushes exactly 3 tags (latest + version + sha)" \
  || bad "main pushes exactly 3 tags" "tags=${tags}"

[ "$(field release)" = "false" ] \
  && ok "a main push is not a release" \
  || bad "a main push is not a release" "release=$(field release)"

# The card's headline complaint, asserted directly on the emitted list: every
# tag other than latest and the sha must be a version number.
echo "${tags}" | tr ',' '\n' | grep -v ':latest$' | grep -v ":${SHA}\$" \
  | grep -qvE '^[a-z0-9.-]+(/[a-z0-9._-]+)*:[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$' \
  && bad "no tag is an opaque identifier standing in for a version" "tags=${tags}" \
  || ok "no tag is an opaque identifier standing in for a version"

# ── 2. A version tag push ───────────────────────────────────────────────────
d="$(fixture rel 1.3.4 1.3.4)"
run "${d}" refs/tags/v1.3.4 v1.3.4

[ "$(rc)" -eq 0 ] && [ "$(field release)" = "true" ] \
  && ok "a matching v* tag push is a release" \
  || bad "a matching v* tag push is a release" "rc=$(rc) $(err)"

has_tag "smiti/verisim-grocery:v1.3.4" \
  && ok "the historic v-prefixed tag shape still publishes" \
  || bad "the historic v-prefixed tag shape still publishes" "tags=$(field tags)"

[ "$(echo "$(field tags)" | tr ',' '\n' | wc -l)" -eq 4 ] \
  && ok "a release pushes 4 tags (adds its own v* name)" \
  || bad "a release pushes 4 tags" "tags=$(field tags)"

# ── 3. A v* tag that disagrees with VERSION must fail ────────────────────────
# Shipping it would put a version in the registry that the repo does not claim.
d="$(fixture mismatch 1.3.4 1.3.4)"
run "${d}" refs/tags/v9.9.9 v9.9.9
[ "$(rc)" -ne 0 ] && grep -q "VERSION" "${TMP}/err" \
  && ok "a v* tag disagreeing with VERSION fails, naming VERSION" \
  || bad "a v* tag disagreeing with VERSION fails" "rc=$(rc) $(err)"

# ── 4. Bad VERSION values ───────────────────────────────────────────────────
d="$(fixture notnum abcdef 1.3.4)"
run "${d}" refs/heads/main main
[ "$(rc)" -ne 0 ] \
  && ok "a non-numeric VERSION fails" \
  || bad "a non-numeric VERSION fails" "the script published $(out)"

d="$(fixture drift 1.3.4 0.1.0)"
run "${d}" refs/heads/main main
[ "$(rc)" -ne 0 ] && grep -q "pyproject" "${TMP}/err" \
  && ok "VERSION disagreeing with pyproject.toml fails, naming pyproject" \
  || bad "VERSION disagreeing with pyproject.toml fails" "rc=$(rc) $(err)"

d="$(fixture noversion '' '')"
rm -f "${d}/VERSION"
run "${d}" refs/heads/main main
[ "$(rc)" -ne 0 ] && grep -q "VERSION" "${TMP}/err" \
  && ok "a missing VERSION fails loudly (no silent fallback to the SHA)" \
  || bad "a missing VERSION fails loudly" "rc=$(rc) $(err)"

# ── 5. An unpublishable ref ─────────────────────────────────────────────────
d="$(fixture refguard 1.3.4 1.3.4)"
run "${d}" refs/heads/some-feature-branch some-feature-branch
[ "$(rc)" -ne 0 ] \
  && ok "a non-main branch ref is refused" \
  || bad "a non-main branch ref is refused" "the script published $(out)"

# ── 6. The real repo's own files agree ──────────────────────────────────────
if [ -f "${REPO}/VERSION" ]; then
  v="$(tr -d '[:space:]' < "${REPO}/VERSION")"
  is_version_tag "${v}" \
    && ok "the checked-out VERSION is a valid version (${v})" \
    || bad "the checked-out VERSION is a valid version" "VERSION reads '${v}'"
else
  bad "the checked-out VERSION exists" "no VERSION file in ${REPO}"
fi

run "${REPO}" refs/heads/main main
if [ "$(rc)" -eq 0 ]; then
  ok "the real checkout passes the script end to end ($(field tags))"
else
  bad "the real checkout passes the script end to end" "$(err)"
fi

# ── 7. The harness is immune to the ambient environment ─────────────────────
# This case exists because of a real failure, not a hypothetical one. On
# 2026-10-04 the grocery test job went red on 3 of these cases while the same
# commit was green on every developer machine: verisim-grocery.yml exports
# REGISTRY_IMAGE at the WORKFLOW level, so the pytest process inherited it, and
# compute-push-tags.sh — correctly — appended the second registry's refs to a
# publish whose fixture asked for a Hub-only one. The tag COUNTS changed from 3
# to 6 and the count assertions failed.
#
# A harness that passes in one environment and fails in another is not testing
# the convention, it is testing the weather. So: run this same harness with a
# REGISTRY_IMAGE exported, and require it to give the identical answer.
#
# The child gets PUBLISH_TAG_TESTS_CHILD=1 and STOPS at this point. Without that
# guard this case re-runs the whole harness — which re-enters this case, and so
# on until the job times out. (It did, the first time: a 60s timeout, no output.)
if [ -z "${PUBLISH_TAG_TESTS_CHILD:-}" ]; then
  AMBIENT_OUT="$(mktemp)"
  PUBLISH_TAG_TESTS_CHILD=1 REGISTRY_IMAGE='gitea.afastbox.com/admin/verisim-grocery' \
    REQUIRE_SECOND_REGISTRY=true \
    bash "${0}" "${REPO}" > "${AMBIENT_OUT}" 2>&1
  ambient_rc=$?
  ambient_summary="$(grep -E '^publish-tag-tests: ' "${AMBIENT_OUT}" | tail -1)"
  rm -f "${AMBIENT_OUT}"

  # The child's own count is one lower (it skips this case), so compare the
  # OUTCOME and the per-case verdict lines, not the summary string verbatim.
  if [ "${ambient_rc}" -eq 0 ] && ! grep -qE '^  FAIL' "${AMBIENT_OUT}"; then
    ok "the result is identical under an ambient REGISTRY_IMAGE (${ambient_summary})"
  else
    bad "the result is identical under an ambient REGISTRY_IMAGE" \
        "with REGISTRY_IMAGE exported: rc=${ambient_rc} '${ambient_summary}'. This harness must pin every input, or CI fails for reasons unrelated to the convention."
  fi
fi

echo
SUMMARY="publish-tag-tests: ${PASS} passed, ${FAIL} failed"
echo "${SUMMARY}"
[ "${FAIL}" -eq 0 ]