#!/usr/bin/env bash
# reclaim-runner-disk-tests.sh — prove the reclaim script deletes the right things
# and nothing else.
#
# WHY (kanban t_66c9f612)
# This script deletes docker objects on a SHARED host (bench/108 runs the act
# runner and the monitoring pane on one 40 G volume). An over-eager rule deletes
# `docker.gitea.com/runner-images:ubuntu-latest` (which `force_pull: false` makes
# load-bearing), a monitoring volume, or a live build's builder — and the next job
# fails for a new and more confusing reason. A pruning script nobody has watched
# refuse is a pruning script that will one day take the wrong thing, so every rule
# gets a case that must REFUSE as well as a case that must delete.
#
# It runs offline: a stub `docker` and `df` stand in for the daemon and the
# filesystem, and every assertion reads the stub's call log.
#
# Run: bash .github/scripts/reclaim-runner-disk-tests.sh
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/reclaim-runner-disk.sh"
ROOT=$(mktemp -d)
trap 'rm -rf "$ROOT"' EXIT

FAILS=0
ok()   { printf '  ok   %s\n' "$*"; }
bad()  { printf '  FAIL %s\n' "$*"; FAILS=$((FAILS + 1)); }
check_contains()     { if grep -qxF -- "$2" "$1"; then ok "$3"; else bad "$3 (call log lacks: $2)"; fi; }
check_not_contains() { if grep -qxF -- "$2" "$1"; then bad "$3 (call log HAS: $2)"; else ok "$3"; fi; }
check_grep()         { if grep -qE -- "$2" "$1"; then ok "$3"; else bad "$3 (no line matching: $2)"; fi; }

# ---------------------------------------------------------------- the stub docker
# State lives in $FAKE as plain files; every mutating call appends to $FAKE/calls.
make_stub() {
  local d="$1"; mkdir -p "$d/bin"
  cat > "$d/bin/df" <<'EOF'
#!/usr/bin/env bash
# df -P /  -> header + one line, avail (KB) read from $FAKE/avail_kb
cat <<OUT
Filesystem     1024-blocks    Used Available Capacity Mounted on
/dev/mapper/x    41943040 10000000 $(cat "$FAKE/avail_kb") 50% /
OUT
EOF
  cat > "$d/bin/docker" <<'EOF'
#!/usr/bin/env bash
# Record every call, answer from $FAKE/*, and record every mutation.
echo "docker $*" >> "$FAKE/calls"
cmd="${1:-}"; shift || true
case "$cmd" in
  info) exit 0 ;;
  ps)
    filter=""; fmt=""
    while [ $# -gt 0 ]; do
      case "$1" in
        -a) shift ;;
        --filter) filter="$2"; shift 2 ;;
        --format) fmt="$2"; shift 2 ;;
        -q) fmt="id"; shift ;;
        *) shift ;;
      esac
    done
    case "$filter" in
      name=buildx_buildkit_builder-*) cat "$FAKE/builders" 2>/dev/null ;;
      volume=*)  # a volume reference: answer iff the volume is listed in vol_refs
        v="${filter#volume=}"
        grep -qxF -- "$v" "$FAKE/vol_refs" 2>/dev/null && echo "container-id" ;;
      ancestor=*)  # an image reference: answer iff listed in image_refs
        t="${filter#ancestor=}"
        grep -qxF -- "$t" "$FAKE/image_refs" 2>/dev/null && echo "container-id" ;;
    esac
    exit 0 ;;
  rm)    echo "docker $cmd $*" >> "$FAKE/mutations"; exit 0 ;;
  rmi)   echo "docker $cmd $*" >> "$FAKE/mutations"; exit 0 ;;
  images)
    # honour the --format shape the script asks for: repo|tag|created
    cat "$FAKE/images" 2>/dev/null ;;
  image)
    sub="${1:-}"; shift || true
    if [ "$sub" = prune ]; then
      echo "docker image prune $*" >> "$FAKE/mutations"
      echo "Total reclaimed space: 12.3MB"
      exit 0
    fi
    exit 0 ;;
  volume)
    sub="${1:-}"; shift || true
    case "$sub" in
      ls)
        # `-q` only; with a name filter answer just the buildx volumes.
        if printf '%s\n' "$*" | grep -q 'name=buildx_buildkit_'; then
          cat "$FAKE/buildx_vols" 2>/dev/null
        else
          cat "$FAKE/anon_vols" 2>/dev/null; cat "$FAKE/buildx_vols" 2>/dev/null
        fi
        exit 0 ;;
      rm) echo "docker volume rm $*" >> "$FAKE/mutations"; exit 0 ;;
    esac
    exit 0 ;;
esac
exit 0
EOF
  chmod +x "$d/bin/df" "$d/bin/docker"
}

# A fresh, fully-populated fake host.
new_fake() {
  local d="$1"; rm -rf "$d"; make_stub "$d"
  : > "$d/builders"; : > "$d/buildx_vols"; : > "$d/anon_vols"
  : > "$d/vol_refs"; : > "$d/image_refs"; : > "$d/images"
  : > "$d/calls"; : > "$d/mutations"
  echo 20971520 > "$d/avail_kb"   # 20 GiB free: above the default floor
  mkdir -p "$d/tmp"
}

run_script() {  # new_fake-dir mode [image]
  local d="$1"; shift
  FAKE="$d" RUNNER_TEMP="$d/tmp" PATH="$d/bin:$PATH" \
    RECLAIM_FLOOR_GB="${RECLAIM_FLOOR_GB:-5}" \
    bash "$SCRIPT" "$@"
}

old_ts() { date -u -d '-5 hours' '+%Y-%m-%d %H:%M:%S +0000 UTC'; }
young_ts() { date -u -d '-2 minutes' '+%Y-%m-%d %H:%M:%S +0000 UTC'; }

echo "reclaim-runner-disk-tests"

# ── 1. builders: stale removed, a young (possibly live) one kept ────────────────
F="$ROOT/f1"; new_fake "$F"
printf 'buildx_buildkit_builder-old|%s\nbuildx_buildkit_builder-new|%s\n' \
  "$(old_ts)" "$(young_ts)" > "$F/builders"
run_script "$F" before >/dev/null 2>&1
check_contains     "$F/mutations" "docker rm -f buildx_buildkit_builder-old" "stale builder removed"
check_not_contains "$F/mutations" "docker rm -f buildx_buildkit_builder-new" "builder younger than KEEP_MINUTES kept"

# ── 2. buildx state volumes: orphan removed, referenced one kept ───────────────
F="$ROOT/f2"; new_fake "$F"
printf 'buildx_buildkit_a_state\nbuildx_buildkit_b_state\n' > "$F/buildx_vols"
printf 'buildx_buildkit_b_state\n' > "$F/vol_refs"
run_script "$F" before >/dev/null 2>&1
check_contains     "$F/mutations" "docker volume rm buildx_buildkit_a_state" "orphaned builder state volume removed"
check_not_contains "$F/mutations" "docker volume rm buildx_buildkit_b_state" "builder state volume with a container kept"

# ── 3. images: newest per repo kept, older tags removed, referenced one kept ───
F="$ROOT/f3"; new_fake "$F"
NOW_CUT=$(( $(date -u +%s) - 3600 ))
t1=$(date -u -d "@$((NOW_CUT - 7200))" '+%Y-%m-%d %H:%M:%S')   # oldest
t2=$(date -u -d "@$((NOW_CUT - 3600))" '+%Y-%m-%d %H:%M:%S')
t3=$(date -u -d "@$NOW_CUT" '+%Y-%m-%d %H:%M:%S')              # newest
{
  echo "smiti/verisim-grocery|sha-new|$t3|i3"
  echo "smiti/verisim-grocery|sha-mid|$t2|i2"
  echo "smiti/verisim-grocery|sha-old|$t1|i1"
  echo "smiti/verisim-gas-station|sha-gas|$t2|i4"
} > "$F/images"
printf 'smiti/verisim-grocery:sha-mid\n' > "$F/image_refs"
run_script "$F" before >/dev/null 2>&1
check_not_contains "$F/mutations" "docker rmi smiti/verisim-grocery:sha-new"  "newest image per repo kept"
check_not_contains "$F/mutations" "docker rmi smiti/verisim-grocery:sha-mid"  "image a container references kept"
check_contains     "$F/mutations" "docker rmi smiti/verisim-grocery:sha-old"  "older image tag removed"
check_not_contains "$F/mutations" "docker rmi smiti/verisim-gas-station:sha-gas" "the one image of the other repo kept"

# ── 4. anonymous volumes: orphan removed, referenced and NAMED ones kept ───────
F="$ROOT/f4"; new_fake "$F"
A=$(printf 'a%.0s' $(seq 1 64)); B=$(printf 'b%.0s' $(seq 1 64))
printf '%s\n%s\nmonitoring_prometheus-data\n' "$A" "$B" > "$F/anon_vols"
printf '%s\n' "$B" > "$F/vol_refs"
run_script "$F" before >/dev/null 2>&1
check_contains     "$F/mutations" "docker volume rm $A" "orphaned anonymous volume removed"
check_not_contains "$F/mutations" "docker volume rm $B" "anonymous volume with a container kept"
check_not_contains "$F/mutations" "docker volume rm monitoring_prometheus-data" "named monitoring volume never touched"

# ── 5. the never-list: no blanket prune, runner image never removed ────────────
check_not_contains "$F/mutations" "docker image prune -a -f" "no blanket 'image prune -a'"
check_not_contains "$F/mutations" "docker image prune -af"   "no blanket 'image prune -af'"
check_not_contains "$F/mutations" "docker system prune -f"   "no 'system prune'"
check_grep        "$F/mutations" '^docker image prune -f$'   "dangling-only image prune runs"
check_not_contains "$F/mutations" "docker.gitea.com/runner-images" "runner image never removed"
check_not_contains "$F/mutations" "monitoring"                    "monitoring stack never removed"

# ── 6. after-mode: only this job's builder, plus the image it loaded ──────────
F="$ROOT/f6"; new_fake "$F"
printf 'buildx_buildkit_builder-preexisting|%s\n' "$(old_ts)" > "$F/builders"
run_script "$F" before >/dev/null 2>&1            # writes the pre-job snapshot
: > "$F/mutations"
printf 'buildx_buildkit_builder-preexisting|%s\nbuildx_buildkit_builder-ours|%s\n' \
  "$(old_ts)" "$(young_ts)" > "$F/builders"
run_script "$F" after "smiti/verisim-grocery:deadbeef" >/dev/null 2>&1
check_contains     "$F/mutations" "docker rm -f buildx_buildkit_builder-ours"          "after: this job's builder removed"
check_not_contains "$F/mutations" "docker rm -f buildx_buildkit_builder-preexisting"   "after: a builder older than the job is untouched"
check_contains     "$F/mutations" "docker rmi smiti/verisim-grocery:deadbeef"          "after: the loaded image removed"

# ── 7. after-mode keeps the image while a container still runs it ─────────────
F="$ROOT/f7"; new_fake "$F"
printf 'smiti/verisim-gas-station:sha\n' > "$F/image_refs"
run_script "$F" after "smiti/verisim-gas-station:sha" >/dev/null 2>&1
check_not_contains "$F/mutations" "docker rmi smiti/verisim-gas-station:sha" "after: image kept while a container references it"

# ── 8. the floor: below it the job fails as a RUNNER condition ────────────────
F="$ROOT/f8"; new_fake "$F"
echo 2097152 > "$F/avail_kb"      # 2 GiB free
OUT=$(run_script "$F" before 2>&1); rc=$?
if [ "$rc" -eq 1 ]; then ok "below the floor exits 1"; else bad "below the floor must exit 1 (got $rc)"; fi
case "$OUT" in
  *"::error title=CI runner out of disk"*) ok "the failure names the runner, not the commit" ;;
  *) bad "no ::error annotation naming the runner" ;;
esac
F="$ROOT/f9"; new_fake "$F"
OUT=$(run_script "$F" before 2>&1); rc=$?
if [ "$rc" -eq 0 ]; then ok "above the floor exits 0"; else bad "above the floor must exit 0 (got $rc): $OUT"; fi

# ── 9. no docker at all (a hosted runner) is a no-op, not a failure ───────────
F="$ROOT/f10"; new_fake "$F"
rm -f "$F/bin/docker"
PATH="$F/bin:/usr/bin:/bin" bash "$SCRIPT" before >/dev/null 2>&1
rc=$?
if [ "$rc" -eq 0 ]; then ok "no docker daemon is a no-op"; else bad "missing docker must not fail the job (got $rc)"; fi

echo
if [ "$FAILS" -ne 0 ]; then
  echo "reclaim-runner-disk-tests: FAILED ($FAILS assertion(s))"
  exit 1
fi
echo "reclaim-runner-disk-tests: PASSED"
