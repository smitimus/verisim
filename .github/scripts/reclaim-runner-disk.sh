#!/usr/bin/env bash
# reclaim-runner-disk.sh — stop the self-hosted act runner's one 40 G volume from
# filling up between image builds.
#
# WHY (kanban t_66c9f612, 2026-11-15)
# bench/108 (192.168.3.8) is the act runner AND the monitoring pane on ONE 40 G
# root volume. A push to main runs two workflows, each building a ~1 GB image and
# loading it into the host daemon (load: true, t_19eb986e), plus a per-job buildx
# builder that leaves a `buildx_buildkit_*_state` volume behind. Nothing removed
# the loaded images except infra's 3-hourly cron, so a grocery build followed by a
# gas-station build on the SAME push did not fit: the gas-station buildx export
# died ENOSPC twice in one run window —
#
#   run 243 / job 635  #23 ERROR: write .../libarrow_substrait.so.2600: no space left
#   run 243 / job 641  #23 ERROR: write /blobs/sha256/c96992ef…: no space left
#
# — and because the failure landed in the image build, the job never reached its
# smoke test or its API contract suite, and `publish` (needs: integration) was
# skipped every time. A disk-full runner reads as broken code; this script makes it
# read as a disk problem, before the build starts.
#
# WHAT IT DOES
#   before  (top of an image job, BEFORE setup-buildx):
#     * removes `buildx_buildkit_builder-*` containers older than KEEP_YOUNGER_MIN
#       minutes (a live build's builder is younger, and the runner's capacity is 1)
#     * removes the `buildx_buildkit_*_state` volumes those left orphaned
#     * removes `smiti/verisim-*` image tags beyond the newest KEEP_IMAGES per
#       repository, skipping anything a container still references
#     * removes anonymous volumes no container references — a killed smoke test
#       leaves its ~900 MB postgres data dir behind (`docker rm -f` without -v)
#     * prunes dangling images
#     * prints df before/after, then FAILS if the volume is still under FLOOR_GB,
#       with an ::error that names the runner rather than the commit
#   after   (last step of an image job, `if: always()`):
#     * removes the image this job loaded and the builder it created, so the job
#       leaves the runner as it found it
#
# WHAT IT NEVER TOUCHES
#   * `docker.gitea.com/runner-images:*` — the runner config has force_pull: false,
#     so deleting it makes the next job re-pull 1.65 GB or fail outright.
#   * the monitoring pane's containers, images (`monitoring_*`, prom/*, grafana/*,
#     grafana/*, postgres) and named volumes.
#   * any image or volume that a container references.
#   * `docker image prune -a` and `docker system prune` — never run.
#   * the local build cache: the workflows export their cache to the Gitea/GitHub
#     cache (cache-to: type=gha), so there is nothing worth keeping on this volume.
#
# USAGE
#   reclaim-runner-disk.sh before            # first step after checkout/deps
#   reclaim-runner-disk.sh after  IMAGE_REF  # `if: always()`, after the teardown
# Tunables (env, all have safe defaults): RECLAIM_FLOOR_GB, RECLAIM_KEEP_MINUTES,
# RECLAIM_KEEP_IMAGES, RECLAIM_MIN_AGE_MIN.
set -uo pipefail

MODE="${1:-before}"
IMAGE_REF="${2:-}"

FLOOR_GB="${RECLAIM_FLOOR_GB:-5}"
KEEP_MINUTES="${RECLAIM_KEEP_MINUTES:-30}"
KEEP_IMAGES="${RECLAIM_KEEP_IMAGES:-1}"
MIN_AGE_MIN="${RECLAIM_MIN_AGE_MIN:-30}"

STATE="${RUNNER_TEMP:-/tmp}/reclaim-builders-before"
REPORT=()

say() { echo "reclaim-runner-disk[$MODE]: $*"; }

# A hosted runner has no shared volume and no builder state to reclaim: no-op.
if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  say "no reachable docker daemon — nothing to reclaim (hosted runner)"
  exit 0
fi

avail_kb() { df -P / | awk 'NR==2 {print $4}'; }

BEFORE_KB=$(avail_kb)
say "free on / before: $(df -P / | awk 'NR==2 {print $4/1048576 " GiB (" $5 " used)"}')"

# ------------------------------------------------------------------ builders
# Name, oldest first. `before` keeps anything younger than KEEP_MINUTES (a build
# that is just starting); `after` removes only the builders this job added, which
# is the set not present in the snapshot the `before` run wrote.
builders_now() {
  docker ps -a --filter 'name=buildx_buildkit_builder-' --format '{{.Names}}|{{.CreatedAt}}'
}

if [ "$MODE" = before ]; then
  printf '%s\n' "$(builders_now | cut -d'|' -f1)" > "$STATE"
  NOW=$(date -u +%s)
  while IFS='|' read -r name created; do
    [ -n "${name:-}" ] || continue
    # "2026-11-16 03:22:49 +0000 UTC" -> epoch; an unparsable age is treated as
    # young, so a parse failure keeps a builder instead of deleting a live one.
    ts=$(date -u -d "${created%% +0000 UTC}" +%s 2>/dev/null || echo "$NOW")
    age_min=$(( (NOW - ts) / 60 ))
    if [ "$age_min" -lt "$KEEP_MINUTES" ]; then
      say "keeping builder $name (${age_min}m old)"
      continue
    fi
    if docker rm -f "$name" >/dev/null 2>&1; then
      say "removed stale builder $name (${age_min}m old)"; REPORT+=("builder:$name")
    else
      say "could not remove builder $name"; fi
  done <<< "$(builders_now)"
else
  # Ours = the builder that is not in the pre-job snapshot (or every builder, when
  # the snapshot is missing because the `before` step never ran).
  for name in $(builders_now | cut -d'|' -f1); do
    if [ -f "$STATE" ] && grep -qxF "$name" "$STATE"; then
      say "keeping builder $name (present before this job)"
      continue
    fi
    if docker rm -f "$name" >/dev/null 2>&1; then
      say "removed this job's builder $name"; REPORT+=("builder:$name")
    else
      say "could not remove builder $name"; fi
  done
fi

# --------------------------------------------------- orphaned builder state volumes
# `docker volume prune` never reclaims these while the builder container exists,
# and the builder's own post-job cleanup drops the container but not the volume.
for vol in $(docker volume ls -q --filter 'name=buildx_buildkit_'); do
  refs=$(docker ps -a --filter "volume=${vol}" -q 2>/dev/null | wc -l)
  [ "${refs:-1}" -eq 0 ] || continue
  if docker volume rm "$vol" >/dev/null 2>&1; then
    say "removed orphaned builder volume $vol"; REPORT+=("builder-volume:$vol")
  else
    say "could not remove orphaned builder volume $vol"; fi
done

# ------------------------------------------------------------------ images
if [ "$MODE" = before ]; then
  # Newest KEEP_IMAGES per repository are kept; so is anything younger than
  # MIN_AGE_MIN (a sibling job's fresh image) and anything a container references.
  TO_DELETE=$(docker images --format '{{.Repository}}|{{.Tag}}|{{.CreatedAt}}' \
    | grep -E '^smiti/verisim-' \
    | awk -F'|' -v keep="$KEEP_IMAGES" \
          -v cutoff="$(date -u -d "-${MIN_AGE_MIN} minutes" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo '9999-12-31 00:00:00')" '
        { repo=$1; tag=$2; created=$3;
          if (created > cutoff) next;              # too fresh to touch
          n[repo]++; if (n[repo] > keep) print repo ":" tag }')
  for tag in $TO_DELETE; do
    if [ "$(docker ps -a --filter "ancestor=${tag}" -q 2>/dev/null | wc -l)" -gt 0 ]; then
      say "keeping image $tag (a container references it)"
      continue
    fi
    if docker rmi "$tag" >/dev/null 2>&1; then
      say "removed image $tag"; REPORT+=("image:$tag")
    else
      say "could not remove image $tag"; fi
  done
elif [ -n "$IMAGE_REF" ]; then
  if [ "$(docker ps -a --filter "ancestor=${IMAGE_REF}" -q 2>/dev/null | wc -l)" -gt 0 ]; then
    say "keeping image $IMAGE_REF (a container still references it)"
  elif docker rmi "$IMAGE_REF" >/dev/null 2>&1; then
    say "removed the image this job loaded: $IMAGE_REF"; REPORT+=("image:$IMAGE_REF")
  else
    say "image $IMAGE_REF is already gone or was not loaded locally"
  fi
fi

# ---------------------------------------------------------- anonymous volumes
# A smoke test runs with `-v /var/lib/postgresql/data` (anonymous) so the data dir
# dies with `--rm`; when the job is torn down with `docker rm -f` instead, that
# ~900 MB volume survives with no container pointing at it. Named volumes
# (monitoring_*, GITEA-ACTIONS-*) are never candidates — only docker's 64-hex
# anonymous names are.
if [ "$MODE" = before ]; then
  for vol in $(docker volume ls -q); do
    [[ "$vol" =~ ^[0-9a-f]{64}$ ]] || continue
    refs=$(docker ps -a --filter "volume=${vol}" -q 2>/dev/null | wc -l)
    [ "${refs:-1}" -eq 0 ] || continue
    if docker volume rm "$vol" >/dev/null 2>&1; then
      say "removed orphaned anonymous volume $vol"; REPORT+=("anonymous-volume:$vol")
    else
      say "could not remove orphaned anonymous volume $vol"; fi
  done

  # Dangling images only (never -a): the runner's base images and the monitoring
  # stack's images are referenced, so this removes only superseded intermediates.
  OUT=$(docker image prune -f 2>&1)
  RECLAIMED=$(printf '%s\n' "$OUT" | sed -n 's/^Total reclaimed space: //p')
  [ -n "$RECLAIMED" ] && REPORT+=("dangling-images:$RECLAIMED")
fi

# ------------------------------------------------------------------ the floor
AFTER_KB=$(avail_kb)
FREED_MB=$(( (AFTER_KB - BEFORE_KB) / 1024 ))
FREE_GB=$(( AFTER_KB / 1048576 ))
say "free on / after: $(( AFTER_KB / 1048576 )) GiB (${FREE_GB} GiB) — freed ${FREED_MB} MiB"
if [ "${#REPORT[@]}" -gt 0 ]; then
  say "removed ${#REPORT[@]} item(s): ${REPORT[*]}"
fi

if [ "$MODE" = before ] && [ "$FREE_GB" -lt "$FLOOR_GB" ]; then
  # Fail HERE, with a message that blames the runner, rather than 30 s later inside
  # the buildx export with `no space left on device` — which reads as a code fault
  # and cost two runs (and one skipped publish) on 2026-11-15.
  echo "::error title=CI runner out of disk::bench/108 has ${FREE_GB} GiB free on its 40 G volume after reclaiming everything CI rebuilds (floor ${FLOOR_GB} GiB). This is a RUNNER condition, not a fault in this commit: the image build will not fit. Free space on 192.168.3.8 (infra: scripts/ci-runner-prune.sh --aggressive --force) or raise RECLAIM_FLOOR_GB after checking what else lives there."
  exit 1
fi
exit 0
