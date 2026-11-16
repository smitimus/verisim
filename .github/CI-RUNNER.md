# The self-hosted act runner, and the two things that used to make CI lie

This is the runbook for the CI runner both workflows execute on. It exists because
two failures of the *runner* were repeatedly read as failures of the *code*.

Host: **bench / 108, `192.168.3.8`** — a **40 GB** root volume, shared with a
monitoring stack. The runner is [act](https://github.com/nektos/act) driven by
Gitea's `act_runner`, and **the job runs inside a container** with the host's
`/var/run/docker.sock` bind-mounted into it. That single fact is the cause of the
first failure below.

`AGENTS.md` §"Test Coverage (CI gating)" is the summary; this file is the detail.
(It lives here rather than in `AGENTS.md` because that file is agent-protected and
needs a human-approved edit. If you are a human reading this and the two files
disagree, `AGENTS.md` is the one that is stale — update it from here, t_19eb986e.)

## 1. The runner can run out of disk, and then *your* tests fail

At 98 % full (976 MB free) every `Generator unit tests + coverage gate` job on this
runner died with `OSError: [Errno 28] No space left on device` — a red gate that had
nothing to do with the commit under test (t_cbb3b8dc). What filled it: every push
built a ~1 GB grocery image and a ~1 GB gas-station image tagged by commit SHA, plus
a per-run `buildx_buildkit_builder-<uuid>` container and its multi-GB `_state`
volume. Nothing pruned any of it for six weeks.

Infra now prunes the runner every 3 hours (`admin/infra`
`scripts/ci-runner-prune.sh`: keeps the newest image per repo, drops anything older
than 45 minutes, never touches `runner-images`, no-ops during a live job) and the host
is on the disk watchdog at 80 %. That is the safety net, not the fix.

The fix is in the workflows:

* **`integration` runs on pushes only — `main` and `v*` tags, never a pull request.**
  A card branch typically pushes several times per PR, and each of those runs paid
  ~1 GB of image plus multi-GB builder state on the volume the blocking `test` job
  needs. `publish` builds its own image anyway, so a PR build produced an artifact
  nobody consumed. Gitea PRs are squash-merged, so every merge still runs the image
  on `main`, and `publish` needs both `test` and `integration` — a broken image
  cannot be released. What is given up is pre-merge feedback on image-level
  breakage; it now appears as a red `main` minutes after the merge.
* **`load: true` instead of `outputs: type=docker,dest=/tmp/image.tar` + `docker load`.**
  The tar was a second full copy of the image, inside the job container's writable
  layer on the runner's root. buildx's docker exporter loads straight into the daemon.
* **The PG data dir is an anonymous volume** (`-v /var/lib/postgresql/data`), so
  `--rm` deletes it with the container. The old bind mount to `/tmp/verisim_data`
  survived the job and left a half-initialised data dir (and hundreds of MB) behind
  for the next run to trip over.

A second runner, or a separate buildx cache, was considered and rejected: there is one
volume, so a second cache directory on it is the same 40 GB, and a second runner is a
new host to keep rather than disk saved.

## 2. `localhost` inside the job is the job, not the published container

The `Smoke test` step starts the image with `docker run -d -p 8010:8000 …` and then
curls the API. On a GitHub-hosted runner that is correct: the job runs on the VM, so
the publish lands on the same loopback the step talks to.

Here it is not. The job runs **in a container on its own per-job bridge network**
(the cleanup line in a job log names it:
`GITEA-ACTIONS-TASK-<n>-WORKFLOW-…-JOB-…-network`), and it drives the **host** daemon
through the mounted socket. So `-p 8010:8000` publishes on **bench's** loopback, and
`localhost:8010` from inside the job is the job container itself, where nothing
listens.

Measured 2026-11-15 on a job-shaped container, against a sibling actually serving:

| probe from the job container | result |
|---|---|
| `localhost:<port>` (published sibling) | **connection refused**, every time |
| `<bridge gateway>:<port>` (published sibling) | 200 |
| sibling on the job network, by name or by IP | 200 |
| sibling with `--network container:<job>` + `127.0.0.1:8000` | 200 |

The symptom was a red `Smoke test` that burned its whole 180 s budget while
supervisord reported all four processes RUNNING — and `Build image` / `Load image`
had already succeeded. The image was fine; the address was wrong. (Note: a note
saying this job was "permanently red at Build image / Load image" was itself a
by-product of the disk-full runner — both steps succeed.)

### What the smoke step does now

1. Work out whether the job is itself a container: `docker inspect` on `/etc/hostname`,
   `$HOSTNAME`, and the container id in `/proc/self/mountinfo` — first one that
   resolves wins.
2. **Container:** run the image with `--network container:<self>`, so it shares the
   job's network namespace. The API is `http://127.0.0.1:8000`; nothing is published,
   so no host firewall rule and no DNS can get in the way.
   **Host:** publish as before, and also probe the default gateway and
   `host.docker.internal`, because that is the shape of a future runner that defeats
   detection.
3. Whichever candidate answers 200 is exported for the rest of the job:
   `VERISIM_API_BASE_URL` (grocery) / `VERISIM_GAS_API_BASE_URL` (gas-station, the
   name its `api/tests/conftest.py` reads). The backfill wait and the API contract
   suite read that variable. **Do not hard-code an address in a later step** — the
   old `localhost:8010` in the backfill wait would have failed one step after the
   smoke test was fixed.

`tools/check_workflows.py` fails the build on all three regressions: `integration`
without an `if:` naming `pull_request`, an integration step whose `run:` contains
`http://localhost:`, and `outputs: type=docker,dest=…`. The negative cases live in
`gas-station/generator/tests/test_build_checks.py`, so a checker that stops checking
is itself a red test.
