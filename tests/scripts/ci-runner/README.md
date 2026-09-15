# CI runner host provisioning

Host-side helpers for the GitLab CI runners. These are **not** run by the
pipeline; they are installed on the runner hosts.

## `compss-ci-docker-prune` — Docker cleanup

Frees disk on the runners in three steps.

### 1. Processes of ended jobs

When a job times out or is cancelled, the shell executor kills only the PID it
tracks. That PID has often exited already, and the runner log says
`Failed to force-kill: no such process`. The job's children keep running. On
the builder, the `deploy` job's `docker bake` leaked this way for up to 10 days.

The script finds every process of the `gitlab-runner` user that has a
`CI_JOB_ID` in its environment. If `journalctl -u gitlab-runner` shows
`Removed job from processing list` for that job, the script kills the process.

### 2. Builds dockerd still counts as running

Killing a leaked `docker bake` does not end its build inside dockerd. The build
stays `Running` in `docker buildx history ls` and pins its build cache, so the
prune frees 0 B. Only a dockerd restart ends it.

The script restarts docker only when dockerd reports a running build **and** the
host is idle: no GitLab job process, no running container, and no `docker`,
`docker-buildx` or `docker-compose` client. A real build always has a client,
so such a build has no owner. The unit uses `Wants=docker.service`, not
`Requires=`, so the restart does not stop the script.

### 3. Age-based prune

Removes Docker resources **older than a retention window**. It replaces the old
per-job prune (`prune_old_ci_images` in `tests/.gitlab-ci.yml`), which deleted other concurrently-running pipelines'
images and raced their pulls (`failed to lease content: NotFound`) on runners
with `concurrent > 1`.

Because every filter is age-based, a running pipeline's fresh image — and the
content its `docker pull` is leasing — is never touched, so it is safe at any
concurrency. It also clears leaked `compss_test_*` containers.

Windows (override via `Environment=` in the unit):

| Variable        | Default | Removes                                   |
|-----------------|---------|-------------------------------------------|
| `CONTAINER_AGE` | `2h`    | stopped containers older than this        |
| `IMAGE_AGE`     | `24h`   | unused images older than this             |
| `CACHE_AGE`     | `48h`   | build cache older than this               |

On a builder runner set `IMAGE_AGE=72h` so base images stay cache-warm
(uncomment the `Environment=` line in the `.service`).

### Install

```bash
sudo install -m 755 compss-ci-docker-prune.sh      /usr/local/bin/
sudo install -m 644 compss-ci-docker-prune.service /etc/systemd/system/
sudo install -m 644 compss-ci-docker-prune.timer   /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now compss-ci-docker-prune.timer
sudo systemctl start compss-ci-docker-prune.service   # run once to verify
```

### Inspect

```bash
systemctl list-timers compss-ci-docker-prune.timer
journalctl -u compss-ci-docker-prune
```
