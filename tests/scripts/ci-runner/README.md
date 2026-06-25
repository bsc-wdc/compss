# CI runner host provisioning

Host-side helpers for the GitLab CI runners. These are **not** run by the
pipeline; they are installed on the runner hosts.

## `compss-ci-docker-prune` — age-based Docker cleanup

Frees disk on the runners by removing Docker resources **older than a retention
window**. It replaces the old per-job prune (`prune_old_ci_images` in
`tests/.gitlab-ci.yml`), which deleted other concurrently-running pipelines'
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
