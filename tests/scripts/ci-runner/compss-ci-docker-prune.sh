#!/bin/bash
# Docker cleanup for COMPSs CI runners. Run out-of-band on a timer, not inside jobs.
#
# 1. Kill processes left behind by GitLab jobs that have already ended.
# 2. Restart dockerd if it still counts builds as running while no client is
#    left to own them, and the host is otherwise idle.
# 3. Remove Docker resources OLDER than a retention window.
#
# Step 3 replaces the old per-job "delete every ci image except my pipeline's"
# prune, which deleted concurrently-running pipelines' images and raced their
# pulls ("failed to lease content") on runners with concurrent > 1. It is
# concurrency-safe: it only removes resources older than the retention window,
# so a running pipeline's fresh image -- and the content its pull is leasing --
# is never touched.
set -euo pipefail

IMAGE_AGE="${IMAGE_AGE:-24h}"         # unused images older than this
CONTAINER_AGE="${CONTAINER_AGE:-2h}"  # stopped containers older than this (clears leaked compss_test_*)
CACHE_AGE="${CACHE_AGE:-48h}"         # build cache older than this (matches the in-job builder window)
RUNNER_USER="${RUNNER_USER:-gitlab-runner}"  # user the shell executor runs jobs as
JOURNAL_SINCE="${JOURNAL_SINCE:-60 days ago}" # how far back to look for ended jobs

log() { echo "[$(date -Is)] $*"; }

# Print "<pid> <CI_JOB_ID>" for every process of a GitLab job.
job_processes() {
    local pid job
    for pid in $(pgrep -u "$RUNNER_USER" || true); do
        job=$(tr '\0' '\n' 2>/dev/null <"/proc/$pid/environ" | sed -n 's/^CI_JOB_ID=//p' || true)
        if [ -n "$job" ]; then
            echo "$pid $job"
        fi
    done
}

# True if a job, a container or a docker client is running.
host_busy() {
    [ -n "$(job_processes)" ] ||
        [ -n "$(docker ps -q)" ] ||
        pgrep '^docker(-buildx|-compose)?$' >/dev/null
}

log "compss-ci-docker-prune: containers>$CONTAINER_AGE images>$IMAGE_AGE cache>$CACHE_AGE"

# --- 1. Processes of ended jobs ---------------------------------------------
# On a job timeout or cancel, the shell executor kills only the PID it tracks.
# That PID has often exited already ("Failed to force-kill: no such process"),
# so the job's children (e.g. `docker bake`) keep running for days.
procs=$(job_processes)
if [ -n "$procs" ]; then
    # The runner logs with colour codes ("job\e[0;m=123"), so strip them first.
    ended=$(journalctl -u gitlab-runner --no-pager -o cat --since "$JOURNAL_SINCE" -g "Removed job from processing list" |
        sed 's/\x1b\[[0-9;]*m//g' | grep -oE ' job=[0-9]+' | cut -d= -f2 | sort -u || true)
    leaked=$(awk 'NR == FNR { ended[$1]; next } ($2 in ended)' <(echo "$ended") <(echo "$procs"))
    if [ -n "$leaked" ]; then
        pids=$(echo "$leaked" | cut -d' ' -f1)
        log "killing $(echo "$pids" | wc -l) process(es) of ended jobs: $(echo "$leaked" | cut -d' ' -f2 | sort -u | xargs)"
        kill -TERM $pids 2>/dev/null || true
        sleep 10
        kill -KILL $pids 2>/dev/null || true
    fi
fi

# --- 2. Builds dockerd still counts as running --------------------------------
# Killing a `docker bake` client does not end its build inside dockerd. The
# build stays "Running" and pins its build cache, so step 3 frees 0 B. Only a
# dockerd restart ends it. A real build always has a live docker client, so a
# running build on an idle host has no owner.
running=$({ docker buildx history ls --builder default --filter status=running --format '{{.Ref}}' 2>/dev/null || true; } | wc -l)
if [ "$running" -gt 0 ]; then
    if host_busy; then
        log "dockerd reports $running running build(s); host busy, not restarting"
    else
        log "dockerd reports $running running build(s) with no client; restarting docker"
        systemctl restart docker
    fi
fi

# --- 3. Age-based prune ------------------------------------------------------
docker container prune -f  --filter "until=$CONTAINER_AGE"
docker image     prune -af --filter "until=$IMAGE_AGE"
docker builder   prune -f  --filter "until=$CACHE_AGE"
log "done. disk usage:"
docker system df
