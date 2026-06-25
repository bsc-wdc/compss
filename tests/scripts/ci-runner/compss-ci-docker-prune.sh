#!/bin/bash
# Age-based Docker cleanup for COMPSs CI runners.
#
# Replaces the old per-job "delete every ci image except my pipeline's" prune,
# which deleted concurrently-running pipelines' images and raced their pulls
# ("failed to lease content") on runners with concurrent > 1.
#
# This is concurrency-safe: it only removes resources OLDER than the retention
# window, so a running pipeline's fresh image -- and the content its pull is
# leasing -- is never touched. Run out-of-band on a timer, not inside jobs.
set -euo pipefail

IMAGE_AGE="${IMAGE_AGE:-24h}"         # unused images older than this
CONTAINER_AGE="${CONTAINER_AGE:-2h}"  # stopped containers older than this (clears leaked compss_test_*)
CACHE_AGE="${CACHE_AGE:-48h}"         # build cache older than this (matches the in-job builder window)

echo "[$(date -Is)] compss-ci-docker-prune: containers>$CONTAINER_AGE images>$IMAGE_AGE cache>$CACHE_AGE"
docker container prune -f  --filter "until=$CONTAINER_AGE"
docker image     prune -af --filter "until=$IMAGE_AGE"
docker builder   prune -f  --filter "until=$CACHE_AGE"
echo "[$(date -Is)] done. disk usage:"
docker system df
