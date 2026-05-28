#!/bin/bash
# Polls until the given TCP port is no longer in LISTEN state.
# Returns 1 and prints an error if the port is still held after 30 seconds.
wait_for_port_release() {
  local port=$1
  local retries=30
  while ss -tlnp 2>/dev/null | grep -qE ":${port}[^0-9]" && [ "${retries}" -gt 0 ]; do
    sleep 1
    retries=$((retries - 1))
  done
  if ss -tlnp 2>/dev/null | grep -qE ":${port}[^0-9]"; then
    echo "[ERROR] Port ${port} still in use after 30s timeout" >&2
    return 1
  fi
}
