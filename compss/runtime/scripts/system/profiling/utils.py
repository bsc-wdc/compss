#!/usr/bin/env python3
#
#  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import os
import sys
import time
import datetime
from multiprocessing import Process, Event
from pathlib import Path


def find_root_cgroup_paths() -> dict:
    """
    Finds the root paths for required cgroup v1 controllers:
    'cpu', 'memory', and 'blkio'.
    """
    base = Path("/sys/fs/cgroup")
    paths = {}

    # List of controllers we *need* for full cgroup profiling
    controllers_to_find = ['cpu', 'memory', 'blkio']

    if not base.is_dir():
        print(f"PROVENANCE | ERROR: cgroup base directory not found: {base}")
        return {} # Return empty dict

    for controller in controllers_to_find:
        controller_path = base / controller

        if controller_path.is_dir():
            paths[controller] = controller_path
        else:
            # Handle common case where cpu and cpuacct are merged
            if controller == 'cpu':
                merged_path = base / 'cpu,cpuacct'
                if merged_path.is_dir():
                    paths['cpu'] = merged_path
                    continue # Found it, move to next controller

            # If we're here, the controller wasn't found
            print(f"PROVENANCE | WARNING: cgroup controller path not found: {controller_path}")
            # We don't add it to the dict, so the check in profiler.py will fail.

    return paths


def get_total_memory_kb():
    """
    Gets total physical RAM from /proc/meminfo.
    """
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    parts = line.split()
                    return int(parts[1]) # Value is already in kB
    except FileNotFoundError:
        print("PROVENANCE | ERROR: Could not read /proc/meminfo to get total memory.")
        return None
    except Exception as e:
        print(f"PROVENANCE | ERROR: Error parsing /proc/meminfo: {e}")
        return None


def get_total_cpu_count():
    """
    Gets total logical CPUs using the built-in 'os' module.
    """
    try:
        # os.cpu_count() is the psutil-free, built-in way
        total_cpus = os.cpu_count()
        if total_cpus is None:
            print("PROVENANCE | ERROR: os.cpu_count() returned None.")
            return None
        return total_cpus
    except Exception as e:
        print(f"PROVENANCE | ERROR: Error calling os.cpu_count(): {e}")
        return None


def read_cgroup_file(path):
    """
    Reads the content of a cgroup file as a string.
    Returns None on failure (e.g., file gone, permission denied).
    """
    try:
        with open(path, "r") as f:
            return f.read().strip()
    except Exception:
        # This can happen if the script is shutting down, so we don't spam errors
        return None

# (This function assumes 'read_cgroup_file' is in the same utils.py file)
def _parse_blkio_file(lines_str: str) -> dict:
    """Helper to parse blkio file content."""
    stats = {'read': 0, 'write': 0}
    if lines_str is None:
        return stats

    for line in lines_str.strip().split('\n'):
        parts = line.split()
        if not parts:
            continue

        # We are looking for lines like '253:0 Read 12345'
        if len(parts) == 3:
            op = parts[1]
            try:
                value = int(parts[2])
                if op == 'Read':
                    stats['read'] += value
                elif op == 'Write':
                    stats['write'] += value
            except (ValueError, IndexError):
                continue # Ignore lines that don't match

    return stats


def get_cgroup_io_stats(blkio_path: Path) -> dict:
    """
    Parses cgroup blkio files for disk bytes and time.

    Reads 'blkio.throttle.io_service_bytes' for bytes.
    Reads 'blkio.throttle.io_service_time' for time (in nanoseconds).

    Returns a dict:
    {
        'read_bytes': <int>,
        'write_bytes': <int>,
        'read_time_ms': <int>,
        'write_time_ms': <int>
    }
    """
    bytes_file = blkio_path / 'blkio.throttle.io_service_bytes'
    time_file = blkio_path / 'blkio.throttle.io_service_time'

    # Get byte stats
    bytes_str = read_cgroup_file(bytes_file)
    byte_stats = _parse_blkio_file(bytes_str)

    # Get time stats (in nanoseconds)
    time_str = read_cgroup_file(time_file)
    time_stats_ns = _parse_blkio_file(time_str)

    # Convert time from nanoseconds to milliseconds to match psutil
    return {
        'read_bytes': byte_stats['read'],
        'write_bytes': byte_stats['write'],
        'read_time_ms': time_stats_ns['read'] // 1_000_000,
        'write_time_ms': time_stats_ns['write'] // 1_000_000
    }
