#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
import subprocess, os, sys
import time
import socket
from datetime import datetime
import signal
from pathlib import Path
import json

try:
    import psutil

    psutil_imported = True
except ImportError:
    print(
        "PROVENANCE | ERROR: psutil is not installed. Install it, if you want to monitor all the resources status during the execution."
    )
    psutil_imported = False

from utils import (
    find_root_cgroup_paths,
    get_total_cpu_count,
    get_total_memory_kb,
    read_cgroup_file,
    get_cgroup_io_stats
)

LIST_PROFILER = [
    "psutil",
    "top",
    "cgroup"
]

# Flag to control the profiling loop
profiling_active = True
# Global variables to store profiling data and file handle
profiling_data = []
output_file = None
log_dir = None
hostname = None

def end_profiling(sig, frame):
    """
    Signal handler.
    Sets the global flag to stop the profiling loop.
    This function should do the absolute minimum work possible.
    """
    global profiling_active
    if not profiling_active:
        return  # Prevent multiple invocations

    profiling_active = False
    print("PROVENANCE | Finishing profiling (signal received)...")
    # All file I/O and cleanup is now handled in the main() function's
    # finally block and post-loop logic.


def setup_signal_handlers():
    """Setup handlers for various termination signals"""
    signals_to_handle = [
        signal.SIGTERM,  # Termination signal
        signal.SIGINT,   # Ctrl+C
        signal.SIGHUP,   # Hangup
        signal.SIGQUIT,  # Quit
        signal.SIGUSR1,  # User-defined signal 1
    ]

    for sig in signals_to_handle:
        signal.signal(sig, end_profiling)


def get_cpu_top() -> list:
    """
    Execute the command to get the values of cpu and memory usage, by calling a bash command

    :return: list containing the as first value the cpu percentage and second value the memory percentage
    """
    COMMAND = "export LC_NUMERIC=C && top -b -n 1 | awk '/^%Cpu/ { cpu_usage = 100 - $8; cpu_usage = cpu_usage * 2; cpu_usage = (cpu_usage > 100) ? 100.0 : cpu_usage } /^MiB Mem/ { mem_usage = ($8 / $4) * 100 } END { printf \"%.2f,%.2f,\", cpu_usage, mem_usage }'"

    result = subprocess.check_output(COMMAND, shell=True, text=True).strip().split(",")
    return result


def profiling_function(
        byte_read: int,
        byte_write: int,
        time_read: int,
        time_write: int,
        prev_bytes_sent: int,
        prev_bytes_recv: int,
        config: str,
        interval: int,
) -> tuple:
    """
    Function to profile and monitor system resource usage, including CPU, memory, and network I/O.

    :param byte_read: The total number of bytes read during the profiling period.
    :param byte_write: The total number of bytes written during the profiling period.
    :param time_read: The time taken (in seconds) for read operations.
    :param time_write: The time taken (in seconds) for write operations.
    :param prev_bytes_sent: The total number of bytes sent before this profiling period.
    :param prev_bytes_recv: The total number of bytes received before this profiling period.
    :param system_type: Type of the system where the application is executed
    :param interval: Interval to use between every measurement

    :return: A tuple containing three elements:
        - A formatted string with the following comma-separated values:
            * CPU usage (average percentage)
            * Memory usage (percentage)
            * Bytes sent during the interval
            * Bytes received during the interval
            * Bytes read during the interval
            * Bytes written during the interval
            * Time taken for read operations
            * Time taken for write operations
            * Timestamp of the profiling event
        - The updated total number of bytes sent.
        - The updated total number of bytes received.
    """
    if config == LIST_PROFILER[0]: # psutil
        logical_processors = psutil.cpu_count(logical=True)
        physical_cores = psutil.cpu_count(logical=False)
        multiplication_factor = float(round(logical_processors / physical_cores, 2))
        cpu = psutil.cpu_percent(interval=interval) * multiplication_factor
        cpu = cpu if cpu < 100 else 100
        mem = psutil.virtual_memory().percent
        net = psutil.net_io_counters()
        ref_byte_sent = net.bytes_sent
        ref_byte_recv = net.bytes_recv
        byte_sent = ref_byte_sent - prev_bytes_sent
        byte_recv = ref_byte_recv - prev_bytes_recv
    else: # top
        cpu_mem = get_cpu_top()
        cpu = cpu_mem[0]
        mem = cpu_mem[1]
        ref_byte_sent = 0
        ref_byte_recv = 0
        byte_sent = None
        byte_recv = None

    new_entry = f"{cpu},{mem},{byte_sent},{byte_recv},{byte_read},{byte_write},{time_read},{time_write},{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    return new_entry, ref_byte_sent, ref_byte_recv


def get_config(machine, config_file_path):
    try:
        with Path(config_file_path).open("r") as f:
            config_data = json.load(f)
    except FileNotFoundError:
        print(f"PROVENANCE | ERROR: Config file not found at {config_file_path}")
        return None
    except json.JSONDecodeError:
        print(f"PROVENANCE | ERROR: Config file is not valid JSON: {config_file_path}")
        return None
    except Exception as e:
        print(f"PROVENANCE | ERROR: Could not read config file: {e}")
        return None

    profiler_tool = None
    if psutil_imported:
        if machine in config_data.get('psutil', []):
            profiler_tool = LIST_PROFILER[0]  # psutil
        elif machine in config_data.get('top', []):
            profiler_tool = LIST_PROFILER[1]  # top
        elif machine in config_data.get('cgroup', []):
            profiler_tool = LIST_PROFILER[2]  # cgroup
    else:
        # Fallback if psutil is not imported
        if machine in config_data.get('top', []):
            profiler_tool = LIST_PROFILER[1]  # top
        elif machine in config_data.get('cgroup', []):
            profiler_tool = LIST_PROFILER[2]  # cgroup

    return profiler_tool

def main():
    global profiling_active, profiling_data, output_file, log_dir, hostname

    # Setup signal handlers first
    setup_signal_handlers()

    try:
        config_file_path = sys.argv[1]
        log_dir = sys.argv[2]
    except IndexError:
        print("PROVENANCE | ERROR: Missing arguments.")
        print("Usage: python profiler.py [config_file_path] [log_dir]")
        sys.exit(1)


    try:
        profiling_interval = int(os.getenv("COMPSS_PROFILING_INTERVAL", "5")) # Default to 5s
    except ValueError:
        print("PROVENANCE | Warning: Invalid COMPSS_PROFILING_INTERVAL. Defaulting to 5s.")
        profiling_interval = 5

    machine = os.getenv("BSC_MACHINE", subprocess.check_output("uname -s", shell=True, text=True).strip()).lower()
    current_config = get_config(machine, config_file_path)
    # print(f"DEBUG: VALUE OF CURRENT CONFIG {current_config}")

    if current_config is None:
        print(f"PROVENANCE | ERROR: No valid profiler config found for machine '{machine}'.")
        if not psutil_imported:
            print("PROVENANCE | INFO: 'psutil' is not installed, which limits options.")
        print("PROVENANCE | ERROR: it is not possible to monitor the resources on this system")
        exit(1)

    is_local = not os.getenv("ENQUEUE_COMPSS_ARGS")
    hostname = "localhost" if is_local else socket.gethostname()

    # This is the 9-column header for psutil, top, and cgroup (with 0s)
    to_write_header = "CPU,MEM,BYTE_SENT,BYTE_RECV,BYTE_READ_DISK,BYTE_WRITE_DISK,TIME_READ_DISK,TIME_WRITE_DISK,TIME\n"
    counter = 0

    try:
        if current_config == LIST_PROFILER[0] or current_config == LIST_PROFILER[1]:
            # --- psutil / top branch ---
            output_file = open(f"{log_dir}/resource_profiling_{hostname}.csv", "w")

            if current_config == LIST_PROFILER[0]: # psutil
                io_initial = psutil.disk_io_counters()
                ref_read, ref_write, ref_time_read, ref_time_write = (
                    io_initial.read_bytes,
                    io_initial.write_bytes,
                    io_initial.read_time,
                    io_initial.write_time,
                )
                net = psutil.net_io_counters()
                ref_byte_sent, ref_byte_recv = net.bytes_sent, net.bytes_recv
            else: # top
                ref_read, ref_write, ref_time_read, ref_time_write = (0, 0, 0, 0) # Init for first call
                ref_byte_sent = 0
                ref_byte_recv = 0

            # Get first measurement
            new_entry, ref_byte_sent, ref_byte_recv = profiling_function(
                0, 0, 0, 0, ref_byte_sent, ref_byte_recv, current_config, profiling_interval
            )

            output_file.write(to_write_header) # Write header
            output_file.write(new_entry) # Write first entry
            output_file.flush()
            profiling_data.append(new_entry.strip())  # Store data for summary

            while profiling_active:  # Loop until flag is set by signal
                if current_config != LIST_PROFILER[0]:
                    # 'top' mode needs manual sleep, 'psutil' interval handles it
                    time.sleep(profiling_interval)

                # Check if we should still be profiling after sleep
                if not profiling_active:
                    break

                if current_config == LIST_PROFILER[0]: # psutil
                    io_current = psutil.disk_io_counters()
                    byte_read = io_current.read_bytes - ref_read
                    byte_write = io_current.write_bytes - ref_write
                    time_read = io_current.read_time - ref_time_read
                    time_write = io_current.write_time - ref_time_write

                    ref_read, ref_write, ref_time_read, ref_time_write = (
                        io_current.read_bytes,
                        io_current.write_bytes,
                        io_current.read_time,
                        io_current.write_time,
                    )
                else: # top
                    byte_read = byte_write = time_read = time_write = None

                new_entry, ref_byte_sent, ref_byte_recv = profiling_function(
                    byte_read,
                    byte_write,
                    time_read,
                    time_write,
                    ref_byte_sent,
                    ref_byte_recv,
                    current_config,
                    profiling_interval,
                )

                output_file.write(new_entry)
                output_file.flush()
                profiling_data.append(new_entry.strip())  # Store data for summary
                counter += 1

        else:
            # --- cgroup branch ---
            total_mem_kb = get_total_memory_kb()
            total_node_cpus = get_total_cpu_count()
            cgroup_paths = find_root_cgroup_paths()

            # Check for the NEW 'blkio' path.
            # You must update find_root_cgroup_paths in utils.py to return this!
            if not all(k in cgroup_paths for k in ['cpu', 'memory', 'blkio']):
                print("PROVENANCE | ERROR: Failed to find cgroup paths (cpu, memory, or blkio).")
                print("PROVENANCE | INFO: Make sure 'find_root_cgroup_paths' in utils.py finds the 'blkio' controller path.")
                sys.exit(1)

            if not total_mem_kb or not total_node_cpus:
                print("PROVENANCE | ERROR: Failed to get required system info. Exiting.")
                sys.exit(1)

            # These files represent the *total* usage for the *entire node*
            cpu_usage_file = cgroup_paths['cpu'] / 'cpuacct.usage'
            mem_usage_file = cgroup_paths['memory'] / 'memory.usage_in_bytes'
            blkio_path = cgroup_paths['blkio'] # Path to blkio directory

            try:
                output_file = open(f"{log_dir}/resource_profiling_{hostname}.csv", "w")
                output_file.write(to_write_header) # Write 9-column header

                # Get initial CPU stats
                last_cpu_ns_str = read_cgroup_file(cpu_usage_file)
                if last_cpu_ns_str is None:
                    print(f"PROVENANCE | ERROR: Could not read initial CPU usage from {cpu_usage_file}.")
                    sys.exit(1) # Exit before loop

                last_cpu_ns = int(last_cpu_ns_str)
                last_read_time = time.monotonic()

                # Get initial Disk I/O stats
                last_io_stats = get_cgroup_io_stats(blkio_path)

                # Write first entry as 0s before loop
                first_entry = f"0.00,0.00,0,0,0,0,0,0,{datetime.now().isoformat()}\n"
                output_file.write(first_entry)
                output_file.flush()
                profiling_data.append(first_entry.strip())

            except Exception as e:
                print(f"PROVENANCE | ERROR: Could not open output file .csv: {e}")
                sys.exit(1)

            while profiling_active:
                time.sleep(profiling_interval)

                # Check flag immediately after waking up
                if not profiling_active:
                    break

                timestamp = datetime.now().isoformat()

                # --- Memory ---
                mem_bytes_str = read_cgroup_file(mem_usage_file)
                if mem_bytes_str is None:
                    print("PROVENANCE | Lost cgroup memory file. Stopping.")
                    break
                mem_used_kb = int(mem_bytes_str) / 1024.0
                mem_percent = (mem_used_kb / total_mem_kb) * 100

                # --- CPU ---
                current_cpu_ns_str = read_cgroup_file(cpu_usage_file)
                current_read_time = time.monotonic()
                if current_cpu_ns_str is None:
                    print("PROVENANCE | Lost cgroup CPU file. Stopping.")
                    break
                current_cpu_ns = int(current_cpu_ns_str)
                time_delta_ns = (current_read_time - last_read_time) * 1e9
                cpu_delta_ns = current_cpu_ns - last_cpu_ns

                cpu_cores_used = 0.0
                if time_delta_ns > 0:
                    cpu_cores_used = cpu_delta_ns / time_delta_ns
                cpu_percent = (cpu_cores_used / total_node_cpus) * 100

                last_cpu_ns = current_cpu_ns
                last_read_time = current_read_time

                # --- Disk I/O ---
                current_io_stats = get_cgroup_io_stats(blkio_path)

                byte_read_delta = current_io_stats['read_bytes'] - last_io_stats['read_bytes']
                byte_write_delta = current_io_stats['write_bytes'] - last_io_stats['write_bytes']
                time_read_delta_ms = current_io_stats['read_time_ms'] - last_io_stats['read_time_ms']
                time_write_delta_ms = current_io_stats['write_time_ms'] - last_io_stats['write_time_ms']

                last_io_stats = current_io_stats

                # --- Format Entry ---
                # This entry now includes Disk I/O stats
                new_entry = (
                    f"{cpu_percent:.2f},"
                    f"{mem_percent:.2f},"
                    f"0,0,"  # Network: BYTE_SENT, BYTE_RECV
                    f"{byte_read_delta},"
                    f"{byte_write_delta},"
                    f"{time_read_delta_ms},"
                    f"{time_write_delta_ms},"
                    f"{timestamp}\n"
                )

                output_file.write(new_entry)
                output_file.flush()
                profiling_data.append(new_entry.strip())
                counter += 1

    except KeyboardInterrupt:
        print("PROVENANCE | Profiling interrupted by user.")
        # Loop will exit, finally will run
    except Exception as e:
        if profiling_active:
            # Only print if we weren't already shutting down
            print(f"PROVENANCE | ERROR during profiling loop: {e}")
    finally:
        # This is the safe cleanup block
        if output_file and not output_file.closed:
            try:
                output_file.close()
                if __debug__:
                    print("PROVENANCE DEBUG | CSV file closed in finally block.")
            except Exception as e:
                print(f"PROVENANCE | Warning: Error in final cleanup: {e}")

    # --- Summary Writing ---
    # This logic is executed when the file is closed and loop is stopped.
    if counter > 1:
        print("PROVENANCE | Profiling completed.")
    if log_dir and hostname:
        try:
            with open(f"{log_dir}/profiling_summary_{hostname}.log", "w") as summary:
                summary.write(f"Profiling completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                summary.write(f"Total measurements collected: {len(profiling_data)}\n")
                summary.write(f"Profiling duration: {len(profiling_data)} intervals\n")
            if __debug__:
                print("PROVENANCE DEBUG | Summary file created successfully.")
        except Exception as e:
            print(f"PROVENANCE | Warning: Could not write summary file: {e}")


if __name__ == "__main__":
    main()
