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

try:
    import psutil

    psutil_imported = True
except ImportError:
    print(
        "PROVENANCE | ERROR: psutil is not installed. Install it, if you want to monitor all the resources status during the execution."
    )
    psutil_imported = False

PROFILER_CONFIG = (
    "linux",
    "macos",
    "linux-top",
)

# Flag to control the profiling loop
profiling_active = True
# Global variables to store profiling data and file handle
profiling_data = []
output_file = None
log_dir = None
hostname = None

def end_profiling(sig, frame):
    global profiling_active, profiling_data, output_file, log_dir, hostname
    print("PROVENANCE | Finishing profiling...")
    profiling_active = False

    # Immediate cleanup - flush and close the main CSV file
    if output_file and not output_file.closed:
        try:
            output_file.flush()
            output_file.close()
            if __debug__:
                print("PROVENANCE DEBUG | Data recorded successfully.")
        except Exception as e:
            print(f"PROVENANCE | Warning: Recording data: {e}")

    # Write final summary file
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

    print("PROVENANCE | Profiling completed.")


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
    if config == PROFILER_CONFIG[1]:
        logical_processors = psutil.cpu_count(logical=True)
        physical_cores = psutil.cpu_count(logical=False)
        multiplication_factor = float(round(logical_processors / physical_cores, 2))
        cpu = psutil.cpu_percent(interval=interval) * multiplication_factor
        cpu = cpu if cpu < 100 else 100
        mem = psutil.virtual_memory().percent
    else:
        cpu_mem = get_cpu_top()
        cpu = cpu_mem[0]
        mem = cpu_mem[1]

    if config != PROFILER_CONFIG[2]:
        net = psutil.net_io_counters()
        ref_byte_sent = net.bytes_sent
        ref_byte_recv = net.bytes_recv
        byte_sent = ref_byte_sent - prev_bytes_sent
        byte_recv = ref_byte_recv - prev_bytes_recv
    else:
        ref_byte_sent = 0
        ref_byte_recv = 0
        byte_sent = None
        byte_recv = None

    new_entry = f"{cpu},{mem},{byte_sent},{byte_recv},{byte_read},{byte_write},{time_read},{time_write},{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    return new_entry, ref_byte_sent, ref_byte_recv


def main():
    global profiling_active, profiling_data, output_file, log_dir, hostname

    # Setup signal handlers first
    setup_signal_handlers()

    log_dir = sys.argv[1]

    profiling_interval = int(os.getenv("COMPSS_PROFILING_INTERVAL"))
    compss_home = os.getenv("COMPSS_HOME")

    computing_units = None
    # hostname = "localhost"

    CHECK_SYSTEM = "uname -s"
    system_type = subprocess.check_output(CHECK_SYSTEM, shell=True, text=True).strip()
    if psutil_imported:
        config_map = {
            "Linux": PROFILER_CONFIG[0],
            "Darwin": PROFILER_CONFIG[1],
        }
    else:
        config_map = {
            "Linux": PROFILER_CONFIG[2],
        }
    current_config = config_map.get(system_type, None)

    if current_config is None:
        print("PROVENANCE | ERROR: it is not possible to monitor the resources on this system")
        exit(1)

    is_local = not os.getenv("ENQUEUE_COMPSS_ARGS")
    hostname = "localhost" if is_local else socket.gethostname()

    to_write = "CPU,MEM,BYTE_SENT,BYTE_RECV,BYTE_READ_DISK,BYTE_WRITE_DISK,TIME_READ_DISK,TIME_WRITE_DISK,TIME\n"

    if current_config != PROFILER_CONFIG[2]:
        io_initial = psutil.disk_io_counters()
        ref_read, ref_write, ref_time_read, ref_time_write = (
            io_initial.read_bytes,
            io_initial.write_bytes,
            io_initial.read_time,
            io_initial.write_time,
        )
        net = psutil.net_io_counters()
        ref_byte_sent, ref_byte_recv = net.bytes_sent, net.bytes_recv
    else:
        ref_byte_sent = 0
        ref_byte_recv = 0

    new_entry, ref_byte_sent, ref_byte_recv = profiling_function(
        0, 0, 0, 0, ref_byte_sent, ref_byte_recv, current_config, profiling_interval
    )
    to_write += new_entry
    profiling_data.append(new_entry.strip())  # Store data for summary

    try:
        output_file = open(f"{log_dir}/resource_profiling_{hostname}.csv", "w")
        output_file.write(to_write)
        output_file.flush()

        while profiling_active:  # Changed from while True to while profiling_active
            if system_type == "Linux":
                time.sleep(profiling_interval)

            # Check if we should still be profiling after sleep
            if not profiling_active:
                break

            if current_config != PROFILER_CONFIG[2]:
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
            else:
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

    except KeyboardInterrupt:
        print("PROVENANCE | Profiling interrupted by user.")
        end_profiling(None, None)  # Call cleanup explicitly
    except Exception as e:
        print(f"PROVENANCE | Error during profiling: {e}")
        end_profiling(None, None)  # Call cleanup explicitly
    finally:
        # Safety check - only close if not already closed
        if output_file and not output_file.closed:
            try:
                output_file.close()
                if __debug__:
                    print("PROVENANCE DEBUG | CSV file closed in finally block.")
            except Exception as e:
                print(f"PROVENANCE | Warning: Error in final cleanup: {e}")


if __name__ == "__main__":
    main()
