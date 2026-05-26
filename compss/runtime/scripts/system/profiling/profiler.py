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
import subprocess, os, sys
import time
import socket
from datetime import datetime
import signal
import shutil
import platform

# Flag to control the profiling loop
profiling_active = True
# Global variables to store profiling data and file handle
profiling_data = []
output_file = None
log_dir = None
hostname = None

# import psutil only on macOS
machine_os = sys.platform
if machine_os == "darwin":
    try:
        import psutil
    except ImportError:
        print("PROVENANCE | PROFILING | ERROR: psutil is required on macOS for CPU/memory profiling. Please install it and try again.")
        sys.exit(1)


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
    print("PROVENANCE | PROFILING | Finishing profiling (signal received)...")
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


def get_gpu_usage(architecture: str):
    """
    Execute the command to get the values of gpu usage for different architectures.

    :param architecture: String specifying the GPU architecture ('nvidia', 'amd', 'intel', or 'apple')
    :return: List containing [gpu_percentage, gpu_memory_percentage]
    """

    try:
        if architecture == "nvidia":
            cmd = "nvidia-smi --query-gpu=utilization.gpu,utilization.memory --format=csv,noheader,nounits"
            result = subprocess.check_output(cmd, shell=True, text=True).strip().splitlines()[0].split(",")
            return [x.strip() for x in result]

        # This is AI generated and not tested yet because we don't have the following hardware available for testing. Please review and adjust as needed when testing on AMD, Intel, or Apple Silicon GPUs.
        elif architecture == "amd":
            # rocm-smi is the standard tool for AMD GPUs
            # --csv flag forces output to: device,GPU use (%),GPU memory use (%)
            cmd = "rocm-smi --showuse --showmemuse --csv"
            output = subprocess.check_output(cmd, shell=True, text=True).strip().splitlines()
            
            # output[0] is the header, output[1] contains the actual data
            if len(output) > 1:
                result = output[1].split(",")
                # Indices: 1 is GPU Use %, 2 is Mem Use % (0 is device name)
                gpu_util = result[1].strip()
                mem_util = result[2].strip()
                return [gpu_util, mem_util]
            return ["0", "0"]

        elif architecture == "intel":
            # Intel on Linux: Safest route without sudo is reading sysfs (if available)
            # Paths vary based on kernel, but this is standard for modern i915 drivers
            sysfs_path = "/sys/class/drm/card0/engine/rcs0/busy"
            if os.path.exists(sysfs_path):
                with open(sysfs_path, 'r') as f:
                    gpu_util = f.read().strip()
                return [gpu_util, "0"] # Sysfs rarely exposes VRAM util cleanly without extra tools
            else:
                print("PROVENANCE | PROFILING | WARNING: Intel sysfs path not found.")
                return ["0", "0"]
                
        elif architecture == "apple":
            # macOS Apple Silicon (M1/M2/M3)
            # powermetrics requires sudo, but it's the standard way to get Apple GPU stats
            print("PROVENANCE | PROFILING | WARNING: Apple GPU profiling requires 'sudo powermetrics'")
            return ["0", "0"]

        else:
            print(f"PROVENANCE | PROFILING | ERROR: Unsupported GPU architecture '{architecture}'")
            return ["0", "0"]

    except subprocess.CalledProcessError as e:
        print(f"PROVENANCE | PROFILING | ERROR: Failed to get {architecture.upper()} GPU usage: {e}")
        return ["0", "0"]
    except Exception as e:
        print(f"PROVENANCE | PROFILING | ERROR: Unexpected error for {architecture.upper()}: {e}")
        return ["0", "0"]


def detect_profilable_gpu():
    """
    Detects which GPU vendor can be profiled based on available system tools.
    """
    # 1. Check for NVIDIA
    if shutil.which("nvidia-smi") is not None:
        return "nvidia"
    
    # 2. Check for AMD
    if shutil.which("rocm-smi") is not None:
        return "amd"
    
    # 3. Check for Apple Silicon (macOS)
    if platform.system() == "Darwin" and platform.machine() == "arm64":
        return "apple"
        
    # 4. Check for Intel (Linux sysfs path from our previous function)
    if platform.system() == "Linux" and os.path.exists("/sys/class/drm/card0/engine/rcs0/busy"):
        return "intel"

    return "unknown"


def _get_multiplication_factor() -> float:
    """ 
    Reads /proc/cpuinfo to determine the number of physical cores and logical processors,
    and computes a multiplication factor to adjust CPU percentages accordingly.

    :return: multiplication factor (logical cores / physical cores) as float
    """ 
    unique_cores = set()
    current_phys_id = ""
    current_core_id = ""
    logical_cores = 0 

    with open("/proc/cpuinfo", "r") as f:
        for line in f:
            line = line.strip()
    
            if line.startswith("physical id"):
                current_phys_id = line.split(":")[1].strip()
            elif line.startswith("core id"):
                current_core_id = line.split(":")[1].strip()
            elif line == "": 
                # A blank line indicates the end of a processor block in cpuinfo.
                # If we found both IDs, add them as a unique pair to our set.
                if current_phys_id and current_core_id:
                    logical_cores += 1
                    unique_cores.add(f"{current_phys_id}:{current_core_id}")
    
                # Reset for the next processor block
                current_phys_id = ""
                current_core_id = ""

    # Fallback: some older or virtualized systems don't list 'core id'. 
    # If the set is empty, we assume at least 1 physical core.
    physical_cores = len(unique_cores) if unique_cores else 1
    return logical_cores / physical_cores if physical_cores > 0 else 1.0


def _read_proc_stat_raw() -> tuple:
    """
    Read raw CPU tick counters from /proc/stat.
    The first line aggregates all CPUs: cpu <user> <nice> <system> <idle> <iowait> ...

    :return: (idle_ticks, total_ticks) as integers
    """
    with open("/proc/stat", "r") as f:
        line = f.readline()  # First line is always the aggregate "cpu  ..."
    
    fields = list(map(int, line.split()[1:]))
    
    # idle = idle (3) + iowait (4)
    idle = fields[3] + fields[4]
    
    # Sum only the first 8 fields to avoid double-counting 
    # guest (8) and guest_nice (9), which are already included in user and nice.
    total = sum(fields[:8])
    
    return idle, total


def _read_proc_meminfo() -> float:
    """
    Read memory stats from /proc/meminfo.
    Uses MemAvailable (accounts for reclaimable caches) rather than MemFree
    to compute a realistic used-memory percentage, consistent with how
    psutil.virtual_memory().percent works.

    :return: memory used percentage (0.0 - 100.0)
    """
    stats = {}
    with open("/proc/meminfo", "r") as f:
        for line in f:
            parts = line.split()
            if len(parts) >= 2:
                stats[parts[0].rstrip(":")] = int(parts[1])  # values are in kB
    total = stats.get("MemTotal", 0)
    available = stats.get("MemAvailable", 0)
    if total == 0:
        return 0.0
    used = total - available
    return (used / total) * 100.0


def _get_cpu_mem_proc_linux(interval: int) -> tuple:
    """
    Measures system-wide CPU usage by computing a delta between two readings
    of /proc/stat separated by `interval` seconds, and reads memory usage
    from /proc/meminfo.

    This is a zero-dependency fallback equivalent to:
        psutil.cpu_percent(interval=N) and psutil.virtual_memory().percent

    Both /proc/stat and /proc/meminfo are kernel-wide files, unaffected by
    cgroup boundaries, so this works correctly even inside SLURM job steps.

    :param interval: seconds to wait between the two /proc/stat readings
    :return: (cpu_percent, mem_percent) as floats
    """
    idle_start, total_start = _read_proc_stat_raw()
    time.sleep(interval)
    idle_current, total_current = _read_proc_stat_raw()
    delta_idle = idle_current - idle_start
    delta_total = total_current - total_start

    if delta_total == 0:
        cpu_pct = 0.0
    else:
        cpu_pct = (1.0 - delta_idle / delta_total) * 100.0
    
    cpu_pct *= _get_multiplication_factor()  # Adjust for hyperthreading
    mem_pct = _read_proc_meminfo()
    return round(cpu_pct, 2), round(mem_pct, 2)


def _get_cpu_mem_proc_darwin(interval: int) -> tuple:
    """
    Measures system-wide CPU and memory usage on macOS using psutil.
    """
    # psutil blocks for 'interval' seconds to calculate the accurate delta
    cpu_pct = psutil.cpu_percent(interval=interval)
    mem_pct = psutil.virtual_memory().percent
    return round(cpu_pct, 2), round(mem_pct, 2)


def get_cpu_mem_proc(interval: int, machine_os: str) -> tuple:
    if machine_os == "linux":
        return _get_cpu_mem_proc_linux(interval)
    elif machine_os == "darwin":
        return _get_cpu_mem_proc_darwin(interval)
    else:
        # Stop profiling if we don't know how to read CPU/mem stats on this platform
        raise ValueError(f"Unsupported platform for CPU/memory profiling: {machine_os}")


def _get_infiniband_traffic() -> tuple:
    """
    Reads hardware performance counters for InfiniBand devices to capture 
    all traffic, including RDMA kernel-bypass traffic.
    
    :return: (total_rx_bytes, total_tx_bytes)
    """
    total_rx_bytes = 0
    total_tx_bytes = 0
    ib_base_path = "/sys/class/infiniband"
    
    # If the system doesn't have the infiniband class, it has no IB hardware
    if not os.path.exists(ib_base_path):
        return 0, 0

    # Iterate through all InfiniBand devices (e.g., mlx5_0, mlx5_1)
    for hca in os.listdir(ib_base_path):
        ports_path = os.path.join(ib_base_path, hca, "ports")
        if not os.path.exists(ports_path):
            continue
            
        # A single card might have multiple ports (e.g., port 1, port 2)
        for port in os.listdir(ports_path):
            counters_path = os.path.join(ports_path, port, "counters")
            
            try:
                # Read Received Data
                with open(os.path.join(counters_path, "port_rcv_data"), "r") as f:
                    # Multiply by 4 to convert DWords to Bytes
                    total_rx_bytes += int(f.read().strip()) * 4
                    
                # Read Transmitted Data
                with open(os.path.join(counters_path, "port_xmit_data"), "r") as f:
                    # Multiply by 4 to convert DWords to Bytes
                    total_tx_bytes += int(f.read().strip()) * 4
                    
            except (FileNotFoundError, ValueError):
                # Safely ignore if a specific counter file is missing or unreadable
                continue

    return total_rx_bytes, total_tx_bytes


def read_net_bytes_proc() -> tuple[int, int]:
    """
    Read total RX/TX bytes across all non-loopback interfaces
    from /proc/net/dev.

    Returns (total_tx_bytes, total_rx_bytes).
    """
    total_rx, total_tx = _get_infiniband_traffic()

    with open("/proc/net/dev", "r") as f:
        lines = f.readlines()

    # skip the first 2 header lines
    for line in lines[2:]:
        if ":" not in line:
            continue
        iface, data = line.split(":", 1)
        iface = iface.strip()
        # skip loopback; optionally skip docker/veth, etc.
        if iface == "lo" or iface.startswith("docker") or iface.startswith("veth"):
            continue

        fields = data.split()
        rx_bytes = int(fields[0])   # receive bytes column[web:94]
        tx_bytes = int(fields[8])   # transmit bytes column[web:94]

        total_rx += rx_bytes
        total_tx += tx_bytes

    return total_tx, total_rx


def profiling_function(
        byte_read: int,
        byte_write: int,
        time_read: int,
        time_write: int,
        prev_bytes_sent: int,
        prev_bytes_recv: int,
        interval: int,
        machine_os: str,
) -> tuple:
    """
    Function to profile and monitor system resource usage, including CPU, memory, and network I/O.

    :param byte_read: The total number of bytes read during the profiling period.
    :param byte_write: The total number of bytes written during the profiling period.
    :param time_read: The time taken (in seconds) for read operations.
    :param time_write: The time taken (in seconds) for write operations.
    :param prev_bytes_sent: The total number of bytes sent before this profiling period.
    :param prev_bytes_recv: The total number of bytes received before this profiling period.
    :param interval: Interval to use between every measurement
    :param machine_os: The machine_os name (e.g., "linux", "darwin") to determine which profiling method to use for CPU/memory stats.

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
    cpu, mem = get_cpu_mem_proc(interval, machine_os)
    tx_now, rx_now = read_net_bytes_proc() if machine_os == "linux" else (0, 0)  # Network profiling only implemented for Linux
    ref_byte_sent = tx_now
    ref_byte_recv = rx_now
    byte_sent = tx_now - prev_bytes_sent if prev_bytes_sent is not None else 0
    byte_recv = rx_now - prev_bytes_recv if prev_bytes_recv is not None else 0


    new_entry = (
        f"{cpu},{mem},{byte_sent},{byte_recv},"
        f"{byte_read},{byte_write},{time_read},{time_write},"
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    return new_entry, ref_byte_sent, ref_byte_recv


def main():
    global profiling_active, profiling_data, output_file, log_dir, hostname, machine_os

    # Setup signal handlers first
    setup_signal_handlers()

    try:
        log_dir = sys.argv[1]
        is_master = sys.argv[2].lower() == "true" if len(sys.argv) > 2 else False
        check_gpu = sys.argv[3].lower() == "true" if len(sys.argv) > 3 else False
    except IndexError:
        print("PROVENANCE | PROFILING | ERROR: Missing arguments.")
        print("Usage: python profiler.py [log_dir] [is_master] [check_gpu]")
        sys.exit(1)

    try:
        profiling_interval = int(os.getenv("COMPSS_PROFILING_INTERVAL", "5")) # Default to 5s
    except ValueError:
        # if variable is set but not an integer
        print("PROVENANCE | PROFILING | Warning: Invalid COMPSS_PROFILING_INTERVAL. Defaulting to 5s.")
        profiling_interval = 5

    # check if it is a local machine_os or a cluster node
    is_local = not os.getenv("ENQUEUE_COMPSS_ARGS")
    hostname = "localhost" if is_local else socket.gethostname()

    if check_gpu:
        to_write_header = "CPU,MEM,BYTE_SENT,BYTE_RECV,BYTE_READ_DISK,BYTE_WRITE_DISK,TIME_READ_DISK,TIME_WRITE_DISK,TIME,GPU_USAGE,GPU_MEM\n"
        graphics_arch = detect_profilable_gpu()
    else:
        to_write_header = "CPU,MEM,BYTE_SENT,BYTE_RECV,BYTE_READ_DISK,BYTE_WRITE_DISK,TIME_READ_DISK,TIME_WRITE_DISK,TIME\n"
        graphics_arch = "unknown"

    counter = 0

    try:
        # open the output file for writing; this will be closed in the finally block
        output_file = open(f"{log_dir}/resource_profiling_{hostname}.csv", "w")

        # top and proc: no disk I/O tracking, no network tracking
        ref_read, ref_write, ref_time_read, ref_time_write = (0, 0, 0, 0)
        ref_byte_sent = 0
        ref_byte_recv = 0

        # Get first measurement
        new_entry, ref_byte_sent, ref_byte_recv = profiling_function(
            0, 0, 0, 0, ref_byte_sent, ref_byte_recv, profiling_interval, machine_os
        )

        if check_gpu:
            gpu_usage = get_gpu_usage(graphics_arch)
            new_entry = f"{new_entry.strip()},{gpu_usage[0]},{gpu_usage[1]}\n"

        output_file.write(to_write_header)  # Write header
        output_file.write(new_entry)        # Write first entry
        output_file.flush()
        profiling_data.append(new_entry.strip())

        while profiling_active:
            # top and proc: disk I/O not tracked
            byte_read = byte_write = time_read = time_write = None

            new_entry, ref_byte_sent, ref_byte_recv = profiling_function(
                byte_read,
                byte_write,
                time_read,
                time_write,
                ref_byte_sent,
                ref_byte_recv,
                profiling_interval,
                machine_os
            )

            if check_gpu:
                gpu_usage = get_gpu_usage(graphics_arch)
                new_entry = f"{new_entry.strip()},{gpu_usage[0]},{gpu_usage[1]}\n"

            output_file.write(new_entry)
            output_file.flush()
            profiling_data.append(new_entry.strip())
            counter += 1

    except KeyboardInterrupt:
        print("PROVENANCE | PROFILING | Profiling interrupted by user.")
        # Loop will exit, finally will run
    except Exception as e:
        if profiling_active:
            # Only print if we weren't already shutting down
            print(f"PROVENANCE | PROFILING | ERROR during profiling loop: {e}")
    finally:
        # This is the safe cleanup block
        if output_file and not output_file.closed:
            try:
                output_file.close()
                if __debug__:
                    print("PROVENANCE DEBUG | PROFILING | CSV file closed in finally block.")
            except Exception as e:
                print(f"PROVENANCE | PROFILING | Warning: Error in final cleanup: {e}")

    # --- Summary Writing ---
    # This logic is executed when the file is closed and loop is stopped.
    if is_master:
        if counter > 1:
            print("PROVENANCE | PROFILING | Profiling completed.")
        if log_dir and hostname:
            try:
                with open(f"{log_dir}/profiling_summary_{hostname}.log", "w") as summary:
                    summary.write(f"Profiling completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    summary.write(f"Total measurements collected: {len(profiling_data)}\n")
                    summary.write(f"Profiling duration: {len(profiling_data)} intervals\n")
                if __debug__:
                    print("PROVENANCE DEBUG | PROFILING | Summary file created successfully.")
            except Exception as e:
                print(f"PROVENANCE | PROFILING | Warning: Could not write summary file: {e}")


if __name__ == "__main__":
    main()
