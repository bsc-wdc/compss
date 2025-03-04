import subprocess, os, sys
import socket
from datetime import datetime

try:
    import psutil
except ImportError:
    print("psutil is not installed. Install it, if you want to monitor the resources status during the execution.")
    exit(1)

def get_cpu_top() -> list:
    """
    Execute the command to get the values of cpu and memory usage, by calling a bash command

    :return: list containing the as first value the cpu percentage and second value the memory percentage
    """
    COMMAND = "export LC_NUMERIC=C && top -b -n 1 | awk '/^%Cpu/ { cpu_usage = 100 - $8; cpu_usage = cpu_usage * 2; cpu_usage = (cpu_usage > 100) ? 100.0 : cpu_usage } /^MiB Mem/ { mem_usage = ($8 / $4) * 100 } END { printf \"%.2f,%.2f,\", cpu_usage, mem_usage }'"

    result = subprocess.check_output(COMMAND, shell=True, text=True).strip().split(",")
    return result

def profiling_function(
        interval: int,
        computing_units: int,
        byte_read: int,
        byte_write: int,
        time_read: int,
        time_write: int,
        prev_bytes_sent: int,
        prev_bytes_recv: int,
) -> tuple:
    """
    Function to profile and monitor system resource usage, including CPU, memory, and network I/O.

    :param interval: The time interval (in seconds) over which to calculate CPU usage.
    :param computing_units: The number of computing units (e.g., CPU cores) to consider for CPU usage calculation. If None, all available cores are used.
    :param byte_read: The total number of bytes read during the profiling period.
    :param byte_write: The total number of bytes written during the profiling period.
    :param time_read: The time taken (in seconds) for read operations.
    :param time_write: The time taken (in seconds) for write operations.
    :param prev_bytes_sent: The total number of bytes sent before this profiling period.
    :param prev_bytes_recv: The total number of bytes received before this profiling period.

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
    logical_processors = psutil.cpu_count(logical=True)
    physical_cores = psutil.cpu_count(logical=False)
    multiplication_factor = float(round(logical_processors / physical_cores, 2))
    cpu_mem = get_cpu_top()
    cpu = cpu_mem[0]
    mem = cpu_mem[1]

    net = psutil.net_io_counters()
    byte_sent = net.bytes_sent - prev_bytes_sent
    byte_recv = net.bytes_recv - prev_bytes_recv

    new_entry = f"{cpu},{mem},{byte_sent},{byte_recv},{byte_read},{byte_write},{time_read},{time_write},{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    return new_entry, net.bytes_sent, net.bytes_recv


def main():
    log_dir = sys.argv[1]
    print(f"LOG DIRECTORY RECEIVED: {log_dir}")

    profiling_interval = int(os.getenv("COMPSS_PROFILING_INTERVAL"))
    compss_home = os.getenv("COMPSS_HOME")

    computing_units = None
    # hostname = "localhost"

    is_local = os.getenv("IS_LOCAL")
    hostname = "localhost" if is_local else socket.gethostname()

    to_write = "CPU,MEM,BYTE_SENT,BYTE_RECV,BYTE_READ_DISK,BYTE_WRITE_DISK,TIME_READ_DISK,TIME_WRITE_DISK,TIME\n"

    io_initial = psutil.disk_io_counters()
    ref_read, ref_write, ref_time_read, ref_time_write = (
        io_initial.read_bytes,
        io_initial.write_bytes,
        io_initial.read_time,
        io_initial.write_time,
    )
    net = psutil.net_io_counters()
    ref_byte_sent, ref_byte_recv = net.bytes_sent, net.bytes_recv

    new_entry, ref_byte_sent, ref_byte_recv = profiling_function(
        1, computing_units, 0, 0, 0, 0, ref_byte_sent, ref_byte_recv
    )
    to_write += new_entry

    with open(f"{log_dir}/resource_profiling_{hostname}.csv", "w") as resource:
        resource.write(to_write)
        resource.flush()

        while True:
            io_current = psutil.disk_io_counters()
            byte_read = io_current.read_bytes - ref_read
            byte_write = io_current.write_bytes - ref_write
            time_read = io_current.read_time - ref_time_read
            time_write = io_current.write_time - ref_time_write

            # Update references
            ref_read, ref_write, ref_time_read, ref_time_write = (
                io_current.read_bytes,
                io_current.write_bytes,
                io_current.read_time,
                io_current.write_time,
            )

            new_entry, ref_byte_sent, ref_byte_recv = profiling_function(
                profiling_interval,
                computing_units,
                byte_read,
                byte_write,
                time_read,
                time_write,
                ref_byte_sent,
                ref_byte_recv,
            )

            resource.write(new_entry)
            resource.flush()

if __name__ == "__main__":
    main()
