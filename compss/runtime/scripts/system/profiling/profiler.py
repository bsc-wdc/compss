import subprocess, psutil, os, sys
import socket
import xml.etree.ElementTree as ET
from datetime import datetime


def get_status_profiling(prof_status):
    try:
        with open(prof_status, "r") as prof:
            return "true" in prof.readline()
    except:
        return False

def profiling_function(
        interval,
        computing_units,
        byte_read,
        byte_write,
        time_read,
        time_write,
        prev_bytes_sent,
        prev_bytes_recv,
):
    logical_processors = psutil.cpu_count(logical=True)
    physical_cores = psutil.cpu_count(logical=False)
    multiplication_factor = float(round(logical_processors / physical_cores, 2))
    cpu_avg = psutil.cpu_percent(interval=interval) * multiplication_factor
    cpu_avg = cpu_avg if cpu_avg < 100 else 100
    # cpus = psutil.cpu_percent(interval=interval, percpu=True)
    # if computing_units is None:
    #     computing_units = len(cpus)
    # cpu_avg = round(sum(cpus[:computing_units]) / computing_units, 2)
    mem = psutil.virtual_memory().percent

    net = psutil.net_io_counters()
    byte_sent = net.bytes_sent - prev_bytes_sent
    byte_recv = net.bytes_recv - prev_bytes_recv

    new_entry = f"{cpu_avg},{mem},{byte_sent},{byte_recv},{byte_read},{byte_write},{time_read},{time_write},{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    return new_entry, net.bytes_sent, net.bytes_recv


def main():
    log_dir = sys.argv[1]
    print(f"LOG DIRECTORY RECEIVED: {log_dir}")
    # profiling_status_file = sys.argv[1]
    # log_dir = sys.argv[2]
    # config_file = sys.argv[3]

    compss_home = os.getenv("COMPSS_HOME")

    computing_units = None
    # hostname = "localhost"

    is_local = os.getenv("IS_LOCAL")
    hostname = "localhost" if is_local else socket.gethostname()

    # is_bsc = os.getenv('BSC_MACHINE')
    # hostname = hostname if is_bsc else 'localhost'
    # if hostname == 'localhost':
    #     to_parse = True
    #     while to_parse:
    #         try:
    #             root = ET.parse(config_file)
    #             computing_units = int(root.find('.//ComputingUnits').text)
    #             hostname = "localhost"
    #             print("ComputingUnits: "+str(computing_units))
    #             to_parse = False
    #         except:
    #             print(f'Wrong file {config_file}, using the default configuration.')
    #             config_file = compss_home + "/Runtime/configuration/xml/resources/default_resources.xml"
    #             to_parse = True

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
                5,
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

        # write_processors += str(psutil.cpu_percent(interval=None, percpu=True)) + "\n"

        # check = get_status_profiling(profiling_status_file)

    # ----------------------------------------------------------------

    # to_write += new_entry

    # while check:
    #     io_current = psutil.disk_io_counters()
    #     byte_read = io_current.read_bytes - ref_read
    #     byte_write = io_current.write_bytes - ref_write
    #     time_read = io_current.read_time - ref_time_read
    #     time_write = io_current.write_time - ref_time_write

    #     # Update references
    #     ref_read, ref_write, ref_time_read, ref_time_write = (
    #         io_current.read_bytes,
    #         io_current.write_bytes,
    #         io_current.read_time,
    #         io_current.write_time,
    #     )

    #     new_entry, ref_byte_sent, ref_byte_recv = profiling_function(
    #         5,
    #         computing_units,
    #         byte_read,
    #         byte_write,
    #         time_read,
    #         time_write,
    #         ref_byte_sent,
    #         ref_byte_recv,
    #     )
    #     to_write += new_entry

    #     check = get_status_profiling(profiling_status_file)

    # stats_path = f"{log_dir}/stats"
    # os.makedirs(stats_path, exist_ok=True)
    # with open(f"{stats_path}/resource_profiling_{hostname}.csv", "w") as f:
    #     f.write(to_write)


if __name__ == "__main__":
    main()
