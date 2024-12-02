import subprocess, psutil, os, sys
import xml.etree.ElementTree as ET
from datetime import datetime

def get_status_profiling(prof_status):
    with open(prof_status, 'r') as prof:
        return 'true' in prof.readline()

def profiling_function(interval, computing_units, byte_read, byte_write, time_read, time_write):
    net_start = psutil.net_io_counters()
    cpus = psutil.cpu_percent(interval=interval, percpu=True)
    if computing_units is None:
        computing_units = len(cpus)
    cpu_avg = round(sum(cpus[:computing_units]) / computing_units, 2)
    mem = psutil.virtual_memory().percent

    net_end = psutil.net_io_counters()
    byte_sent = net_end.bytes_sent - net_start.bytes_sent
    byte_recv = net_end.bytes_recv - net_start.bytes_recv

    new_entry = f"{cpu_avg},{mem},{byte_sent},{byte_recv},{byte_read},{byte_write},{time_read},{time_write},{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    return new_entry


def main():
    profiling_status_file = sys.argv[1]
    log_dir = sys.argv[2]
    # config_file = sys.argv[3]

    compss_home = os.getenv("COMPSS_HOME")

    computing_units = None
    hostname = 'localhost'
    #hostname = str(subprocess.check_output("hostname", shell=True, universal_newlines=True)).strip()

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

    to_write = 'CPU,MEM,BYTE_SENT,BYTE_RECV,BYTE_READ_DISK,BYTE_WRITE_DISK,TIME_READ_DISK,TIME_WRITE_DISK,TIME\n'

    check = get_status_profiling(profiling_status_file)

    io = psutil.disk_io_counters()
    ref_read = io.read_bytes # 10
    ref_write = io.write_bytes
    ref_time_read = io.read_time
    ref_time_write = io.write_time

    to_write += profiling_function(1, computing_units, 0, 0,0,0)

    io = psutil.disk_io_counters()
    new_read = io.read_bytes
    new_write = io.write_bytes
    new_time_read = io.read_time
    new_time_write = io.write_time

    while check:
        byte_read = new_read - ref_read
        byte_write = new_write - ref_write
        time_read = new_time_read - ref_time_read
        time_write = new_time_write - ref_time_write

        ref_read += byte_read
        ref_write += byte_write
        ref_time_read += time_read
        ref_time_write += time_write

        to_write += profiling_function(5, computing_units, byte_read, byte_write, time_read, time_write)
        io = psutil.disk_io_counters()
        new_read = io.read_bytes
        new_write = io.write_bytes
        new_time_read = io.read_time
        new_time_write = io.write_time

        check = get_status_profiling(profiling_status_file)

    stats_path = f'{log_dir}/stats'
    os.makedirs(stats_path, exist_ok=True)
    with open(f'{stats_path}/resource_profiling_{hostname}.csv', 'w') as f:
        f.write(to_write)


if __name__ == '__main__':
    main()
