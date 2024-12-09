import subprocess, psutil, os, sys
import xml.etree.ElementTree as ET
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt



def plot_percentage(time_list, cpu_list, mem_list):
    plt.style.use("ggplot")
    fig, ax = plt.subplots(figsize=(12, 8))

    ax.plot(time_list, cpu_list, marker="o", linestyle="-", label="CPU")
    ax.plot(time_list, mem_list, marker="o", linestyle="-", label="MEM")

    plt.xticks(time_list, labels=[f"{i}" for i in time_list], rotation=80)
    plt.subplots_adjust(top=0.95, bottom=0.25)

    plt.ylim(0, 100)
    ax.set_title("CPU and Memory usage", fontsize=20)
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Percentage (%)")
    ax.grid(True)
    ax.legend()

    return fig, ax


def plot_byte(time_list, byte_out, byte_in, label_out, label_in):
    plt.style.use("ggplot")
    fig, ax = plt.subplots(figsize=(12, 8))

    ax.plot(time_list, byte_out, marker="o", linestyle="-", label=label_out)
    ax.plot(time_list, byte_in, marker="o", linestyle="-", label=label_in)

    plt.xticks(time_list, labels=[f"Label {i}" for i in time_list], rotation=80)
    plt.subplots_adjust(top=0.95, bottom=0.3)

    ax.set_title(f"{label_in} and {label_out}", fontsize=20)
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Byte (B)")
    ax.grid(True)
    ax.legend()

    return fig, ax


def plot_results(folder_pathname):
    # folder_pathname = 'matmul_files.py_53/stats'
    if not os.path.exists(folder_pathname):
        exit(1)

    # iterate on every file in the directory
    for csv_resources in os.listdir(folder_pathname):
        csv_resources = os.path.join(folder_pathname, csv_resources)
        if not csv_resources.endswith(".csv") or os.path.isdir(csv_resources):
            continue

        machine_name = csv_resources.split(".csv")[0].split("_")[-1]
        df = pd.read_csv(csv_resources)
        cpu_usage = df["CPU"]
        mem_usage = df["MEM"]
        byte_sent = df["BYTE_SENT"]
        byte_recv = df["BYTE_RECV"]
        byte_read_disk = df["BYTE_READ_DISK"]
        byte_write_disk = df["BYTE_WRITE_DISK"]
        time_read_disk = df["TIME_READ_DISK"]
        time_write_disk = df["TIME_WRITE_DISK"]
        timestamps = df["TIME"]
        # timestamps = [i for i in range(0, len(timestamps))]

        output_path = folder_pathname.split(".csv")[0] + "/plots"
        os.makedirs(output_path, exist_ok=True)

        fig, ax = plot_percentage(timestamps, cpu_usage, mem_usage)
        plt.savefig(output_path + f"/cpu_mem_{machine_name}.png")

        fig, ax = plot_byte(
            timestamps, byte_sent, byte_recv, "Byte sent", "Byte received"
        )
        plt.savefig(output_path + f"/data_transferred_{machine_name}.png")

        fig, ax = plot_byte(
            timestamps, byte_write_disk, byte_read_disk, "Byte written", "Byte read"
        )
        plt.savefig(output_path + f"/disk_{machine_name}.png")


def get_status_profiling(prof_status):
    with open(prof_status, "r") as prof:
        return "true" in prof.readline()


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
    cpu_avg = psutil.cpu_percent(interval=interval)
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
    profiling_status_file = sys.argv[1]
    log_dir = sys.argv[2]
    # config_file = sys.argv[3]

    compss_home = os.getenv("COMPSS_HOME")

    computing_units = None
    hostname = "localhost"
    # hostname = str(subprocess.check_output("hostname", shell=True, universal_newlines=True)).strip()

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

    check = get_status_profiling(profiling_status_file)

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
    write_processors = ""

    while check:
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
        to_write += new_entry
        write_processors += str(psutil.cpu_percent(interval=None, percpu=True)) + "\n"

        check = get_status_profiling(profiling_status_file)

    stats_path = f"{log_dir}/stats"
    os.makedirs(stats_path, exist_ok=True)
    with open(f"{stats_path}/resource_profiling_{hostname}.csv", "w") as f:
        f.write(to_write)

    plot_results(stats_path)


if __name__ == "__main__":
    main()
