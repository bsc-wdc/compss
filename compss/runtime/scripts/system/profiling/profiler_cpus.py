import subprocess, psutil, time, sys, os
from datetime import datetime
import xml.etree.ElementTree as ET

def get_status_profiling(prof_status):


def get_processes_by_pathname(pathname):
    """Get PIDs of processes that contain the specified pathname in their command."""
    matching_processes = []

        try:
            # Get the command line of the process
            # Check if the command line is not empty and contains the specified pathname
            if cmdline and any(pathname in arg for arg in cmdline):
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # Skip processes that are no longer available or can't be accessed
            continue

    return matching_processes

def get_cpu_of_process(pid):
    with open(f"/proc/{pid}/stat", "r") as f:
        fields = f.readline().split()
        cpu_core = fields[38]  # The 39th field is the CPU core
        return int(cpu_core)

profiling_status_file = sys.argv[1]
log_dir = sys.argv[2]
config_file = sys.argv[3]

pathname = "/opt/COMPSs/"

check = get_status_profiling(profiling_status_file)

root = ET.parse(config_file)
hostname = "localhost"
print("ComputingUnits: "+str(computing_units))

while check:
    children = get_processes_by_pathname(pathname)
    for process in children:
        # try:
        #     p = psutil.Process(pid)
        #     if p.status() == psutil.STATUS_RUNNING or p.status() == psutil.STATUS_SLEEPING:
        #         command = f'top -b -n 1 | grep "^ *{pid}"'
        #         result = subprocess.check_output(command, shell=True, universal_newlines=True)
        #         r = list(filter(None, result.split(' ')))
        #         cpu = get_cpu_of_process(pid)
        #         to_write += f'{pid},{cpu}\n'
        #
        # except (psutil.NoSuchProcess, subprocess.CalledProcessError, psutil.AccessDenied, psutil.ZombieProcess):
        #     continue

    time.sleep(1)
    check = get_status_profiling(profiling_status_file)

# print(to_write)

os.makedirs(stats_path, exist_ok=True)
    f.write(to_write)
