import subprocess, psutil, time, sys
from datetime import datetime

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

profiling_status_file = sys.argv[1]
log_dir = sys.argv[2]

pathname = "/opt/COMPSs/"

check = get_status_profiling(profiling_status_file)

while check:
    children = get_processes_by_pathname(pathname)
    for process in children:
        print(pid)
        try:
            p = psutil.Process(pid)
                command = f'top -b -n 1 | grep "^ *{pid}"'
                to_write += f'{pid},{cpu},{mem},{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n'

            continue

    time.sleep(1)
    check = get_status_profiling(profiling_status_file)

# print(to_write)

    f.write(to_write)
