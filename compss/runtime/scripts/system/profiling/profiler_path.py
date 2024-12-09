import subprocess, psutil, time, sys
from datetime import datetime


def get_status_profiling(prof_status):
    with open(prof_status, "r") as prof:
        return "true" in prof.readline()


def get_processes_by_pathname(pathname):
    """Get PIDs of processes that contain the specified pathname in their command."""
    matching_processes = []

    for process in psutil.process_iter(["pid", "cmdline"]):
        try:
            # Get the command line of the process
            cmdline = process.info["cmdline"]
            # Check if the command line is not empty and contains the specified pathname
            if cmdline and any(pathname in arg for arg in cmdline):
                matching_processes.append(
                    {
                        "pid": process.info["pid"],
                        "cmdline": " ".join(
                            cmdline
                        ),  # Join the cmdline list into a single string for display
                    }
                )
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # Skip processes that are no longer available or can't be accessed
            continue

    return matching_processes


profiling_status_file = sys.argv[1]
log_dir = sys.argv[2]

to_write = "PID,CPU,MEM,TIME\n"
pathname = "/opt/COMPSs/"

check = get_status_profiling(profiling_status_file)

while check:
    children = get_processes_by_pathname(pathname)
    for process in children:
        pid = process["pid"]
        print(pid)
        try:
            p = psutil.Process(pid)
            if (
                p.status() == psutil.STATUS_RUNNING
                or p.status() == psutil.STATUS_SLEEPING
            ):
                command = f'top -b -n 1 | grep "^ *{pid}"'
                result = subprocess.check_output(
                    command, shell=True, universal_newlines=True
                )
                r = list(filter(None, result.split(" ")))
                cpu = r[8].strip().replace(",", ".")
                mem = r[9].strip().replace(",", ".")
                to_write += f'{pid},{cpu},{mem},{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n'

        except (
            psutil.NoSuchProcess,
            subprocess.CalledProcessError,
            psutil.AccessDenied,
            psutil.ZombieProcess,
        ):
            continue

    time.sleep(1)
    check = get_status_profiling(profiling_status_file)

# print(to_write)

with open(f"{log_dir}/PROFILING_TOP.csv", "w") as f:
    f.write(to_write)
