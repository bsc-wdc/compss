#!/usr/bin/env python3

import os
import subprocess
import sys
from pathlib import Path

OUTS = "outs.csv"


def main():
    """Main function."""
    target_base_dir = sys.argv[1]
    start = int(sys.argv[2])
    end = int(sys.argv[3])

    tests_base_dir = os.path.join(target_base_dir, "apps")
    logs_base_dir = os.path.join(target_base_dir, "logs")

    queue_file = os.path.join(target_base_dir, ".queue.txt")
    with open(queue_file, "r", encoding="UTF-8") as f:
        processes_apps = f.read(-1).split("\n")
        print(f"[WRITING RESULTS] processes_apps: {processes_apps}")
        processes_apps.pop()

        outs_file = os.path.join(target_base_dir, OUTS)
        Path(outs_file).touch()
        print(f"[WRITING RESULTS] outs_file: {outs_file}")

        test_num = 0
        for log_dir in sorted(os.listdir(tests_base_dir)):
            print(f"[WRITING RESULTS] log_dir: {log_dir}")
            print(
                f"[WRITING RESULTS] test_num: {test_num} - start: {start} - end: {end}"
            )
            # Check if this test must be executed in this batch
            if test_num < start:
                test_num += 1
                continue
            elif test_num > end:
                break
            else:
                test_num += 1
            skip_file = os.path.join(logs_base_dir, log_dir, "skip")
            print(f"[WRITING RESULTS] Checking if test is skipped: {skip_file} ?")
            if os.path.isfile(skip_file):
                print(f"[WRITING RESULTS] Skip test {log_dir} results.")
                with open(outs_file, "a", encoding="UTF-8") as file:
                    file.write(f"{log_dir},none,none,2\n")
                continue
            else:
                print(f"[WRITING RESULTS] Checking test {log_dir} results.")
            processes = []
            for process in sorted(
                os.listdir(os.path.join(logs_base_dir, log_dir, ".COMPSs"))
            ):
                processes.append(process)
            print(f"[WRITING RESULTS] Processes: {processes}")
            for process in processes:
                print(f"[WRITING RESULTS] - Process: {process}")
                output_log_path = os.path.join(
                    logs_base_dir, log_dir, f"compss-{process}.out"
                )
                error_log_path = os.path.join(
                    logs_base_dir, log_dir, f"compss-{process}.err"
                )
                runtime_path = os.path.join(logs_base_dir, log_dir, ".COMPSs", process)
                matching = [s for s in processes_apps if process in s]
                l = matching[0].split()

                result_path = os.path.join(tests_base_dir, l[1], "result")
                cmd = [result_path, output_log_path, error_log_path, runtime_path]
                print(f"[WRITING RESULTS] - cmd: {cmd}")
                process = subprocess.Popen(cmd)
                process.communicate()
                exit_value = process.returncode
                print(f"[WRITING RESULTS] - l: {l}")
                print(f"[WRITING RESULTS] - exit_value: {exit_value}")
                with open(outs_file, "a", encoding="UTF-8") as file:
                    try:
                        file.write(f"{l[1]},{l[2]},{l[0]},{exit_value}\n")
                    except IndexError:
                        file.write(f"{l[1]},undefined,{l[0]},2\n")


if __name__ == "__main__":
    main()
