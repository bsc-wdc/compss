#!/usr/bin/env python3

import os
import subprocess
import sys
from pathlib import Path


def create_log_skip_file(log_path: str) -> None:
    """Create a skip file.

    :param log_path: Log path where to put the skip file.
    :return: None
    """
    skip_file = os.path.join(log_path, "skip")
    Path(skip_file).touch()


def main():
    """Main function."""
    tests_base_dir = sys.argv[1]
    tests_apps_dir = os.path.join(tests_base_dir, "apps")
    logs_base_dir = os.path.join(tests_base_dir, "logs")

    runcompss_bin = sys.argv[2]
    comms = sys.argv[3]
    user_opts = sys.argv[4]  # ' '
    if user_opts == "none":
        user_opts = ""
    module = sys.argv[5]
    queue = sys.argv[6]
    qos = sys.argv[7]
    project_name = sys.argv[8]
    start = int(sys.argv[9])
    end = int(sys.argv[10])
    execution_envs = sys.argv[11:]  # python3
    # module = sys.argv[6] #COMPSs/2.6
    # master_working_dir = sys.argv[9]
    # worker_working_dir = sys.argv[10]

    # Start deploying tests
    if not os.path.exists(logs_base_dir):
        os.mkdir(logs_base_dir)

    queue_file = os.path.join(tests_base_dir, ".queue.txt")
    with open(queue_file, "w+", encoding="UTF-8") as f:
        test_num = 0
        for test_dir in sorted(os.listdir(tests_apps_dir)):
            # Check if this test must be executed in this batch
            if test_num < start:
                test_num += 1
                continue
            elif test_num > end:
                break
            else:
                test_num += 1
            test_path = os.path.join(tests_apps_dir, test_dir)
            test_logs_path = os.path.join(logs_base_dir, test_dir)
            skip_file = os.path.join(test_path, "skip")
            os.makedirs(test_logs_path)
            if os.path.isfile(skip_file):
                create_log_skip_file(test_logs_path)
                f.write(f"skip {test_dir} none\n")
                print("0")
                continue
            execution_envs_str = " ".join(str(x) for x in execution_envs)
            execution_script_path = os.path.join(test_path, "execution")
            cmd = [
                str(execution_script_path),
                str(runcompss_bin),
                str(comms),
                str(user_opts),
                str(test_path),
                str(test_logs_path),
                str(module),
                str(queue),
                str(qos),
                str(project_name),
                # Add more parameters here. Let execution_envs_str for the last argument
                str(execution_envs_str),
            ]

            try:
                process = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
                )
                out, err = process.communicate()
                if process.returncode != 0:
                    print(f"[ERROR] Executing test {cmd}")
                    print(f"[ERROR] {err}")
                    sys.exit(1)
                out = out.split("\n")
                job_id = "-1"
                environment = "none"
                for x in out:
                    if x.startswith("- Running with Environment:"):
                        environment = x.split(" ")
                        environment = environment[-1]
                    if x.startswith("Submitted batch job"):
                        job_id = x.split(" ")
                        job_id = job_id[-1]
                        f.write(f"{job_id} {test_dir} {environment}\n")
                        # printing job_id for being captured by the execute_sc_tests
                        print(job_id)
            except Exception as e:
                print(f"[ERROR] Executing test {cmd}")
                print(f"[ERROR] {e}")
                sys.exit(1)


if __name__ == "__main__":
    main()
