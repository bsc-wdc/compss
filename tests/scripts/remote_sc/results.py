import os
import subprocess
import shutil
import sys

target_base_dir = sys.argv[1]
start = int(sys.argv[2])
end = int(sys.argv[3])
tests_base_dir = os.path.join(target_base_dir,"apps")
logs_base_dir = os.path.join(target_base_dir,"logs")

f = open(os.path.join(target_base_dir, ".queue.txt"), "r")

processes_apps = f.read(-1).split("\n")
processes_apps.pop()

with open(os.path.join(target_base_dir, "outs.csv"), "w") as file:
    pass

test_num = 0
for log_dir in sorted(os.listdir(tests_base_dir)):
    # Check if this test must be executed in this batch
    if test_num < start :
        test_num += 1
        continue
    elif test_num > end :
        break
    else :
        test_num += 1
    skip_file = os.path.join(logs_base_dir, log_dir, "skip")
    if os.path.isfile(skip_file):
        print("Skip test " + log_dir + " results.")
        with open(os.path.join(target_base_dir, "outs.csv"), "a") as file:
            file.write(log_dir+ ",none,none,2\n")
        continue
    processes = []
    for process in sorted(os.listdir(os.path.join(logs_base_dir,log_dir,".COMPSs"))):
        processes.append(process)
    for process in processes:
        output_log_path = os.path.join(logs_base_dir, log_dir, "compss-{}.out".format(process))
        error_log_path = os.path.join(logs_base_dir, log_dir, "compss-{}.err".format(process))
        runtime_path = os.path.join(logs_base_dir, log_dir, ".COMPSs",process)
        matching = [s for s in processes_apps if process in s]
        l  = matching[0].split()

        result_path = os.path.join(tests_base_dir,l[1],"result")
        cmd = [result_path, output_log_path, error_log_path, runtime_path]
        process = subprocess.Popen(cmd)
        process.communicate()
        exit_value = process.returncode
        with open(os.path.join(target_base_dir, "outs.csv"), "a") as file:
            file.write(str(l[1]) + "," + str(l[2])+","+ str(l[0])+","+ str(exit_value)+"\n")

f.close()
