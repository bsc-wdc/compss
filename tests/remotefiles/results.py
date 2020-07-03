import os
import subprocess
import shutil
import sys
import csv
target_base_dir = "/home/bsc19/bsc19073"
tests_base_dir = os.path.join(target_base_dir,"tests_execution_sandbox","apps")
logs_base_dir = os.path.join(target_base_dir,"tests_execution_sandbox","logs")

f = open(os.path.join(target_base_dir, "cua.txt"), "r")

processes_apps = f.read(-1).split("\n")
processes_apps.pop()

with open(os.path.join(target_base_dir, "outs.csv"), "w") as file:
    writer = csv.writer(file)
    writer.writerow(["App Name", "Process ID", "Result"])


for log_dir in sorted(os.listdir(logs_base_dir)):
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
        if exit_value == 0:
            exit_value = "\033[92m"+"OK"+"\033[0m"
        else:
            exit_value = "\033[91m"+"FAIL"+"\033[0m"
        with open(os.path.join(target_base_dir, "outs.csv"), "a") as file:
            writer = csv.writer(file)
            writer.writerow([l[1], l[0], exit_value])
        
f.close()       
        
        
