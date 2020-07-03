import os
import subprocess
import shutil
import sys

target_base_dir = sys.argv[1]
tests_base_dir = os.path.join(target_base_dir,"tests_execution_sandbox","apps")
logs_base_dir = os.path.join(target_base_dir, "tests_execution_sandbox","logs")


runcompss_bin= sys.argv[2]
comms = sys.argv[3]
user_opts = sys.argv[4] # ' '
compss_logs_root = sys.argv[5] #.COMPSs
retry = sys.argv[6] #1
execution_envs = sys.argv[7] #python3
module = sys.argv[8] #COMPSs/2.6
master_working_dir = sys.argv[9]
worker_working_dir = sys.argv[10]


output = subprocess.check_output("rm -f cua.txt", shell=True)
output = subprocess.check_output("rm -rf {}".format(master_working_dir), shell=True)
output = subprocess.check_output("rm -rf {}".format(os.path.join(compss_logs_root,".COMPSs")), shell=True)
output = subprocess.check_output("rm -rf {}".format(logs_base_dir), shell=True)

os.mkdir(master_working_dir)
os.mkdir(logs_base_dir)

f = open("cua.txt", "w+")

g = open("comando.txt","w")
for test_dir in sorted(os.listdir(tests_base_dir)):
    test_path = os.path.join(tests_base_dir, test_dir)
    test_logs_path = os.path.join(logs_base_dir,test_dir+"_"+retry)
    
    execution_script_path = os.path.join(tests_base_dir, test_dir, "execution")
    os.makedirs(test_logs_path)
    cmd = [str(execution_script_path),
           str(runcompss_bin),
           str(comms),
           str(user_opts),
           str(test_path),
           str(".COMPSs"),
           str(test_logs_path),
           str(retry),
           str(execution_envs),
           str(module),
           str(test_logs_path),
           str(worker_working_dir)]
    
    g.write(str(cmd))
    
    exec_env = os.environ.copy()
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    out = out.split("\n")
    process_id = "-1"
    for x in out:
        if x.startswith("Submitted batch job"):
            process_id = x.split(" ")
            process_id = process_id[-1]
            f.write(process_id+" "+test_dir+"\n")
            print(process_id)
f.close()
g.close()    
