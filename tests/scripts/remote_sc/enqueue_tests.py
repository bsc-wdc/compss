import os
import subprocess
import shutil
import sys

def create_log_skip_file(log_path):
    skip_file = os.path.join(log_path, "skip")
    with open(skip_file, "w") as file:
        pass

tests_base_dir = sys.argv[1]
tests_apps_dir = os.path.join(tests_base_dir, "apps")
logs_base_dir = os.path.join(tests_base_dir, "logs")


runcompss_bin= sys.argv[2]
comms = sys.argv[3]
user_opts = sys.argv[4] # ' '
if user_opts == "none" :
    user_opts=""
module = sys.argv[5]
queue = sys.argv[6]
qos = sys.argv[7]
start = int(sys.argv[8])
end = int(sys.argv[9])
execution_envs = sys.argv[10:] #python3
#module = sys.argv[6] #COMPSs/2.6
#master_working_dir = sys.argv[9]
#worker_working_dir = sys.argv[10]


#output = subprocess.check_output("rm -f tests_execution_sandbox/.queue", shell=True)
#output = subprocess.check_output("rm -rf {}".format(master_working_dir), shell=True)
#output = subprocess.check_output("rm -rf {}".format(os.path.join(compss_logs_root,".COMPSs")), shell=True)
#output = subprocess.check_output("rm -rf {}".format(logs_base_dir), shell=True)

if not os.path.exists(logs_base_dir):
    os.mkdir(logs_base_dir)

queue_file = os.path.join(tests_base_dir, ".queue.txt")
f = open(queue_file, "w+")
test_num = 0
for test_dir in sorted(os.listdir(tests_apps_dir)):
    # Check if this test must be executed in this batch
    if test_num < start :
        test_num += 1
        continue
    elif test_num > end :
        break
    else :
        test_num += 1
    test_path = os.path.join(tests_apps_dir, test_dir)
    test_logs_path = os.path.join(logs_base_dir,test_dir)
    skip_file = os.path.join(test_path, "skip")
    os.makedirs(test_logs_path)
    if os.path.isfile(skip_file):
        create_log_skip_file(test_logs_path)
        continue
    execution_envs_str = ' '.join(str(x) for x in execution_envs)
    execution_script_path = os.path.join(test_path, "execution")
    cmd = [str(execution_script_path),
           str(runcompss_bin),
           str(comms),
           str(user_opts),
           str(test_path),
           str(test_logs_path),
           str(module),
           str(queue),
           str(qos), #Add more parameters here. Let exec_envs for the last argument
           str(execution_envs_str)]

    #exec_env = os.environ.copy()
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        if process.returncode != 0 :
            print("[ERROR] Executing test " + str(cmd) + "\n" +str(err))
            exit(1)
        out = out.split("\n")
        job_id = "-1"
        environment = 'none'
        for x in out:
            if x.startswith("- Running with Environment:"):
                environment = x.split(" ")
                environment = environment[-1]
            if x.startswith("Submitted batch job"):
                job_id = x.split(" ")
                job_id = job_id[-1]
                f.write(job_id + " " + test_dir + " " + environment + "\n")
                #printing job_id for being caputured by the execute_sc_tests
                print(job_id)
    except Exception as e:
        print("[ERROR] Executing test " + str(cmd) + "\n" + str(e))
        exit(1)

f.close()
