import os
import subprocess
import shutil
import sys

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
execution_envs = sys.argv[8:] #python3
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

for test_dir in sorted(os.listdir(tests_apps_dir)):
    test_path = os.path.join(tests_apps_dir, test_dir)
    test_logs_path = os.path.join(logs_base_dir,test_dir)
    execution_envs_str = ' '.join(str(x) for x in execution_envs)
    execution_script_path = os.path.join(test_path, "execution")
    os.makedirs(test_logs_path)
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
