

import shutil
import os
import subprocess
import math
import os
import time
from enum import Enum
import sys
import csv
from constants import TESTS_DIR
from constants import CONFIGURATIONS_DIR
from constants import RUNCOMPSS_REL_PATH
from constants import CLEAN_PROCS_REL_PATH

class TestCompilationError(Exception):
    """
    Class representing an error when compiling the tests

    :attribute msg: Error message when compiling the tests
        + type: String
    """

    def __init__(self, msg):
        """
        Initializes the TestCompilationError class with the given error message

        :param msg: Error message when compiling the tests
        """
        self.msg = msg

    def __str__(self):
        return str(self.msg)


class TestDeploymentError(Exception):
    """
    Class representing an error when deploying the tests

    :attribute msg: Error message when deploying the tests
        + type: String
    """

    def __init__(self, msg):
        """
        Initializes the TestDeploymentError class with the given error message

        :param msg: Error message when deploying the tests
        """
        self.msg = msg

    def __str__(self):
        return str(self.msg)


def _compile(working_dir):
	pom_file = os.path.join(working_dir, "pom.xml")
	if not os.path.isfile(pom_file):
		print("[WARN] No pom.xml file found. Skipping compilation")
	else:
		cmd = ["mvn", "-U", "clean", "install"]
		exec_env = os.environ.copy()
		p = subprocess.Popen(cmd, cwd=working_dir)
		p.communicate()
		exit_value = p.returncode

        # Log command exit_value/output/error
		print("[INFO] Compilation command EXIT_VALUE: " + str(exit_value))

        # Raise an exception if command has failed
		if exit_value != 0:
			raise TestCompilationError("[ERROR] Compile command has failed with exit value: " + str(exit_value))
		print("[INFO] Compilation of " + working_dir + " successful")


def _deploy(source_path, test_exec_sandbox_global, test_num):
    """
    Executes the test deployment script

    :param source_path: Test source path
    :param test_exec_sandbox_global: Execution deployment path
    :param test_num: Global test number
    :param cmd_args: Object representing the command line arguments
        + type: argparse.Namespace
    :param compss_cfg:  Object representing the COMPSs test configuration options available in the given cfg file
        + type: COMPSsConfiguration
    :return:
    :raise TestCompilationError: If any compilation error has raised
    """

    test_exec_sandbox = os.path.join(test_exec_sandbox_global, "app" + "{:03d}".format(test_num))
    try:
        os.makedirs(test_exec_sandbox)
    except OSError:
        raise TestDeploymentError("[ERROR] Cannot create base execution sandbox directory: " + str(test_exec_sandbox))

    print("[INFO] Deploying " + str(source_path) + " to " + str(test_exec_sandbox))

    # Search deploy script
    deploy_script_path = os.path.join(source_path, "deploy")
    if not os.path.isfile(deploy_script_path):
    	raise TestDeploymentError("[ERROR] Cannot find deploy script " + str(deploy_script_path))

        # Invoke deploy script
    import subprocess
    cmd = [deploy_script_path, source_path, test_exec_sandbox]
    p = subprocess.Popen(cmd, cwd=source_path)
    p.communicate()
    exit_value = p.returncode

    # Log command exit_value/output/error
    print("[INFO] Deployment command EXIT_VALUE: " + str(exit_value))

    # Raise an exception if command has failed
    if exit_value != 0:
    	raise TestDeploymentError("[ERROR] Deployment command has failed with exit value: " + str(exit_value))
    print("[INFO] Deployment of " + str(source_path) + " completed")


def

def execute_tests_sc():
	import subprocess
	import polling
	import configparser
	config = configparser.ConfigParser()
	cfg_file = "MN.cfg"
	cfg_file = os.path.join(CONFIGURATIONS_DIR, cfg_file)
	config.read(cfg_file)
    	cfg_vars = {k: v for k, v in config.items("SUPERCOMPUTER")}
	username = cfg_vars["username"]
	module = cfg_vars["module"]
	comm = cfg_vars["comm"]
	deploy_path = cfg_vars["deploy_path"]
	exec_envs = cfg_vars["exec_envs"]
	runcompss_opts = '" "'
	runcompss_bin = "/apps/{}/Runtime/scripts/user/enqueue_compss".format(module)
	master_working_dir = cfg_vars["master_working_dir"]
	worker_working_dir = cfg_vars["worker_working_dir"]

        cmd = "ssh "+username+" "+"'python loop.py "+deploy_path+" "+runcompss_bin+" "+comm+" "+runcompss_opts+" "+".COMPSs"+" "+str(1)+" "+str(exec_envs)+" "+module+" "+master_working_dir+" "+worker_working_dir+"'"
	print(cmd)
	process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    	out, err = process.communicate()
    	out = str(out)
	print("[INFO] Executing tests on Supercomputer:")
	import subprocess
	jobs = out.split()
	print("[INFO] Jobs: {}".format(str(jobs)))
	for job in jobs:
		print("[INFO] Waiting for job {}".format(job))
		polling.poll(
			lambda: not subprocess.check_output('ssh {} "squeue -h -j {}"'.format(username, job), shell=True),
			step=2,
			poll_forever=True
		)
	print("[INFO] All jobs finished")
	print("[INFO] Checking results")
	cmd = "ssh "+username+" "+"'python results.py "+deploy_path+"'"
	process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	out, err = process.communicate()

	cmd = "scp "+username+":"+os.path.join(deploy_path,"outs.csv")+" /tmp"
	subprocess.check_output(cmd, shell=True)
	import pandas as pd
	pd.set_option("display.colheader_justify", "left")
	data = pd.read_csv("/tmp/outs.csv")
	print(data)


	sys.exit()

def main():


target_base_dir = "/home/sergi/tests_execution_sandbox"

try:
	shutil.rmtree(target_base_dir)
except Exception:
	print("[ERROR] Cannot clean target directory "+str(target_base_dir))

compss_base_log_dir = "/home/sergi/.COMPSs"

try:
	os.makedirs(target_base_dir)
except OSError:
	raise TestCompilationError("Error, cannot create base deployment directory "+ str(target_base_dir))

tests_exec_sandbox = os.path.join(target_base_dir, "apps")
try:
	os.makedirs(tests_exec_sandbox)
except OSError:
	raise TestCompilationError(
			"Error cannot create executing sandbox deloyment directory "+str(tests_exec_sandbox))

tests_logs = os.path.join(target_base_dir, "logs")

try:
	os.makedirs(tests_logs)
except OSError:
	raise TestCompilationError("Error cannot create log deployment directory "+str(tests_logs))
print("[INFO] deployment structure created")

TESTS_DIR = "../sources"

test_numbers = {"global": {}}
num_global = 1
compiled_tests = []
for family_dir in sorted(os.listdir(TESTS_DIR)):
	family_path = os.path.join(TESTS_DIR, family_dir)
	if os.path.isdir(family_path):
		test_numbers[family_dir] = {}
		num_family = 1
		for test_dir in sorted(os.listdir(family_path)):
			test_path = os.path.join(family_path, test_dir)
			if test_dir != ".target" and test_dir != ".settings" and test_dir != "target" and test_dir != ".idea" and os.path.isdir(test_path):
				test_numbers["global"][num_global] = (test_dir, test_path, family_dir, num_family)
				test_numbers[family_dir][num_family] = (test_dir, test_path, num_global)
				num_global = num_global + 1
				num_family = num_family + 1

tests = ['99','100']

for test in tests:
	test_num = int(test)
	if test_num not in test_numbers["global"].keys():
		raise TestCompilationError("Error invalid test number "+str(test_num))
	test_dir, test_path, family_dir, family_num = test_numbers["global"][test_num]
	print("[INFO] Compiling specific test")
	_compile(test_path)
        compiled_tests.append((test_dir, test_path, test_num))
for test_dir, test_path, test_global_num in compiled_tests:
	print("[INFO] Deploying test " + str(test_dir))
        _deploy(test_path, tests_exec_sandbox, test_global_num)


print("[INFO] Tests locally deployed")
print("[INFO] Deploying to SUPERCOMPUTER")
cfg_file = "MN.cfg"
cfg_file = os.path.join(CONFIGURATIONS_DIR, cfg_file)
print("[INFO] Loading values from " + str(cfg_file))
import configparser
config = configparser.ConfigParser()
config.read(cfg_file)
# Load default variables
cfg_vars = {k: v for k, v in config.items("SUPERCOMPUTER")}
username = cfg_vars["username"]
deploy_path = cfg_vars["deploy_path"]
import subprocess
output = subprocess.check_output(["ssh",username,"rm -rf {}/tests_execution_sandbox".format(deploy_path)])
output = subprocess.check_output(["scp","-r",target_base_dir,username+":"+deploy_path])

print("[INFO] All tests deployed to Supercomputer")
execute_tests_mn()
