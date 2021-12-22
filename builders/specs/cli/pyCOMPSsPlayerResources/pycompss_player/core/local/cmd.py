import json
import os
import pickle
import subprocess
import sys
import tarfile
import tempfile
import shutil
from typing import List
from uuid import uuid4
import signal

from pycompss_player.core.cmd_helpers import command_runner

# ################ #
# GLOBAL VARIABLES #
# ################ #

# ############# #
# API FUNCTIONS #
# ############# #

def local_deploy_compss(working_dir: str = "") -> None:
    """ Starts the main COMPSs image in Docker.
    It stops any existing one since it can not coexist with itself.

    :param working_dir: Given working directory
    :param image: Given docker image
    :param restart: Force stop the existing and start a new one.
    :returns: None
    """
    
    # cfg_content = '{"working_dir":"' + working_dir + \
    #                 '","resources":"","project":""}'
    # tmp_path, cfg_file = _store_temp_cfg(cfg_content)
    # _copy_file(cfg_file, default_cfg)
    # shutil.rmtree(tmp_path)
    pass


def local_run_app(cmd: List[str]) -> None:
    """ Execute the given command in the main COMPSs image in Docker.

    :param cmd: Command to execute.
    :returns: The execution stdout.
    """

    cmd = ';'.join(cmd)

    print("Executing cmd: %s" % cmd)

    subprocess.run(cmd, shell=True)


def local_jupyter(work_dir, jupyter_args):
    cmd = 'jupyter notebook --notebook-dir=' + work_dir
    # run cmd in a subprocess, print the output and handle SIGINT signal for the subprocess
    process = subprocess.Popen(cmd + ' ' + jupyter_args, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        while True:
            line = process.stdout.readline()
            if line == '' and process.poll() is not None:
                break
            if line:
                print(line.strip().decode('utf-8'))
            if process.poll() is not None:
                break
    except KeyboardInterrupt:
        print('Closing jupyter...')
        process.kill()
    

def local_exec_app(command):
    subprocess.run(command, shell=True)


def local_submit_job(modules, app_args):
    mod_cmds = ' && '.join(modules)
    res = subprocess.run(f'{mod_cmds} && enqueue_compss {app_args}', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    job_id = res.stdout.decode().strip().split('\n')[-1].split(' ')[-1]
    if res.returncode != 0:
        print('ERROR:', res.stderr.decode())
    else:
        print('Job submitted:', job_id)
        return job_id