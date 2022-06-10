#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import pycompss_cli.core.utils as utils
import subprocess, os, shutil
from typing import List

from pycompss_cli.core.cmd_helpers import command_runner

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

    if utils.check_exit_code('which enqueue_compss') == 1:
        cmd = ['module load COMPSs'] + cmd
    cmd = ';'.join(cmd)

    subprocess.run(cmd, shell=True)


def local_jupyter(work_dir, jupyter_args):
    cmd = 'jupyter notebook --notebook-dir=' + work_dir
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
    

def local_exec_app(command, return_process=False):
    p = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if return_process:
        return p
    return p.stdout.decode().strip()


def local_submit_job(app_args, env_vars):
    cmd = f'enqueue_compss {app_args}'
    if utils.check_exit_code('which enqueue_compss') == 1:
        cmd = 'module load COMPSs;' + cmd
        
    if env_vars:
        cmd = ' ; '.join([*[f'export {var}' for var in env_vars], cmd])

    p = local_exec_app(cmd, return_process=True)
    job_id = p.stdout.decode().strip().split('\n')[-1].split(' ')[-1]
    if p.returncode != 0:
        print('ERROR:', p.stderr.decode())
    else:
        print('Job submitted:', job_id)
        return job_id

def local_job_list(local_job_scripts_dir):
    cmd = f"python3 {local_job_scripts_dir}/find.py"
    if utils.check_exit_code('which enqueue_compss') == 1:
        cmd = 'module load COMPSs;' + cmd
    return local_exec_app(cmd)

def local_cancel_job(local_job_scripts_dir, jobid):
    cmd = f"python3 {local_job_scripts_dir}/cancel.py {jobid}"
    if utils.check_exit_code('which enqueue_compss') == 1:
        cmd = 'module load COMPSs;' + cmd
    return local_exec_app(cmd)

def local_job_status(local_job_scripts_dir, jobid):
    cmd = f"python3 {local_job_scripts_dir}/status.py {jobid}"
    if utils.check_exit_code('which enqueue_compss') == 1:
        cmd = 'module load COMPSs;' + cmd
    status = local_exec_app(cmd)

    if status == 'SUCCESS\nSTATUS:':
        return 'ERROR'
    return status

def local_app_deploy(local_source: str, app_dir: str, dest_dir: str = None):
    dst = os.path.abspath(dest_dir) if dest_dir else app_dir
    os.makedirs(dst, exist_ok=True)
    for f in os.listdir(local_source):
        if os.path.isfile(os.path.join(local_source, f)):
            shutil.copy(os.path.join(local_source, f), os.path.join(dst, f))
        else:
            shutil.copytree(os.path.join(local_source, f), os.path.join(dst, f))
    if dest_dir is not None:
        with open(app_dir + '/.compss', 'w') as f:
            f.write(dst)
    print('App deployed from ' + local_source + ' to ' + dst)
