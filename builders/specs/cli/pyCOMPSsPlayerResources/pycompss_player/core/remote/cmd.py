import json
import os
import pickle
import sys
import tarfile
import tempfile
import shutil
from uuid import uuid4
import subprocess
import tempfile
from pycompss_player.core.cmd_helpers import command_runner
import pycompss_player.core.utils as utils

# ################ #
# GLOBAL VARIABLES #
# ################ #

# ############# #
# API FUNCTIONS #
# ############# #

def remote_deploy_compss(env_id: str, login_info: str, modules) -> None:
    """ Deploy environment COMPSs in remote cluster.
    It stops any existing one since it can not coexist with itself.

    :param working_dir: Given working directory
    :param image: Given docker image
    :param restart: Force stop the existing and start a new one.
    :returns: None
    """

    print('Copying files to remote...')

    remote_env_dir = '~/.COMPSs/envs/' + env_id
    subprocess.run(f"ssh {login_info} 'mkdir -p {remote_env_dir}'", shell=True)


    if isinstance(modules, str):
        modules_file = modules
    else:
        tmp_modules = tempfile.NamedTemporaryFile(delete=False)
        mod_script = '\n'.join([f'module load {m}' for m in modules])
        tmp_modules.write((mod_script + '\n').encode())
        tmp_modules.close()
        modules_file = tmp_modules.name

    subprocess.run(f'scp {modules_file} {login_info}:{remote_env_dir}/modules.sh', shell=True)

    if not isinstance(modules, str):
        os.unlink(tmp_modules.name)

def remote_app_deploy(env_id: str, login_info: str, working_dir: str, remote_working_dir: str):
    
    subprocess.run(f"ssh {login_info} 'mkdir -p {remote_working_dir}'", shell=True)
    subprocess.run(f"scp -r {working_dir}/* {login_info}:'{remote_working_dir}/'", shell=True)
    
    return remote_working_dir

def remote_app_remove(login_info: str, remote_working_dir: str):
    subprocess.run(f"ssh {login_info} 'rm -rf {remote_working_dir}'", shell=True)

def remote_run_app(remote_dir: str, login_info: str, env_name: str, command: str):
    commands = [
        f'cd {remote_dir}',
        f'source ~/.COMPSs/envs/{env_name}/modules.sh',
        command
    ]
    return utils.ssh_run_commands(login_info, commands)

def remote_submit_job(env_id: str, login_info: str, remote_dir: str, app_args: str, envars=[]) -> None:
    """ Execute the given command in the cluster COMPSs environment.

    :param cmd: Command to execute.
    :returns: The execution stdout.
    """
    commands = [
        f'cd {remote_dir}',
        f'source ~/.COMPSs/envs/{env_id}/modules.sh',
        *[f'export {var}' for var in envars],
        f'enqueue_compss {app_args}'
    ]

    print(commands)

    stdout = utils.ssh_run_commands(login_info, commands)
    job_id = stdout.strip().split('\n')[-1].split(' ')[-1]
    print('Job submitted:', job_id)
    return job_id

def remote_list_job(login_info: str):
    subprocess.run(f"ssh {login_info} squeue", shell=True)

def remote_cancel_job(login_info: str, job_id: str):
    subprocess.run(f"ssh {login_info} scancel {job_id}", shell=True)

def remote_exec_app(login_info: str, exec_cmd: str):
    subprocess.run(f"ssh {login_info} {exec_cmd}", shell=True)