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
import os
from re import sub
from sys import stdout
import tempfile
from shutil import copyfile
import subprocess
import tempfile
import zipfile
import pycompss_cli.core.utils as utils

# ################ #
# GLOBAL VARIABLES #
# ################ #

# ############# #
# API FUNCTIONS #
# ############# #

def remote_deploy_compss(env_id: str, login_info: str, modules, envars=[]) -> None:
    """ Deploy environment COMPSs in remote.
    It stops any existing one since it can not coexist with itself.

    :param working_dir: Given working directory
    :param image: Given docker image
    :param restart: Force stop the existing and start a new one.
    :returns: None
    """

    subprocess.run(f"ssh {login_info} 'mkdir -p ~/.COMPSs'", shell=True)

    if isinstance(modules, str):
        modules_file = modules
    else:
        tmp_modules = tempfile.NamedTemporaryFile(delete=False)
        env_vars = '\n'.join([f'export {var}' for var in envars])
        mod_script = '\n'.join([f'module load {m}' for m in modules])
        tmp_modules.write((env_vars + '\n').encode())
        tmp_modules.write((mod_script + '\n').encode())
        tmp_modules.close()
        modules_file = tmp_modules.name

    copyfile(modules_file, f'{os.path.expanduser("~")}/.COMPSs/envs/{env_id}/modules.sh')

    if not isinstance(modules, str):
        os.unlink(tmp_modules.name)

    path = os.path.abspath(__file__)
    local_job_scripts_dir = os.path.dirname(path) + '/job_scripts/'
    subprocess.run(f"scp -r {local_job_scripts_dir} '{login_info}:~/.COMPSs/'", shell=True, stdout=subprocess.PIPE)
    

def remote_app_deploy(app_dir: str, login_info: str, local_source: str, remote_dest_dir: str = None, env_file: str = None):

    if env_file:
        if not os.path.isfile(env_file):
            print(f'ERROR: env file `{env_file}` not found.')
            exit(1)
            

    print('Deploying app...')
    utils.ssh_run_commands(login_info, [f'mkdir -p {app_dir}'])

    cmd_copy_files = f"scp -r {local_source}/* {login_info}:'{app_dir}/'"

    if os.path.isfile(local_source):
        cmd_copy_files = cmd_copy_files.replace('/*', '')

    subprocess.run(cmd_copy_files, shell=True)

    if env_file:
        cmd_copy_files = f"scp {env_file} {login_info}:'{app_dir}/.env'"
        subprocess.run(cmd_copy_files, shell=True)

    if remote_dest_dir:
        print('App deployed to', remote_dest_dir)
        utils.ssh_run_commands(login_info, [
            f'echo {remote_dest_dir} > {app_dir}/.compss',
            f'mkdir -p {os.path.dirname(remote_dest_dir)}',
            f'ln -s {app_dir} {remote_dest_dir}'
        ])

def remote_app_remove(login_info: str, app_dir: str):
    utils.ssh_run_commands(login_info, [
            f'cat {app_dir}/.compss | xargs rm -rf',
            f'rm -rf {app_dir}'
        ])

def remote_run_app(remote_dir: str, login_info: str, env_name: str, command: str, modules):
    commands = [
        f'cd {remote_dir}',
        *modules,
        command
    ]
    return utils.ssh_run_commands(login_info, commands)[0]

def remote_submit_job(login_info: str, remote_dir: str, app_args: str, modules, envars=None, debug=False) -> None:
    """ Execute the given command in the remote COMPSs environment.

    :param cmd: Command to execute.
    :returns: The execution stdout.
    """

    enqueue_debug = '-d' if debug else ''

    commands = [
        f'cd {remote_dir}',
        *modules,
        'if [ -f .env ]; then source .env; fi',
        f'enqueue_compss {enqueue_debug} {app_args}'
    ]

    if envars:
        commands = [f'export {var}' for var in envars] + commands

    if debug:
        print('********* DEBUG *********')
        print('Remote submit job commands:')
        for cmd in commands:
            print('\t', '->', cmd)
        print('***************************')

    stdout, stderr = utils.ssh_run_commands(login_info, commands)
    if debug:
        print('Remote submit job stdout:')
        print(stdout.strip())
        print('***************************')
        print('Remote submit job stderr:')
        print(stderr.strip())
        print('***************************')
    job_id = stdout.strip().split('\n')[-1].split(' ')[-1]
    print('Job submitted:', job_id)
    return job_id

def remote_get_home(login_info: str):
    return utils.ssh_run_commands(login_info, ['echo $HOME'])[0].strip()

def remote_list_apps(env_id: str, login_info: str, remote_home: str):
    commands = [
        f'[ ! -d "{remote_home}/.COMPSsApps/{env_id}" ] && echo "NO_APPS"',
        f'ls ~/.COMPSsApps/{env_id}/',
    ]

    stdout = utils.ssh_run_commands(login_info, commands)[0].strip()
    apps = stdout.split('\n')
    if 'NO_APPS' in stdout or (len(apps) == 1 and apps[0] == ''):
        return []

    return apps

def remote_env_remove(login_info, env_id, env_apps):
    for app_name in env_apps:
        print(f'Deleting {app_name}...')
        remote_dir = f'~/.COMPSsApps/{env_id}/{app_name}'
        remote_app_remove(login_info, remote_dir)

    utils.ssh_run_commands(login_info, [f'rm -rf ~/.COMPSsApps/{env_id}',])

def remote_list_job(login_info: str, modules):

    commands = [
        *modules,
        f'python3 ~/.COMPSs/job_scripts/find.py',
    ]

    stdout = utils.ssh_run_commands(login_info, commands)[0].strip()
    if stdout != 'SUCCESS':
        print(stdout)
    else:
        print('No jobs found')

def remote_cancel_job(login_info: str, job_id: str, modules):
    commands = [
        *modules,
        f'python3 ~/.COMPSs/job_scripts/cancel.py {job_id}',
    ]

    stdout = utils.ssh_run_commands(login_info, commands)[0].strip()
    print(stdout)

def remote_exec_app(login_info: str, exec_cmd: str, debug=False):
    if debug:
        print('Remote exec app command:')
        print('\t', '->', exec_cmd)
    return utils.ssh_run_commands(login_info, [exec_cmd])[0].strip()


def remote_download_file(login_info: str, remote_path: str, local_path: str, debug=False):
    if not os.path.isdir(local_path):
        if os.path.exists(local_path):
            print(f'ERROR: local path `{local_path}` is not a directory.')
            exit(1)
        else:
            os.makedirs(local_path)

    cmd = f'scp -r {login_info}:{remote_path}/* {local_path}'

    if debug:
        print('Remote download file command:')
        print('\t', cmd)

    subprocess.run(cmd, shell=True)
