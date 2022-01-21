import os
from re import sub
from sys import stdout
import tempfile
from shutil import copyfile
import subprocess
import tempfile
import pycompss_cli.core.utils as utils

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

    subprocess.run(f"ssh {login_info} 'mkdir -p ~/.COMPSs'", shell=True)

    if isinstance(modules, str):
        modules_file = modules
    else:
        tmp_modules = tempfile.NamedTemporaryFile(delete=False)
        mod_script = '\n'.join([f'module load {m}' for m in modules])
        tmp_modules.write((mod_script + '\n').encode())
        tmp_modules.close()
        modules_file = tmp_modules.name

    copyfile(modules_file, f'{os.path.expanduser("~")}/.COMPSs/envs/{env_id}/modules.sh')

    if not isinstance(modules, str):
        os.unlink(tmp_modules.name)

    path = os.path.abspath(__file__)
    local_job_scripts_dir = os.path.dirname(path) + '/job_scripts/'
    subprocess.run(f"scp -r {local_job_scripts_dir} '{login_info}:~/.COMPSs/'", shell=True, stdout=subprocess.PIPE)
    

def remote_app_deploy(app_dir: str, login_info: str, local_source: str, remote_dest_dir: str = None):
    print('Deploying app...')
    utils.ssh_run_commands(login_info, [f'mkdir -p {app_dir}'])

    cmd_copy_files = f"scp -r {local_source}/* {login_info}:'{app_dir}/'"

    if os.path.isfile(local_source):
        cmd_copy_files = cmd_copy_files.replace('/*', '')

    subprocess.run(cmd_copy_files, shell=True)

    if remote_dest_dir:
        print('App deployed to', remote_dest_dir)
        utils.ssh_run_commands(login_info, [
            f'echo {remote_dest_dir} > {app_dir}/.compss',
            f'ln -s {app_dir} {remote_dest_dir}'
        ])

def remote_app_remove(login_info: str, app_dir: str):
    utils.ssh_run_commands(login_info, [
            f'cat {app_dir}/.compss | xargs rm',
            f'rm -rf {app_dir}'
        ])

def remote_run_app(remote_dir: str, login_info: str, env_name: str, command: str, modules):
    commands = [
        f'cd {remote_dir}',
        *modules,
        command
    ]
    return utils.ssh_run_commands(login_info, commands)

def remote_submit_job(login_info: str, remote_dir: str, app_args: str, modules, envars=None) -> None:
    """ Execute the given command in the cluster COMPSs environment.

    :param cmd: Command to execute.
    :returns: The execution stdout.
    """
    commands = [
        f'cd {remote_dir}',
        *modules,
        f'enqueue_compss {app_args}'
    ]

    if envars:
        commands = [f'export {var}' for var in envars] + commands

    stdout = utils.ssh_run_commands(login_info, commands)
    job_id = stdout.strip().split('\n')[-1].split(' ')[-1]
    print('Job submitted:', job_id)
    return job_id

def remote_get_home(login_info: str):
    return utils.ssh_run_commands(login_info, ['echo $HOME']).strip()

def remote_list_apps(env_id: str, login_info: str, remote_home: str):
    commands = [
        f'[ ! -d "{remote_home}/.COMPSsApps/{env_id}" ] && echo "NO_APPS"',
        f'ls ~/.COMPSsApps/{env_id}/',
    ]

    stdout = utils.ssh_run_commands(login_info, commands).strip()
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

    stdout = utils.ssh_run_commands(login_info, commands).strip()
    if stdout != 'SUCCESS':
        print(stdout)
    else:
        print('No jobs found')

def remote_cancel_job(login_info: str, job_id: str, modules):
    commands = [
        *modules,
        f'python3 ~/.COMPSs/job_scripts/cancel.py {job_id}',
    ]

    stdout = utils.ssh_run_commands(login_info, commands).strip()
    print(stdout)

def remote_exec_app(login_info: str, exec_cmd: str):
   return utils.ssh_run_commands(login_info, [exec_cmd]).strip()