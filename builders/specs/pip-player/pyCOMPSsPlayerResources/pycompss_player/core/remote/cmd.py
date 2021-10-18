import json
import os
import pickle
import sys
import tarfile
import tempfile
import shutil
from uuid import uuid4
import subprocess

from pycompss_player.core.cmd_helpers import command_runner

# ################ #
# GLOBAL VARIABLES #
# ################ #

# ############# #
# API FUNCTIONS #
# ############# #

def remote_deploy_compss(login_info: str, working_dir: str = "") -> None:
    """ Starts the main COMPSs image in Docker.
    It stops any existing one since it can not coexist with itself.

    :param working_dir: Given working directory
    :param image: Given docker image
    :param restart: Force stop the existing and start a new one.
    :returns: None
    """
    
    print('Copying files to remote...')
    subprocess.run(f'scp -r {working_dir} {login_info}:~/', shell=True)
    res = subprocess.run(f'ssh {login_info} pwd', shell=True, capture_output=True, text=True)
    return res.stdout.strip()


def remote_submit_job(dir: str, login_info: str, remote_dir: str, app: str, args: str) -> None:
    """ Execute the given command in the main COMPSs image in Docker.

    :param cmd: Command to execute.
    :returns: The execution stdout.
    """
    args = ' '.join(args)
    res = subprocess.run(f"ssh {login_info} 'cd {remote_dir}/{dir} ; ./{app} {args}'", shell=True, capture_output=True, text=True)
    print('Job submitted:', res.stdout.strip().split('\n')[-1].split(' ')[-1])

def remote_list_job(login_info: str):
    subprocess.run(f"ssh {login_info} squeue", shell=True)

def remote_cancel_job(login_info: str, job_id: str):
    subprocess.run(f"ssh {login_info} scancel {job_id}", shell=True)

# ################# #
# PRIVATE FUNCTIONS #
# ################# #

def _store_temp_cfg(cfg_content: str) -> tuple:
    """ Stores the given content in the temporary cfg file.

    :param cfg_content: Cfg file contents.
    :returns: The tmp file path and the cfg file name.
    """
    tmp_path = tempfile.mkdtemp()
    cfg_file = os.path.join(tmp_path, default_cfg_file)
    with open(cfg_file, "w") as f:
        f.write(cfg_content)
    return tmp_path, cfg_file