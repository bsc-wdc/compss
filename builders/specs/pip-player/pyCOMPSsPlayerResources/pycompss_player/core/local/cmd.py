import json
import os
import pickle
import sys
import tarfile
import tempfile
import shutil
from uuid import uuid4

from pycompss_player.core.cmd_helpers import command_runner

# ################ #
# GLOBAL VARIABLES #
# ################ #


master_name = "pycompss-master"
worker_name = "pycompss-worker"
service_name = "pycompss-service"
default_workdir = "/home/user/"
default_worker_workdir = "/home/user/.COMPSsWorker"
default_cfg_file = "cfg"
default_cfg = default_workdir + "/" + default_cfg_file
default_image_file = "image"
default_image = default_workdir + "/" + default_image_file

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
    print('LOCAL INIT')
    pass


def local_run_app(cmd: str) -> None:
    """ Execute the given command in the main COMPSs image in Docker.

    :param cmd: Command to execute.
    :returns: The execution stdout.
    """
    print("Executing cmd: %s" % cmd)

    command_runner(cmd.split(' '))




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