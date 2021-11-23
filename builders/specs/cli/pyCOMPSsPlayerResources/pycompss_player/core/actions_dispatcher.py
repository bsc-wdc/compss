from pycompss_player.core.local.actions import LocalActions
from pycompss_player.core.docker.actions import DockerActions
from pycompss_player.core.remote.actions import RemoteActions
from pycompss_player.core import utils
from glob import glob
import os
from pathlib import Path
import json

class ActionsDispatcher(object):
    def __init__(self) -> None:
        super().__init__()
        self.home_path = str(Path.home())

    def run_action(self, arguments, debug):
        print(arguments)

        self.__ensure_default_env()

        if 'env' in arguments and arguments.env:
            self.__env = arguments.env
            env_conf = None
        else:
            envs_path = self.home_path + '/.COMPSs/envs'
            if not os.path.isdir(envs_path) or len(list(os.walk(envs_path))) == 1:
                print("There are no environments created. Try using `pycompss init`")
                exit(1)
            env_conf = utils.get_current_env()
            self.__env = env_conf['env']
            # print('SELECTED ENV', self.__env)

        # print('ENV', self.__env)
        
        self.__actions_cmd = self.__getactions_cmd(arguments, debug, env_conf)

        action_name = utils.get_object_method_by_name(self.__actions_cmd, arguments.action)
        # print('DISPATCH ACTION NAME', action_name)

        action_func = getattr(self.__actions_cmd, action_name)
        action_func()

    def __getactions_cmd(self, arguments, debug, env_conf=None):
        if self.__env == "local":
            return LocalActions(arguments, debug, env_conf)
        elif self.__env == "docker":
            return DockerActions(arguments, debug, env_conf)
        elif self.__env == "cluster":
            return RemoteActions(arguments, debug, env_conf)

    
    def __ensure_default_env(self):
        default_env = self.home_path + '/.COMPSs/envs/default'
        if not os.path.isdir(default_env):
            os.makedirs(default_env)
            open(default_env + '/current', 'a').close()
            with open(default_env + '/env.json', 'w') as def_env:
                json.dump({ 'env': 'local', 'name': 'default' }, def_env)