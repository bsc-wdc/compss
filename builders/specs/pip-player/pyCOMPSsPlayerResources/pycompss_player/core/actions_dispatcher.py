from pycompss_player.core.local.actions import LocalActions
from pycompss_player.core.docker.actions import DockerActions
from pycompss_player.core.remote.actions import RemoteActions
from pycompss_player.core import utils
from glob import glob
import os
from pathlib import Path

class ActionsDispatcher(object):        
    def run_action(self, arguments, debug):
        # print(arguments)

        if 'env' in arguments and arguments.env:
            self.__env = arguments.env
            env_conf = None
        else:
            envs_path = str(Path.home()) + '/.COMPSs/envs'
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

    
