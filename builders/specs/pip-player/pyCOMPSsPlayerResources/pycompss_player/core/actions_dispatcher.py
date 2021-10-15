from pycompss_player.core.local.actions import LocalActions
from pycompss_player.core.docker.actions import DockerActions
from pycompss_player.core import utils
from glob import glob
import json
from pathlib import Path

class ActionsDispatcher(object):
    def __init__(self):
        self.__env = self.__get_current_env()
        print('SELECTED ENV', self.__env)
        
    def run_action(self, arguments, debug):
        print(arguments)

        if 'env' in arguments and arguments.env:
            self.__env = arguments.env

        print('ENV', self.__env)
        
        self.__actions_cmd = self.__getactions_cmd(arguments, debug)

        action_name = utils.get_object_method_by_name(self.__actions_cmd, arguments.action)
        print('DISPATCH ACTION NAME', action_name)

        action_func = getattr(self.__actions_cmd, action_name)
        action_func()

    def __getactions_cmd(self, arguments, debug):
        if self.__env == "local":
            return LocalActions(arguments, debug)
        elif self.__env == "docker":
            return DockerActions(arguments, debug)

    def __get_current_env(self):
        home_path = str(Path.home())
        current_env = glob(home_path + '/.COMPSs/envs/*/current')[0]
        with open(current_env.replace('current', 'env.json'), 'r') as env:
            return json.load(env)['env']
