from pycompss_player.core.local.actions import LocalActions
from pycompss_player.core.docker.actions import DockerActions

class ActionsDispatcher(object):
    def __init__(self):
        self.__env = self.__get_current_env()
        
    def run_action(self, arguments, debug):
        print(arguments)

        if 'env' in arguments and arguments.env:
            self.__env = arguments.env

        print('ENV', self.__env)
        action_name = arguments.action
        self.__actions_cmd = self.__getactions_cmd(arguments, debug)
        action_func = getattr(self.__actions_cmd, action_name)
        action_func(arguments, debug)

    def __getactions_cmd(self, arguments, debug):
        if self.__env == "local":
            return LocalActions(arguments, debug)
        elif self.__env == "docker":
            return DockerActions()

    def __get_current_env(self):
        # TODO
        return "local"