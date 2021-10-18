"""
This file contains the actions supported by pycompss-player.
They are invoked from cli/pycompss.py and uses core/cmd.py.
"""

from pycompss_player.core.local.cmd import local_deploy_compss
from pycompss_player.core.local.cmd import local_run_app
from pycompss_player.core.actions import Actions

class LocalActions(Actions):
    def __init__(self, arguments, debug=False, env_conf=None) -> None:
        super().__init__(arguments, debug=debug, env_conf=env_conf)

    def init(self):
        super().init()
        """ Deploys COMPSs infrastructure local env

        :param arguments: Command line arguments
        :param debug: Debug mode
        :returns: None
        """

        working_dir = ''
        if 'working_dir' in self.arguments:
            working_dir = self.arguments.working_dir

        if self.debug:
            print("Initializing...")
            print("Parameters:")
            # if arguments.working_dir:
            #     working_dir = arguments.working_dir
            # else:
            #     working_dir = "(default)"
            # if arguments.image:
            #     image = arguments.image
            # else:
            #     image = "(default)"
            print("\t- Working dir: " + working_dir)
            # print("\t- Image: " + image)
            # print("\t- Restart: " + str(arguments.restart))
        local_deploy_compss(working_dir)


    def run(self):
        super().run()
        """ Run the given command in the COMPSs infrastructure at docker

        :param arguments: Command line arguments
        :param debug: Debug mode
        :returns: None
        """
        if self.debug:
            print("Running...")
            print("Parameters:")
            print("\t- Application: " + self.arguments.application)
            print("\t- Arguments: " + str(self.arguments.argument))
        application = self.arguments.application + " ".join(self.arguments.argument)
        command = "runcompss " + application
                # "--project=/project.xml " + \
                # "--resources=/resources.xml " + \
                # "--master_name=172.17.0.2 " + \
                # "--base_log_dir=/home/user " + \
                
        local_run_app(command)

    def exec(self, arguments, debug=False):
        super().exec()

    def environment(self):
        super().environment()