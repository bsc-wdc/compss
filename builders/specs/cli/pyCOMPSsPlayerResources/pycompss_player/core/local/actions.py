"""
This file contains the actions supported by pycompss-player.
They are invoked from cli/pycompss.py and uses core/cmd.py.
"""

from pycompss_player.core.local.cmd import local_deploy_compss
from pycompss_player.core.local.cmd import local_run_app
from pycompss_player.core.local.cmd import local_exec_app
from pycompss_player.core.local.cmd import local_jupyter
from pycompss_player.core.local.cmd import local_submit_job
from pycompss_player.core.actions import Actions
import pycompss_player.core.utils as utils
import os

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
            
        app_args = self.arguments.rest_args
        command = "runcompss " + ' '.join(app_args)
                
        local_run_app(command)

    def jupyter(self):
        working_dir = '.'
        if 'working_dir' in self.env_conf:
            working_dir = self.env_conf['working_dir']
        local_jupyter(working_dir)

    def exec(self):
        command = ' '.join(self.arguments.exec_cmd)
        local_exec_app(command)

    def app(self):
        print("ERROR: Wrong Environment! Try switching to a `cluster` environment")
        exit(1)

    def environment(self):
        super().environment()

    def env_remove(self):
        super().env_remove()

    def job(self):
        action_name = 'list'

        if self.arguments.job:
            action_name = self.arguments.job

        action_name = utils.get_object_method_by_name(self, 'job_' + action_name, include_in_name=True)
        getattr(self, action_name)()
                
    def job_submit(self):
        app_args = self.arguments.rest_args
        local_submit_job(app_args)

    def job_list(self):
        pass
        # local_list_job(login_info)

    def job_cancel(self):
        jobid = self.arguments.job_id
        # local_cancel_job(jobid)

    def monitor(self):
        if self.arguments.option == 'start':
            local_exec_app('/etc/init.d/compss-monitor start')
            local_exec_app('firefox http://localhost:8080/compss-monitor &')
        elif self.arguments.option == 'stop':
            local_exec_app('/etc/init.d/compss-monitor stop')
            
    def component(self):
        pass

    def gengraph(self):
        pass
