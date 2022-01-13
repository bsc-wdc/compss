"""
This file contains the actions supported by pycompss-cli.
They are invoked from cli/pycompss.py and uses core/cmd.py.
"""

from pycompss_cli.core.local.cmd import local_deploy_compss
from pycompss_cli.core.local.cmd import local_run_app
from pycompss_cli.core.local.cmd import local_exec_app
from pycompss_cli.core.local.cmd import local_jupyter
from pycompss_cli.core.local.cmd import local_submit_job
from pycompss_cli.core.local.cmd import local_job_list
from pycompss_cli.core.local.cmd import local_cancel_job
from pycompss_cli.core.actions import Actions
import pycompss_cli.core.utils as utils
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

        commands = [
            "runcompss " + ' '.join(app_args)
        ]

        if 'working_dir' in self.env_conf:
            commands.insert(0, 'cd ' + self.env_conf['working_dir'])
                
        local_run_app(commands)

    def jupyter(self):
        working_dir = '.'
        if 'working_dir' in self.env_conf:
            working_dir = self.env_conf['working_dir']

        jupyter_args = self.arguments.rest_args
        local_jupyter(working_dir, ' '.join(jupyter_args))

    def exec(self):
        command = ' '.join(self.arguments.exec_cmd)
        local_exec_app(command)

    def app(self):
        print("ERROR: Wrong Environment! Try switching to a `cluster` environment")
        exit(1)

    def env_remove(self):
        super().env_remove()

    def job(self):
        action_name = 'list'

        if self.arguments.job:
            action_name = self.arguments.job

        action_name = utils.get_object_method_by_name(self, 'job_' + action_name, include_in_name=True)
        getattr(self, action_name)()
                
    def job_submit(self):
        modules = [
            'module load COMPSs',
            'module load python/3.6.1'
        ]
        app_args = ' '.join(self.arguments.rest_args)
        local_submit_job(modules, app_args)

    def job_list(self):
        path = os.path.abspath(__file__)
        local_job_scripts_dir = os.path.dirname(path) + '/../remote/job_scripts'
        local_job_list(local_job_scripts_dir)

    def job_cancel(self):
        jobid = self.arguments.job_id
        path = os.path.abspath(__file__)
        local_job_scripts_dir = os.path.dirname(path) + '/../remote/job_scripts'
        local_cancel_job(local_job_scripts_dir, jobid)

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
