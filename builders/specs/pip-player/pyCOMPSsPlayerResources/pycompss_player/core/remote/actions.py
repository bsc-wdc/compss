"""
This file contains the actions supported by pycompss-player.
They are invoked from cli/pycompss.py and uses core/cmd.py.
"""

from pycompss_player.core.remote.cmd import remote_deploy_compss
from pycompss_player.core.remote.cmd import remote_submit_job
from pycompss_player.core.remote.cmd import remote_list_job
from pycompss_player.core.remote.cmd import remote_cancel_job
from pycompss_player.core.actions import Actions
import pycompss_player.core.utils as utils
import os

class RemoteActions(Actions):
    def __init__(self, arguments, debug=False, env_conf=None) -> None:
        super().__init__(arguments, debug=debug, env_conf=env_conf)

    def init(self):
        super().init()
        """ Deploys COMPSs infrastructure remote|cluster env

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
        pwd = remote_deploy_compss(self.arguments.login, working_dir)
        self.add_env_config({'remote_dir': pwd})

    def run(self, arguments, debug=False):
        super().run()
        raise NotImplementedError("Remote actions are not implemented yet")

    def job(self):
        action_name = 'list'

        if self.arguments.job:
            action_name = self.arguments.job

        action_name = utils.get_object_method_by_name(self, '__job_' + action_name, include_in_name=True)
        getattr(self, action_name)()
                
    def __job_submit(self):
        dir = os.path.basename(os.path.normpath(self.env_conf['working_dir']))
        login_info = self.env_conf['login']
        app = self.arguments.application
        args = self.arguments.argument
        remote_dir = self.env_conf['remote_dir']
        remote_submit_job(dir, login_info, remote_dir, app, args)

    def __job_list(self):
        login_info = self.env_conf['login']
        remote_list_job(login_info)

    def __job_cancel(self):
        login_info = self.env_conf['login']
        jobid = self.arguments.job_id
        remote_cancel_job(login_info, jobid)

    def exec(self):
        super().exec()

    def environment(self):
        super().environment()