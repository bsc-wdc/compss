#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
"""
This file contains the actions supported by pycompss-cli.
They are invoked from cli/pycompss.py and uses core/cmd.py.
"""

from pycompss_cli.core.actions import Actions
from pycompss_cli.core.docker.cmd import DockerCmd
from glob import glob
import os, traceback
 
class DockerActions(Actions):

    def __init__(self, arguments, debug=False, env_conf=None) -> None:
        super().__init__(arguments, debug=debug, env_conf=env_conf)
        env_id = arguments.name if env_conf is None else env_conf['name']
        self.docker_cmd = DockerCmd(env_id)


    def init(self):
        super().init()
        """ Deploys COMPSs infrastructure in Docker

        :param arguments: Command line arguments
        :param debug: Debug mode
        :returns: None
        """

        if self.arguments.working_dir == 'current directory':
            self.arguments.working_dir = os.getcwd()

        if self.arguments.log_dir == 'current directory':
            self.arguments.log_dir = os.getcwd()

        try:
            self.docker_cmd.docker_deploy_compss(self.arguments.working_dir,
                                self.arguments.log_dir,
                                self.arguments.image,
                                self.arguments.restart,
                                self.arguments.privileged,
                                self.arguments.update_image)

            master_ip = self.docker_cmd.docker_exec_in_daemon("hostname -i", return_output=True)
            self.env_add_conf({'master_ip': master_ip})
        except:
            traceback.print_exc()
            print("ERROR: Docker deployment failed")
            self.env_remove(env_id=self.arguments.name)


    def update(self):
        """ Deploys COMPSs infrastructure in Docker

        :param arguments: Command line arguments
        :param debug: Debug mode
        :returns: None
        """
        if self.debug:
            print("Updating...")
        self.docker_cmd.docker_update_image()


    def exec(self):
        super().exec()
        """ Execute the given command in the running Docker image

        :param arguments: Command line arguments
        :param debug: Debug mode
        :returns: None
        """
        if self.debug:
            print("Executing...")
            print("Parameters:")
            print("\t- Command: " + self.arguments.command)
            print("\t- self.Arguments: " + str(self.arguments.argument))
        command = ' '.join(self.arguments.exec_cmd)
        self.docker_cmd.docker_exec_in_daemon(command)


    def run(self):
        super().exec()
        """ Run the given command in the COMPSs infrastructure at docker

        :param arguments: Command line arguments
        :param debug: Debug mode
        :returns: None
        """

        app_args = self.arguments.rest_args

        if '--project' not in app_args:
            app_args.insert(0, '--project=/project.xml ')
        if '--resources' not in app_args:
            app_args.insert(0, '--resources=/resources.xml ')
        if '--master_name' not in app_args:
            app_args.insert(0, f"--master_name={self.env_conf['master_ip']} ")
        if '--base_log_dir' not in app_args:
            app_args.insert(0, '--base_log_dir=/home/user ')

        command = "runcompss " + ' '.join(app_args)

        if self.debug:
            print("Running...")
            print("\t- Docker command: ", command)

        self.docker_cmd.docker_exec_in_daemon(command)

        if 'log_dir' in self.env_conf and \
            self.env_conf['log_dir'] == self.env_conf['working_dir']:
            return
        self.docker_cmd.docker_exec_in_daemon('cp -a /home/user/.COMPSs/. /root/.COMPSs/')


    def monitor(self):
        """ Starts or stops the monitor in the COMPSs infrastructure at docker

        :param arguments: Command line arguments
        :param debug: Debug mode
        :returns: None
        """
        if self.debug:
            print("Monitoring...")
            print("Parameters:")
            print("\t- Option: " + self.arguments.option)
        if self.arguments.option == "start":
            self.docker_cmd.docker_start_monitoring()
        elif self.arguments.option == "stop":
            self.docker_cmd.docker_stop_monitoring()
        else:
            raise Exception("Unexpected monitor option: " + self.arguments.option)


    def jupyter(self):
        """ Starts jupyter in the COMPSs infrastructure at docker

        :param arguments: Command line arguments
        :param debug: Debug mode
        :returns: None
        """

        self.docker_cmd.docker_exec_in_daemon('pkill jupyter')
        
        lab_or_notebook = 'lab' if self.arguments.lab else 'notebook'

        arguments = " ".join(self.arguments.rest_args)
        jupyter_cmd = f"jupyter {lab_or_notebook} " + \
                arguments + " " + \
                f"--ip={self.env_conf['master_ip']} " + \
                "--allow-root " + \
                "--NotebookApp.token="

        try:
            for out_line in self.docker_cmd.docker_exec_in_daemon(jupyter_cmd, return_stream=True):
                print(out_line.decode().strip().replace(self.env_conf['master_ip'], 'localhost'), flush=True)
        except KeyboardInterrupt:
            print('Closing jupyter server...')

        if self.docker_cmd.exists():
            self.docker_cmd.docker_exec_in_daemon('pkill jupyter')


    def gengraph(self):
        """ Converts the given task dependency graph (dot) into pdf
        using the docker image

        :param arguments: Command line arguments
        :param debug: Debug mode
        :returns: None
        """
        dot_path = self.arguments.dot_file
        
        command = "compss_gengraph " + dot_path
        self.docker_cmd.docker_exec_in_daemon(command)


    def gentrace(self):
        command = f"compss_gentrace {self.arguments.trace_dir} "
        command += ' '.join(self.arguments.rest_args)
        self.docker_cmd.docker_exec_in_daemon(command)
        if self.arguments.download_dir:
            self.docker_cmd.docker_exec_in_daemon(f'cp {self.arguments.trace_dir}/* {self.arguments.download_dir}/')


    def app(self):
        print("ERROR: Wrong Environment! Try using a `remote` or `local` environment")
        exit(1)

    def components(self):
        """ Lists/add/remove workers in the COMPSs infrastructure at docker

        :param arguments: Command line arguments
        :param debug: Debug mode
        :returns: None
        """
        if self.debug:
            print("Components: " + self.arguments.components)

        if self.arguments.components == "list":
            if self.debug:
                print("Listing components...")
            self.docker_cmd.docker_components(self.arguments.components)
        if self.arguments.components == "add":
            if self.debug:
                print("Adding components: " + str(self.arguments.worker))
            self.docker_cmd.docker_components(self.arguments.components,
                            self.arguments.add,
                            self.arguments.worker)
        if self.arguments.components == "remove":
            if self.debug:
                print("Removing components:" + str(self.arguments.worker))
            self.docker_cmd.docker_components(self.arguments.components,
                            self.arguments.remove,
                            self.arguments.worker)

    def env_remove(self, env_id=None):
        if self.docker_cmd.exists(self.env_conf['name']):
            try:
                self.docker_cmd.docker_exec_in_daemon('rm -rf .COMPSs')
            except:
                pass
            self.docker_cmd.docker_kill_compss()
        super().env_remove(eid=env_id)

    def job(self):
        print("ERROR: Wrong Environment! Try using a `remote` environment")
        exit(1)
