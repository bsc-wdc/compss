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

        try:
            self.docker_cmd.docker_deploy_compss(self.arguments.working_dir,
                                self.arguments.image,
                                self.arguments.restart)

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
        if self.debug:
            print("Running...")
            print("Parameters:")
            print("\t- Application: " + self.arguments.application)
            print("\t- self.Arguments: " + str(self.arguments.argument))

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

        self.docker_cmd.docker_exec_in_daemon(command)

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
        
        arguments = " ".join(self.arguments.rest_args)
        working_dir = self.env_conf['working_dir']
        for arg in self.arguments.rest_args:
            dir = working_dir + '/' + arg
            if os.path.isdir(dir):
                print(f"Opening jupyter server in `{dir}`")
                break

        jupyter_cmd = "jupyter-notebook " + \
                arguments + " " + \
                f"--ip={self.env_conf['master_ip']} " + \
                "--allow-root " + \
                "--NotebookApp.token="
        self.docker_cmd.docker_exec_in_daemon(jupyter_cmd)

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

    def app(self):
        print("ERROR: Wrong Environment! Try using a `cluster` environment")
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
        if self.docker_cmd.exists():
            self.docker_cmd.docker_exec_in_daemon('rm -rf .COMPSs')
            self.docker_cmd.docker_kill_compss()
        super().env_remove(env_id=env_id)

    def job(self):
        print("ERROR: Wrong Environment! Try using a `cluster` environment")
        exit(1)