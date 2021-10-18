"""
This file contains the actions supported by pycompss-player.
They are invoked from cli/pycompss.py and uses core/cmd.py.
"""

# from pycompss_player.core.docker.cmd import docker_deploy_compss
# from pycompss_player.core.docker.cmd import docker_update_image
# from pycompss_player.core.docker.cmd import docker_kill_compss
# from pycompss_player.core.docker.cmd import docker_exec_in_daemon
# from pycompss_player.core.docker.cmd import docker_start_monitoring
# from pycompss_player.core.docker.cmd import docker_stop_monitoring
# from pycompss_player.core.docker.cmd import docker_components
from pycompss_player.core.actions import Actions
from pycompss_player.core.docker.cmd import DockerCmd

class DockerActions(Actions):

    def __init__(self, arguments, debug=False, env_conf=None) -> None:
        super().__init__(arguments, debug=debug, env_conf=env_conf)

        self.docker_cmd = DockerCmd()

    def init(self):
        super().init()
        """ Deploys COMPSs infrastructure in Docker

        :param arguments: Command line arguments
        :param debug: Debug mode
        :returns: None
        """
        if self.debug:
            print("Initializing...")
            print("Parameters:")
            if self.arguments.working_dir:
                working_dir = self.arguments.working_dir
            else:
                working_dir = "(default)"
            if self.arguments.image:
                image = self.arguments.image
            else:
                image = "(default)"
            print("\t- Working dir: " + working_dir)
            print("\t- Image: " + image)
            print("\t- Restart: " + str(self.arguments.restart))
        self.docker_cmd.docker_deploy_compss(self.arguments.working_dir,
                            self.arguments.image,
                            self.arguments.restart)


    def update(self):
        """ Deploys COMPSs infrastructure in Docker

        :param arguments: Command line arguments
        :param debug: Debug mode
        :returns: None
        """
        if self.debug:
            print("Updating...")
        self.docker_cmd.docker_update_image()


    # def kill(self):
    #     """ Destroys the COMPSs infrastructure in Docker

    #     :param arguments: Command line arguments
    #     :param debug: Debug mode
    #     :returns: None
    #     """
    #     if debug:
    #         print("Killing...")
    #         print("Parameters:")
    #         print("\t- Clean: " + str(arguments.clean))
    #     docker_kill_compss(arguments.clean)


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
        command = self.arguments.command + " ".join(self.arguments.argument)
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
        application = self.arguments.application + " ".join(self.arguments.argument)
        command = "runcompss " + \
                "--project=/project.xml " + \
                "--resources=/resources.xml " + \
                "--master_name=172.17.0.2 " + \
                "--base_log_dir=/home/user " + application
        self.docker_cmd.docker_exec_in_daemon(command)


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
        if self.debug:
            print("Starting jupyter...")
            print("Parameters:")
            print("\t- Arguments: " + str(self.arguments.argument))
        arguments = " ".join(self.arguments.argument)
        command = "jupyter-notebook " + \
                arguments + " " + \
                "--ip=172.17.0.2 " + \
                "--allow-root " + \
                "--NotebookApp.token="
        self.docker_cmd.docker_exec_in_daemon(command)


    def gengraph(self):
        """ Converts the given task dependency graph (dot) into pdf
        using the docker image

        :param arguments: Command line arguments
        :param debug: Debug mode
        :returns: None
        """
        if self.debug:
            print("Converting graph...")
            print("Parameters:")
            print("\t- Dot file: " + self.arguments.dot_file)
        command = "compss_gengraph " + self.arguments.dot_file
        self.docker_cmd.docker_exec_in_daemon(command)


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

    def environment(self):
        super().environment()