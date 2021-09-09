"""
This file contains the actions supported by pycompss-player.
They are invoked from cli/pycompss.py and uses core/cmd.py.
"""

from pycompss_player.core.cmd import docker_deploy_compss
from pycompss_player.core.cmd import docker_update_image
from pycompss_player.core.cmd import docker_kill_compss
from pycompss_player.core.cmd import docker_exec_in_daemon
from pycompss_player.core.cmd import docker_start_monitoring
from pycompss_player.core.cmd import docker_stop_monitoring
from pycompss_player.core.cmd import docker_components


def init(arguments, debug=False):
    """ Deploys COMPSs infrastructure in Docker

    :param arguments: Command line arguments
    :param debug: Debug mode
    :returns: None
    """
    if debug:
        print("Initializing...")
        print("Parameters:")
        if arguments.working_dir:
            working_dir = arguments.working_dir
        else:
            working_dir = "(default)"
        if arguments.image:
            image = arguments.image
        else:
            image = "(default)"
        print("\t- Working dir: " + working_dir)
        print("\t- Image: " + image)
        print("\t- Restart: " + str(arguments.restart))
    docker_deploy_compss(arguments.working_dir,
                         arguments.image,
                         arguments.restart)


def update(arguments, debug=False):
    """ Deploys COMPSs infrastructure in Docker

    :param arguments: Command line arguments
    :param debug: Debug mode
    :returns: None
    """
    if debug:
        print("Updating...")
    docker_update_image()


def kill(arguments, debug=False):
    """ Destroys the COMPSs infrastructure in Docker

    :param arguments: Command line arguments
    :param debug: Debug mode
    :returns: None
    """
    if debug:
        print("Killing...")
        print("Parameters:")
        print("\t- Clean: " + str(arguments.clean))
    docker_kill_compss(arguments.clean)


def exec(arguments, debug=False):
    """ Execute the given command in the running Docker image

    :param arguments: Command line arguments
    :param debug: Debug mode
    :returns: None
    """
    if debug:
        print("Executing...")
        print("Parameters:")
        print("\t- Command: " + arguments.command)
        print("\t- Arguments: " + str(arguments.argument))
    command = arguments.command + " ".join(arguments.argument)
    docker_exec_in_daemon(command)


def run(arguments, debug=False):
    """ Run the given command in the COMPSs infrastructure at docker

    :param arguments: Command line arguments
    :param debug: Debug mode
    :returns: None
    """
    if debug:
        print("Running...")
        print("Parameters:")
        print("\t- Application: " + arguments.application)
        print("\t- Arguments: " + str(arguments.argument))
    application = arguments.application + " ".join(arguments.argument)
    command = "runcompss " + \
              "--project=/project.xml " + \
              "--resources=/resources.xml " + \
              "--master_name=172.17.0.2 " + \
              "--base_log_dir=/home/user " + application
    docker_exec_in_daemon(command)


def monitor(arguments, debug=False):
    """ Starts or stops the monitor in the COMPSs infrastructure at docker

    :param arguments: Command line arguments
    :param debug: Debug mode
    :returns: None
    """
    if debug:
        print("Monitoring...")
        print("Parameters:")
        print("\t- Option: " + arguments.option)
    if arguments.option == "start":
        docker_start_monitoring()
    elif arguments.option == "stop":
        docker_stop_monitoring()
    else:
        raise Exception("Unexpected monitor option: " + arguments.option)


def jupyter(arguments, debug=False):
    """ Starts jupyter in the COMPSs infrastructure at docker

    :param arguments: Command line arguments
    :param debug: Debug mode
    :returns: None
    """
    if debug:
        print("Starting jupyter...")
        print("Parameters:")
        print("\t- Arguments: " + str(arguments.argument))
    arguments = " ".join(arguments.argument)
    command = "jupyter-notebook " + \
              arguments + " " + \
              "--ip=172.17.0.2 " + \
              "--allow-root " + \
              "--NotebookApp.token="
    docker_exec_in_daemon(command)


def gengraph(arguments, debug=False):
    """ Converts the given task dependency graph (dot) into pdf
    using the docker image

    :param arguments: Command line arguments
    :param debug: Debug mode
    :returns: None
    """
    if debug:
        print("Converting graph...")
        print("Parameters:")
        print("\t- Dot file: " + arguments.dot_file)
    command = "compss_gengraph " + arguments.dot_file
    docker_exec_in_daemon(command)

def components(arguments, debug=False):
    """ Lists/add/remove workers in the COMPSs infrastructure at docker

    :param arguments: Command line arguments
    :param debug: Debug mode
    :returns: None
    """
    if debug:
        print("Components: " + arguments.components)

    if arguments.components == "list":
        if debug:
            print("Listing components...")
        docker_components(arguments.components)
    if arguments.components == "add":
        if debug:
            print("Adding components: " + str(arguments.worker))
        docker_components(arguments.components,
                          arguments.add,
                          arguments.worker)
    if arguments.components == "remove":
        if debug:
            print("Removing components:" + str(arguments.worker))
        docker_components(arguments.components,
                          arguments.remove,
                          arguments.worker)
