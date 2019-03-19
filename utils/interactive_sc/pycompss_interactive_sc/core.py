import os
import sys
import argparse
import subprocess
import time
import signal

from .defaults import VERSION
from .defaults import DECODING_FORMAT
from .defaults import SUBMIT_SCRIPT
from .defaults import CANCEL_SCRIPT
from .defaults import DEFAULT_EXEC_TIME
from .defaults import DEFAULT_JOB_NAME
from .defaults import DEFAULT_NUM_NODES
from .defaults import DEFAULT_USER_NAME
from .defaults import DEFAULT_GET_PASSWORD
from .defaults import DEFAULT_SUPERCOMPUTER
from .defaults import DEFAULT_WEB_BROWSER
from .defaults import DEFAULT_QOS
from .defaults import DEFAULT_TRACING
from .defaults import DEFAULT_PORT_FORWARDING
from .defaults import DEFAULT_CLASSPATH
from .defaults import DEFAULT_PYTHONPATH
from .defaults import DEFAULT_STORAGE_HOME
from .defaults import DISABLED_STORAGE_HOME
from .defaults import DEFAULT_STORAGE_PROPS
from .defaults import DEFAULT_STORAGE
from .defaults import DEFAULT_LOG_LEVEL
# from .defaults import DEFAULT_LOG_LEVEL_ARGUMENT
from .defaults import LOG_LEVEL_DEBUG
from .defaults import LOG_LEVEL_INFO
from .defaults import LOG_LEVEL_OFF
from .defaults import DEFAULT_VERBOSE

from .defaults import DEFAULT_SSH

from .defaults import WARNING_USER_NAME_NOT_PROVIDED

from .defaults import ERROR_CONNECTING
from .defaults import ERROR_COMPSS_NOT_DEFINED
from .defaults import ERROR_SUBMITTING_JOB
from .defaults import ERROR_STORAGE_PROPS
from .defaults import ERROR_UNSUPPORTED_STORAGE_SHORTCUT
from .defaults import ERROR_PORT_FORWARDING
from .defaults import ERROR_BROWSER
from .defaults import ERROR_CANCELLING_JOB


# Globals
parser = None
# Needed for propper cleanup
alive_processes = []
user_name = None
supercomputer = None
scripts_path = None
job_id = None
verbose = False


def _argument_parser():
    """
    Define the argument parser.
    :return: Namespace with the arguments parsed following the argparse setup.
    """
    global parser
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-e', '--exec_time',
                        dest='exec_time',
                        type=int,
                        default=DEFAULT_EXEC_TIME,
                        help='Session duration (minutes)')
    parser.add_argument('-j', '--job_name',
                        dest='job_name',
                        type=str,
                        default=DEFAULT_JOB_NAME,
                        help='Job name')
    parser.add_argument('-n', '--num_nodes',
                        dest='num_nodes',
                        type=int,
                        default=DEFAULT_NUM_NODES,
                        help='Amount of nodes to use')
    parser.add_argument('-u', '--user_name',
                        dest='user_name',
                        type=str,
                        default=DEFAULT_USER_NAME,
                        # required=True,
                        help='User name to login into the supercomputer (mandatory)')
    parser.add_argument('-p', '--password',
                        action='store_true',
                        default=DEFAULT_GET_PASSWORD,
                        dest='password',
                        help='Request user password to login into the supercomputer')
    parser.add_argument('-sc', '--supercomputer',
                        dest='supercomputer',
                        type=str,
                        default=DEFAULT_SUPERCOMPUTER,
                        help='Supercomputer to connec')
    parser.add_argument('-w', '--web_browser',
                        dest='web_browser',
                        type=str,
                        default=DEFAULT_WEB_BROWSER,
                        help='Web browser')
    parser.add_argument('--qos',
                        dest='qos',
                        type=str,
                        default=DEFAULT_QOS,
                        help='Quality of service')
    parser.add_argument('--log_level',
                        dest='log_level',
                        type=str,
                        default=DEFAULT_LOG_LEVEL,
                        choices=[LOG_LEVEL_OFF, LOG_LEVEL_INFO, LOG_LEVEL_DEBUG],
                        help='Set the log level')
    parser.add_argument('-t', '--tracing',
                        action='store_true',
                        default=DEFAULT_TRACING,
                        dest='tracing',
                        help='Enable the tracing environment')
    parser.add_argument('-pf', '--port_forwarding',
                        dest='port_forwarding',
                        type=int,
                        default=DEFAULT_PORT_FORWARDING,
                        help='Set a specific port for port forwarding')
    parser.add_argument('-cp', '--classpath',
                        dest='classpath',
                        type=str,
                        default=DEFAULT_CLASSPATH,
                        help='Path for the application classes / modules')
    parser.add_argument('-pp', '--pythonpath',
                        dest='pythonpath',
                        type=str,
                        default=DEFAULT_PYTHONPATH,
                        help='Additional folders or paths to add to the PYTHONPATH')
    parser.add_argument('-sh', '--storage_home',
                        dest='storage_home',
                        type=str,
                        default=DEFAULT_STORAGE_HOME,
                        help='Absolute path at supercomputer of the storage implementation')
    parser.add_argument('-sp', '--storage_props',
                        dest='storage_props',
                        type=str,
                        default=DEFAULT_STORAGE_PROPS,
                        help='Absolute path at supercomputer of the storage properties file')
    parser.add_argument('-s', '--storage',
                        dest='storage',
                        type=str,
                        default=DEFAULT_STORAGE,
                        help='External storage selection shortcut. \
                              Overrides storage_home and needed classpath/pythonpath flags, \
                              uses storage_props.cfg from home if exists. \
                              Otherwise creates and uses an empty one. \
                              Available options: None | redis')

    ###################################################
    # FINAL PARAMETERS:
    ###################################################
    # TODO: ADD PROJECT FLAG WITH A FILE WITH ALL THESE PARAMETERS
    # TODO: ADD A FLAG TO SUBMIT
    # TODO: ADD A FLAG TO CHECK THE RUNNING NOTEBOOKS STATUS
    # TODO: ADD A FLAG TO CONNECT
    # TODO: ADD A FLAG TO CANCEL
    # TODO: Considering the previos ToDos, check which flags can be removed.
    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        default=DEFAULT_VERBOSE,
                        dest='verbose',
                        help='Show all step messages')
    parser.add_argument('--version', action='version', version='pycompss_interactive_sc ' + VERSION)

    if len(sys.argv) < 2:
        # If the user does not include any argument, show the usage
        print(parser.print_usage())
        sys.exit(1)

    arguments = parser.parse_args()
    return arguments

def signal_handler(sig, frame):
    """
    Signal handler. Acts when CTRL + C is pressed.
    Checks the global variables to see what needs to be cleaned:
        - Alive processes
        - Cancel running job
    """
    global alive_processes
    global user_name
    global supercomputer
    global scripts_path
    global job_id
    global verbose
    if verbose:
        print("\n* Quit!!!")
    if alive_processes:
        if verbose:
            print("\t - Killing all alive processes...")
        for p in alive_processes:
            p.kill()
    # Cancel
    if job_id:
        if verbose:
            print("\t - Cancelling job...")
        _cancel_job(user_name, supercomputer, scripts_path, job_id, verbose)
    print("Finished!")
    sys.exit(0)


def display_error(message):
    """
    Display error in a common format.
    :return: None
    """
    global parser
    print("ERROR: " + message)
    exit(1)


def _check_connectivity(user_name, supercomputer, verbose):
    """
    Check the connectivity with the supercomputer.
    :param user_name: User name
    :param supercomputer: Supercomputer to check
    :param verbose: Boolean to print detailed information through stdout.
    :return: None
    """
    if verbose:
        print("Checking connectivity with " + supercomputer)
    cmd = [DEFAULT_SSH, user_name + '@' + supercomputer,
           '-o', 'PasswordAuthentication=no',
           '-o', 'BatchMode=yes',
           'exit']
    return_code, stdout, stderr = _command_runner(cmd)
    if return_code != 0:
        display_error(ERROR_CONNECTING)
    if verbose:
        print("Connectivity - OK")


def _check_remote_compss(user_name, supercomputer, verbose):
    """
    Check if COMPSs is available in the remote supercomputer and retrieve the
    its installation path.
    This path is used to infer the submit_jupyter_job.sh path.
    :param user_name: User name
    :param supercomputer: Supercomputer to check
    :param verbose: Boolean to print detailed information through stdout.
    :return: Remote COMPSs installation path.
    """
    if verbose:
        print("Checking remote COMPSs installation...")
    cmd = [DEFAULT_SSH, user_name + '@' + supercomputer,
           'which', 'enqueue_compss']
    return_code, stdout, stderr = _command_runner(cmd)
    if stdout == '':
        display_error(ERROR_COMPSS_NOT_DEFINED)
    user_scripts_path = os.path.dirname(stdout.strip())
    # Remove the last 3 folders: Runtime/scripts/user to get the real path
    compss_path = os.path.sep + os.path.join(*(user_scripts_path.split(os.path.sep)[:-3]))
    if verbose:
        print("COMPSs found in: " + str(compss_path))
    return compss_path


def _infer_scripts_path(compss_path, verbose):
    """
    Infer the remote helper scripts path.
    Uses the COMPSs installation path as base and includes the necessary
    folders: Runtime/scripts/system/jupyter
    :param compss_path: Remote COMPSs installation path.
    :param verbose: Boolean to print detailed information through stdout.
    :return: Remote helper scripts path.
    """
    # Append the folders to reach teh helper scripts
    scripts_path = os.path.join(compss_path, 'Runtime', 'scripts', 'system', 'jupyter')
    if verbose:
        print("Using scripts located in: " + str(scripts_path))
    return scripts_path


def _submit_command(user_name, supercomputer, scripts_path, arguments, verbose):
    """
    Submit a new notebook request to the supercomputer.
    :param user_name: User name
    :param supercomputer: Supercomputer to submit
    :param scripts_path: Remote helper scripts path
    :param arguments: Arguments received from command line.
    :return: None
    """
    global job_id
    cmd = [DEFAULT_SSH, user_name + '@' + supercomputer,
           str(os.path.join(scripts_path, SUBMIT_SCRIPT)),   # TODO: THIS CAN BE A SOURCE OF ERROR IN WINDOWS IF USES THE SEPARATOR FROM WINDOWS INSTEAD OF THE REMOTE SEPARATOR
           arguments.job_name,
           str(arguments.exec_time),
           str(arguments.num_nodes),
           arguments.qos,
           arguments.log_level,
           str(arguments.tracing).lower(),
           arguments.classpath,
           arguments.pythonpath,
           arguments.storage_home,
           arguments.storage_props,
           arguments.storage
    ]
    if verbose:
        print("Submitting a new notebook:")
        print("\t - Job name: " + str(arguments.job_name))
        print("\t - Execution time: " + str(arguments.exec_time))
        print("\t - Number of nodes: " + str(arguments.num_nodes))
        print("\t - QoS: " + str(arguments.qos))
        print("\t - Log level: " + str(arguments.log_level))
        print("\t - Tracing: " + str(arguments.tracing))
        print("\t - Classpath: " + str(arguments.classpath))
        print("\t - Pythonpath: " + str(arguments.pythonpath))
        print("\t - Storage home: " + str(arguments.storage_home))
        print("\t - Storage props: " + str(arguments.storage_props))
        print("\t - Storage: " + str(arguments.storage))
    # Launch the submission
    return_code, stdout, stderr = _command_runner(cmd)  # TODO: MUST BE NON BLOCKING
    if return_code != 0:
        print("Return code: " + str(return_code))
        print("Standard OUTPUT:")
        print(stdout)
        print("Standard ERROR:")
        print(stderr)
        display_error(ERROR_SUBMITTING_JOB)
    # Parse the stdout to get the jobid, token and main node
    lines = stdout.splitlines()  # TODO: CAN BE A SOURCE OF ERRORS IN WINDOWS IF LOOKS FOR A DIFFERENT LINE BREAK
    job_info = {}
    for i in lines:
        l = i.split(':')
        job_info[l[0]] = l[1].strip()
    # job_info['JobId'], job_info['MainNode'], job_info['Token']
    if verbose:
        print("Found parameters:")
        for k, v in job_info.items():
            print("\t - " + str(k) + " : " + str(v))
    if job_info['JobId'] == 'FAILED':
        display_error(ERROR_SUBMITTING_JOB)
    else:
        job_id = job_info['JobId']
    return job_info


def _establish_port_forwarding(user_name, supercomputer, port, node, verbose):
    """
    Establish the port forwarding to allow the remote connection with
    the web browser.
    :param user_name: User name
    :param supercomputer: Supercomputer to check
    :param port: Port to use for forwarding
    :param node: Supercomputer node where to do the port forwarding
    :param verbose: Boolean to print detailed information through stdout.
    :return: None
    """
    if verbose:
        print("Establishing port forwarding using port: " + port)
    cmd = [DEFAULT_SSH, user_name + '@' + supercomputer,
           '-L', '8888:localhost:' + port,
           'ssh',  node,
           '-L', port + ':localhost:8888']
    _command_runner(cmd, blocking=False)
    if verbose:
        print("Waiting 5 seconds...")
    time.sleep(5)  # Wait 5 seconds
    if verbose:
        print("Port forwarding - OK")


def _open_the_browser(browser, token, verbose):
    """
    Open the browser with the appropriate web link to the remote notebook.
    :param browser: Browser to open
    :param token: Notebook token
    :param verbose: Boolean to print detailed information through stdout.
    :return: None
    """
    if verbose:
        print("Opening the browser: " + browser)
    cmd = [browser,
           'http://localhost:8888/?token=' + token]
    return_code, stdout, stderr = _command_runner(cmd)
    if return_code != 0:
        print("Return code: " + str(return_code))
        print("Standard OUTPUT:")
        print(stdout)
        print("Standard ERROR:")
        print(stderr)
        display_error(ERROR_BROWSER)
    if verbose:
        print("Browser - OK")


def _cancel_job(user_name, supercomputer, scripts_path, job_id, verbose):
    """
    Cancel a notebook job running in the supercomputer.
    :param user_name: User name
    :param supercomputer: Supercomputer to submit
    :param scripts_path: Path where the remote helper scripts are
    :param job_id: Job identifier
    :param verbose: Boolean to print detailed information through stdout.
    :return: None
    """
    if verbose:
        print("Cancelling job: " + job_id)
    cmd = [DEFAULT_SSH, user_name + '@' + supercomputer,
           str(os.path.join(scripts_path, CANCEL_SCRIPT)),   # TODO: THIS CAN BE A SOURCE OF ERROR IN WINDOWS IF USES THE SEPARATOR FROM WINDOWS INSTEAD OF THE REMOTE SEPARATOR
           job_id]
    return_code, stdout, stderr = _command_runner(cmd)
    if return_code != 0:
        print("Return code: " + str(return_code))
        print("Standard OUTPUT:")
        print(stdout)
        print("Standard ERROR:")
        print(stderr)
        display_error(ERROR_CANCELLING_JOB)
    if verbose:
        print("Job successfully cancelled.")


def _command_runner(cmd, blocking=True):
    """
    Run the command defined in the cmd list.
    Decodes the stdout and stderr following the DECODING_FORMAT.
    :param cmd: Command to execute as list.
    :param blocking: blocks until the subprocess finishes. Otherwise,
                     does not wait and appends the process to the global
                     alive processes list
    :return: return code, stdout, stderr | None if non blocking
    """
    global alive_processes
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if blocking:
        stdout, stderr = p.communicate()   # blocks until cmd is done
        stdout = stdout.decode(DECODING_FORMAT)
        stderr = stderr.decode(DECODING_FORMAT)
        return p.returncode, stdout, stderr
    else:
        alive_processes.append(p)


def main():
    # Globals (needed to be defined for propper cleanup)
    global user_name
    global supercomputer
    global verbose
    global scripts_path

    # Parse command line arguments
    arguments = _argument_parser()

    # Extract the most used arguments
    user_name = arguments.user_name
    supercomputer = arguments.supercomputer
    verbose = arguments.verbose
    port = str(arguments.port_forwarding)
    browser = arguments.web_browser

    # Register signal
    signal.signal(signal.SIGINT, signal_handler)

    # Check conectivity
    _check_connectivity(user_name,
                        supercomputer,
                        verbose)

    # Check remote compss
    compss_path = _check_remote_compss(user_name,
                                       supercomputer,
                                       verbose)

    # Infer remote scripts directory
    scripts_path = _infer_scripts_path(compss_path, verbose)

    # Submit notebook
    job_info = _submit_command(user_name,
                               supercomputer,
                               scripts_path,
                               arguments,
                               verbose)

    # Establish port forwarding
    _establish_port_forwarding(user_name,
                               supercomputer,
                               port,
                               job_info['MainNode'],
                               verbose)

    # Open the web browser
    _open_the_browser(browser, job_info['Token'], verbose)

    # Wait until CTRL + C
    print("Ready to work!")
    print("To force quit: CTRL + C")
    signal.pause()
    # The signal is captured and everything cleaned and canceled (if needed)

if __name__ == '__main__':
    main()
