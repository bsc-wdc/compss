import os
import sys
import argparse
import subprocess
import re
import time
import signal
import getpass

from pycompss_interactive_sc.defaults import INTERPRETER
from pycompss_interactive_sc.defaults import SUBMIT_SCRIPT
from pycompss_interactive_sc.defaults import STATUS_SCRIPT
from pycompss_interactive_sc.defaults import INFO_SCRIPT
from pycompss_interactive_sc.defaults import FIND_SCRIPT
from pycompss_interactive_sc.defaults import CANCEL_SCRIPT

from pycompss_interactive_sc.defaults import VERSION
from pycompss_interactive_sc.defaults import DECODING_FORMAT
from pycompss_interactive_sc.defaults import SUCCESS_KEYWORD
from pycompss_interactive_sc.defaults import NOT_RUNNING_KEYWORD
from pycompss_interactive_sc.defaults import DISABLED_VALUE

from pycompss_interactive_sc.defaults import LOG_LEVEL_DEBUG
from pycompss_interactive_sc.defaults import LOG_LEVEL_INFO
from pycompss_interactive_sc.defaults import LOG_LEVEL_OFF
from pycompss_interactive_sc.defaults import DEFAULT_VERBOSE
from pycompss_interactive_sc.defaults import DEFAULT_CONNECTIVITY_CHECK

from pycompss_interactive_sc.defaults import DEFAULT_PROJECT
from pycompss_interactive_sc.defaults import DEFAULT_CREDENTIALS

from pycompss_interactive_sc.defaults import DEFAULT_SSH
from pycompss_interactive_sc.defaults import DEFAULT_SSHPASS
from pycompss_interactive_sc.defaults import DEFAULT_SSH_WINDOWS

from pycompss_interactive_sc.defaults import WARNING_USER_NAME_NOT_PROVIDED
from pycompss_interactive_sc.defaults import WARNING_NOTEBOOK_NOT_RUNNING
from pycompss_interactive_sc.defaults import WARNING_NO_BROWSER

from pycompss_interactive_sc.defaults import ERROR_UNEXPECTED_PARAMETER
from pycompss_interactive_sc.defaults import ERROR_UNRECOGNIZED_ACTION
from pycompss_interactive_sc.defaults import ERROR_SESSION_NOT_PROVIDED
from pycompss_interactive_sc.defaults import ERROR_CONNECTING
from pycompss_interactive_sc.defaults import ERROR_COMPSS_NOT_DEFINED
from pycompss_interactive_sc.defaults import ERROR_SUBMITTING_JOB
from pycompss_interactive_sc.defaults import ERROR_STATUS_JOB
from pycompss_interactive_sc.defaults import ERROR_INFO_JOB
from pycompss_interactive_sc.defaults import ERROR_STORAGE_PROPS
from pycompss_interactive_sc.defaults import ERROR_UNSUPPORTED_STORAGE_SHORTCUT
from pycompss_interactive_sc.defaults import ERROR_BROWSER
from pycompss_interactive_sc.defaults import ERROR_NO_BROWSER
from pycompss_interactive_sc.defaults import ERROR_CANCELLING_JOB

from pycompss_interactive_sc.defaults import is_windows


# Globals
SSH = DEFAULT_SSH
ALIVE_PROCESSES = []  # Needed for proper cleanup
VERBOSE = False
USER_NAME = None
SUPERCOMPUTER = None
SESSION = None


def _argument_parser():
    """
    Define the argument parser and parse the provided arguments.
    :return: Namespace with the arguments parsed following the argument structure.
    """
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--version',
                        action='version',
                        version='pycompss_interactive_sc ' + VERSION)
    # Parent parser - includes all arguments which are common to all actions
    parent_parser = argparse.ArgumentParser(add_help=False,
                                            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parent_parser.add_argument('--credential',
                               dest='credential',
                               type=str,
                               help='Credential file path. Information to allow the connection. \
                                     (the contents of this file will be overriden with explicit flags)')
    parent_parser.add_argument('-u', '--user_name',
                               dest='user_name',
                               type=str,
                               default=DEFAULT_CREDENTIALS['user_name'],
                               # required=True,
                               help='User name to login into the supercomputer (mandatory)')
    parent_parser.add_argument('-p', '--password',
                               action='store_true',
                               dest='password',
                               default=DEFAULT_CREDENTIALS['password'],
                               help='Request user password to login into the supercomputer (requires sshpass)')
    parent_parser.add_argument('-sc', '--supercomputer',
                               dest='supercomputer',
                               type=str,
                               default=DEFAULT_CREDENTIALS['supercomputer'],
                               help='Supercomputer to connect')
    parent_parser.add_argument('-s', '--session',
                               dest='session',
                               type=str,
                               default=DEFAULT_CREDENTIALS['session'],
                               help='Session to use to connect')
    parent_parser.add_argument('-pf', '--port_forward',
                               dest='port_forward',
                               type=str,
                               default=DEFAULT_CREDENTIALS['port_forward'],
                               help='Port to establish the port forwarding')
    parent_parser.add_argument('-dcc', '--disable_connectivity_check',
                               action='store_false',
                               dest='connectivity_check',
                               default=DEFAULT_CONNECTIVITY_CHECK,
                               help='Do not perform connectivity checks.')
    parent_parser.add_argument('-v', '--verbose',
                               action='store_true',
                               dest='verbose',
                               default=DEFAULT_VERBOSE,
                               help='Show all step messages')
    # Submit sub-parser
    subparsers = parser.add_subparsers(dest='action')  # help='Sub-command help'
    parser_submit = subparsers.add_parser('submit',
                                          help='Submit a new job.',
                                          parents=[parent_parser],
                                          formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_submit.add_argument('--project',
                               dest='project',
                               type=str,
                               help='Project file path. Job related information. \
                                     (The contents of this file will be overriden with explicit flags)')
    parser_submit.add_argument('-e', '--exec_time',
                               dest='exec_time',
                               type=int,
                               default=DEFAULT_PROJECT['exec_time'],
                               help='Session duration (minutes)')
    parser_submit.add_argument('-j', '--job_name',
                               dest='job_name',
                               type=str,
                               default=DEFAULT_PROJECT['job_name'],
                               help='Job name')
    parser_submit.add_argument('-n', '--num_nodes',
                               dest='num_nodes',
                               type=int,
                               default=DEFAULT_PROJECT['num_nodes'],
                               help='Amount of nodes to use')
    parser_submit.add_argument('--qos',
                               dest='qos',
                               type=str,
                               default=DEFAULT_PROJECT['qos'],
                               help='Quality of service')
    parser_submit.add_argument('--log_level',
                               dest='log_level',
                               type=str,
                               default=DEFAULT_PROJECT['log_level'],
                               choices=[LOG_LEVEL_OFF, LOG_LEVEL_INFO, LOG_LEVEL_DEBUG],
                               help='Set the log level')
    parser_submit.add_argument('-t', '--tracing',
                               action='store_true',
                               default=DEFAULT_PROJECT['tracing'],
                               dest='tracing',
                               help='Enable the tracing environment')
    parser_submit.add_argument('-cp', '--classpath',
                               dest='classpath',
                               type=str,
                               default=DEFAULT_PROJECT['classpath'],
                               help='Path for the application classes / modules')
    parser_submit.add_argument('-pp', '--pythonpath',
                               dest='pythonpath',
                               type=str,
                               default=DEFAULT_PROJECT['pythonpath'],
                               help='Additional folders or paths to add to the PYTHONPATH')
    parser_submit.add_argument('-sth', '--storage_home',
                               dest='storage_home',
                               type=str,
                               default=DEFAULT_PROJECT['storage_home'],
                               help='Absolute path at supercomputer of the storage implementation')
    parser_submit.add_argument('-stp', '--storage_props',
                               dest='storage_props',
                               type=str,
                               default=DEFAULT_PROJECT['storage_props'],
                               help='Absolute path at supercomputer of the storage properties file')
    parser_submit.add_argument('-st', '--storage',
                               dest='storage',
                               type=str,
                               default=DEFAULT_PROJECT['storage'],
                               choices=[DEFAULT_PROJECT['storage'], 'redis'],
                               help='External storage selection shortcut. \
                                     Overrides storage_home and needed classpath/pythonpath flags, \
                                     uses storage_props.cfg from home if exists. \
                                     Otherwise creates and uses an empty one. \
                                     Available options: None | redis')
    # Status sub-parser
    parser_status = subparsers.add_parser('status',
                                          help='Check job status.',
                                          parents=[parent_parser],
                                          formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_status.add_argument(dest='job_id',
                               type=str,
                               help='Job identifier')
    # Connect sub-parser
    parser_connect = subparsers.add_parser('connect',
                                           help='Connect to a job.',
                                           parents=[parent_parser],
                                           formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_connect.add_argument(dest='job_id',
                                type=str,
                                help='Job identifier')
    parser_connect.add_argument('-wb', '--web_browser',
                                dest='web_browser',
                                type=str,
                                default=DEFAULT_CREDENTIALS['web_browser'],
                                help='Web browser')
    parser_connect.add_argument('-nwb', '--no_web_browser',
                                dest='no_web_browser',
                                action='store_true',
                                default=DEFAULT_CREDENTIALS['no_web_browser'],
                                help='Disable web browser starting. Shows the connection url instead.')
    # List sub-parser - does not have specific arguments
    subparsers.add_parser('list',
                          help='Show all existing jobs.',
                          parents=[parent_parser],
                          formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # Cancel sub-parser
    parser_cancel = subparsers.add_parser('cancel',
                                          help='Cancel job.',
                                          parents=[parent_parser],
                                          formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_cancel.add_argument(dest='job_id',
                               type=str,
                               nargs='+',
                               help='Job identifier')
    # Information
    parser_template = subparsers.add_parser('template',
                                            help='Shows an example of the requested template.',
                                            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_template.add_argument(dest='template',
                                 type=str,
                                 choices=['project', 'credential'],
                                 help='Template file.')

    # Check if the user does not include any argument
    if len(sys.argv) < 2:
        #  Show the usage
        print(parser.print_usage())
        sys.exit(1)

    arguments = parser.parse_args()

    # Check if the user does not include any argument after the action
    raw_argv = ' '.join(sys.argv)
    if not raw_argv.split(arguments.action, 1)[1]:
        print(subparsers.choices[arguments.action].print_usage())
        sys.exit(1)

    return arguments


def _parse_file(project):
    """
    Parse the project file and return a dictionary with its contents.
    Takes the default project and credential keys defined in defaults to
    extract the information.
    Ignores empty lines or which start with #.
    :param project: Project file to parse
    :return: Dictionary with the contents
    """
    reserved_keywords = list(DEFAULT_PROJECT.keys()) + list(DEFAULT_CREDENTIALS.keys())
    with open(project, 'r') as f:
        data = f.read()
    lines = data.splitlines()
    content = {}
    for l in lines:
        if l and not l.startswith('#'):
            k, v = l.split('=')
            if k not in reserved_keywords:
                # Control the variables defined in the document
                display_error(ERROR_UNEXPECTED_PARAMETER + k)
            content[k.strip()] = v.strip()
        else:
            # It is a comment or empty
            pass
    return content


def _argument_checks(arguments):
    """
    Do any needed argument check and apply the defined shortcuts.
    :param arguments: Parsed arguments
    :return: Updated arguments.
    """
    # Check if user wants a template
    if arguments.action == 'template':
        path, _ = os.path.split(os.path.abspath(__file__))
        base_path = os.path.join(path, '..')
        if arguments.template == 'project':
            # Display project template.
            project_path = os.path.join(base_path, 'project.example')
            with open(project_path, 'r') as f:
                print(f.read())
        if arguments.template == 'credential':
            # Display credential template.
            credential_path = os.path.join(base_path, 'credential.example')
            with open(credential_path, 'r') as f:
                print(f.read())
        exit(0)

    # Load job and credential files, and check which ones to keep (flags override their content)
    # Also perform the necessary checks to the arguments
    args = vars(arguments)  # This gives the namespace as dict for direct access.
    if arguments.action == 'submit':
        if arguments.project is not None:
            # The user has specified a file where some of the parameters may be defined
            project_params = _parse_file(arguments.project)
            for k, v in args.items():
                # If what is defined in the arguments is default and exist in
                # the project file, take the value from the project file
                if k in DEFAULT_PROJECT.keys() and \
                        v == DEFAULT_PROJECT[k] and \
                        k in project_params.keys():
                    args[k] = project_params[k]
        # Perform specific checks (currently check storage shortcut)
        arguments = _submit_argument_checks(arguments)
    # Arguments credential is always needed
    if arguments.credential is not None:
        # The user has specified a file where the credential parameters are defined
        credential_params = _parse_file(arguments.credential)
        for k, v in args.items():
            # If what is defined in the arguments is default and exist in
            # the credential file, take the value from the project file
            if k in DEFAULT_CREDENTIALS.keys() and \
                    v == DEFAULT_CREDENTIALS[k] and \
                    k in credential_params.keys():
                args[k] = credential_params[k]
    # Final argument checks
    _generic_argument_checks(arguments)

    return arguments


def _submit_argument_checks(arguments):
    """
    Perform the needed arguments check for the submit action
    :param arguments: Command line arguments parsed
    :return: Updated arguments
    """
    # Check if storage home is defined and no storage props
    if arguments.storage_home != DISABLED_VALUE and \
       arguments.storage_props == DISABLED_VALUE:
        display_error(ERROR_STORAGE_PROPS)

    # Check if storage shortcut is defined and override
    # storage props and storage home
    if arguments.storage == DEFAULT_PROJECT['storage'] or arguments.storage == 'redis':
        arguments.storage_home = DISABLED_VALUE
        arguments.storage_props = DISABLED_VALUE
    else:
        # Unreachable code if choices is set in the argument parser
        display_error(ERROR_UNSUPPORTED_STORAGE_SHORTCUT)

    return arguments


def _generic_argument_checks(arguments):
    """
    Generic argument checks
    :param arguments: Command line arguments parsed
    :return: None
    """
    global SSH
    if is_windows():
        # Check that the user has provided a session since it is mandatory.
        if arguments.session == DEFAULT_CREDENTIALS['session']:
            display_error(ERROR_SESSION_NOT_PROVIDED)

        # Set the default SSH with DEFAULT_SSH_WINDOWS
        SSH = DEFAULT_SSH_WINDOWS
    else:
        # Check if the user has defined the user name
        if arguments.user_name == DEFAULT_CREDENTIALS['user_name']:
            display_warning(WARNING_USER_NAME_NOT_PROVIDED)

        # Check if the password has to be introduced manually
        if arguments.password:
            password = getpass.getpass(prompt='Password:')
            os.environ['SSHPASS'] = password
            SSH = DEFAULT_SSHPASS


def signal_handler(sig, frame):
    """
    Signal handler. Acts when CTRL + C is pressed.
    Checks the global variables to see what needs to be cleaned:
        - Alive processes
        - Cancel running job
    """
    global ALIVE_PROCESSES
    if VERBOSE:
        print("\n* Quit!!!")
    if ALIVE_PROCESSES:
        if VERBOSE:
            print(" - Killing all alive processes...")
        for p in ALIVE_PROCESSES:
            p.kill()
    # # Cancel # Not cancelling here... wait for the user to cancel it explicitly.
    # # If needed, the following information needs to be global
    # global user_name
    # global supercomputer
    # global scripts_path
    # global job_id
    # global verbose
    # if job_id:
    #     if VERBOSE:
    #         print("\t - Cancelling job...")
    #     _cancel_job(user_name, supercomputer, scripts_path, job_id, verbose)
    # else:
    #     display_warning(WARNING_JOB_CANCELLED)
    print("Finished!")
    sys.exit(0)


def display_error(message, return_code=None, stdout=None, stderr=None):
    """
    Display error in a common format.
    :return: None
    """
    # # Hide the client stack trace and show only the prints from remote when fails.
    # if return_code:
    #     print("Return code: " + str(return_code))
    # if stdout:
    #     print("Standard OUTPUT:")
    #     print(stdout)
    if stderr:
        print("Standard ERROR:")
        print(stderr)
    print("ERROR: " + message)
    exit(1)


def display_warning(message):
    """
    Display warning in a common format.
    :return: None
    """
    print("WARNING: " + message)


def _check_connectivity():
    """
    Check the connectivity with the supercomputer.
    :return: None
    """
    if VERBOSE:
        print("Checking connectivity...")

    if is_windows():
        cmd = ['exit']
    else:
        cmd = ['-o', 'PasswordAuthentication=no',
               '-o', 'BatchMode=yes',
               'exit']
    return_code, stdout, stderr = _command_runner(cmd)

    if return_code != 0:
        display_error(ERROR_CONNECTING, return_code, stdout, stderr)
    if VERBOSE:
        print("Connectivity - OK")


def _check_remote_compss():
    """
    Check if COMPSs is available in the remote supercomputer and retrieve the
    its installation path.
    This path is used to infer the submit_jupyter_job.sh path.
    :return: Remote COMPSs installation path.
    """
    if VERBOSE:
        print("Checking remote COMPSs installation...")
    cmd = ['which', 'enqueue_compss']
    return_code, stdout, stderr = _command_runner(cmd)
    if return_code != 0:
        display_error(ERROR_CONNECTING, return_code, stdout, stderr)
    if stdout == '':
        display_error(ERROR_COMPSS_NOT_DEFINED)
    user_scripts_path = os.path.dirname(stdout.strip())
    # Remove the last 3 folders: Runtime/scripts/user to get the real path
    compss_path = '/'.join(user_scripts_path.split('/')[:-3])
    if VERBOSE:
        print("COMPSs found in: " + str(compss_path))
    return compss_path


def _infer_scripts_path(compss_path):
    """
    Infer the remote helper scripts path.
    Uses the COMPSs installation path as base and includes the necessary
    folders: Runtime/scripts/system/jupyter
    :param compss_path: Remote COMPSs installation path.
    :return: Remote helper scripts path.
    """
    # Append the folders to reach teh helper scripts
    scripts_path = compss_path + '/Runtime/scripts/system/jupyter'
    if VERBOSE:
        print("Using scripts located in: " + str(scripts_path))
    return scripts_path


def _submit_job(scripts_path, arguments):
    """
    Submit a new notebook request to the supercomputer.
    :param scripts_path: Remote helper scripts path
    :param arguments: Arguments received from command line.
    :return: None
    """
    cmd = [INTERPRETER,
           str(scripts_path + '/' + SUBMIT_SCRIPT),
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
           arguments.storage]
    if VERBOSE:
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
    return_code, stdout, stderr = _command_runner(cmd)
    if return_code != 0:
        display_error(ERROR_SUBMITTING_JOB, return_code, stdout, stderr)
    else:
        print("Successfully submitted.")


def _job_status(scripts_path, job_id):
    """
    Checks the status of a job in the supercomputer.
    :param scripts_path: Remote helper scripts path
    :param job_id: Job identifier
    :return: None
    """
    if VERBOSE:
        print("Checking status of job: " + job_id)
    cmd = [INTERPRETER,
           str(scripts_path + '/' + STATUS_SCRIPT),
           job_id]
    return_code, stdout, stderr = _command_runner(cmd)
    if return_code != 0:
        display_error(ERROR_STATUS_JOB, return_code, stdout, stderr)

    # Parse the output for fancy printing
    out = stdout.splitlines()
    if out[0] == SUCCESS_KEYWORD:
        status = out[1].split(':')[1]
        print("Job Status: " + str(status))
    else:
        display_error(ERROR_STATUS_JOB, return_code, stdout, stderr)

    if VERBOSE:
        print("Job status finished.")


def _job_list(scripts_path):
    """
    Checks the list of available jobs in the supercomputer.
    :param scripts_path: Remote helper scripts path
    :return: None
    """
    if VERBOSE:
        print("Getting the list of jobs")
    cmd = [INTERPRETER,
           str(scripts_path + '/' + FIND_SCRIPT)]
    return_code, stdout, stderr = _command_runner(cmd)
    if return_code != 0:
        display_error(ERROR_STATUS_JOB, return_code, stdout, stderr)

    # Parse the output for fancy printing
    out = stdout.splitlines()
    if out[0] == SUCCESS_KEYWORD:
        print("Available notebooks: ")
        for job_id in out[1:]:
            print(job_id)
    else:
        display_error(ERROR_STATUS_JOB, return_code, stdout, stderr)

    if VERBOSE:
        print("Job list finished.")


def _connect_job(scripts_path, arguments):
    """
    Establish the connection with an existing notebook.
    :param scripts_path: Remote helper scripts path
    :param arguments: Arguments received from command line.
    :return: None
    """
    # First register the signal (the connection will be ready until CTRL+C)
    signal.signal(signal.SIGINT, signal_handler)

    # Second, get information about the job (node and token)
    node = None
    token = None
    if VERBOSE:
        print("Getting information of job: " + arguments.job_id)
    cmd = [INTERPRETER,
           str(scripts_path + '/' + INFO_SCRIPT),
           arguments.job_id]
    return_code, stdout, stderr = _command_runner(cmd)
    if return_code != 0:
        display_error(ERROR_INFO_JOB, return_code, stdout, stderr)

    # Parse the output
    out = stdout.splitlines()
    if out[0] == NOT_RUNNING_KEYWORD:
        display_warning(WARNING_NOTEBOOK_NOT_RUNNING)
        exit(0)
    elif out[0] == SUCCESS_KEYWORD:
        for i in out[1:]:
            line = i.split(':')
            if line[0] == 'MASTER':
                node = line[1]
            elif line[0] == 'SERVER':
                server_out = ' '.join(line[1:])
                raw_token = re.search("token=\w*", server_out).group(0)
                token = raw_token.split('=')[1]
        if VERBOSE:
            print("Job info:")
            print(" - Node: " + str(node))
            print(" - Token: " + str(token))
    else:
        display_error(ERROR_INFO_JOB, return_code, stdout, stderr)

    if VERBOSE:
        print("Finished getting information.")

    # Third, establish port forward
    if VERBOSE:
        print("Establishing port forwarding using port: " + arguments.port_forward)
    cmd = ['-L', '8888:localhost:' + arguments.port_forward,
           'ssh', node,
           '-L', arguments.port_forward + ':localhost:8888']
    _command_runner(cmd, blocking=False)
    if VERBOSE:
        print("Waiting 5 seconds...")
    time.sleep(5)  # Wait 5 seconds
    if VERBOSE:
        print("Port forwarding ready.")

    # Fourth, check if using the specified web browser or simply show the url
    if type(arguments.no_web_browser) is str:
        # It can be a str if overridden in the credential file
        if arguments.no_web_browser == 'True':
            no_web_browser = True
        elif arguments.no_web_browser == 'False':
            no_web_browser = False
        else:
            display_warning(WARNING_NO_BROWSER)
            no_web_browser = False
    elif type(arguments.no_web_browser) is bool:
        no_web_browser = arguments.no_web_browser
    else:
        display_error(ERROR_NO_BROWSER)
    if no_web_browser:
        if VERBOSE:
            print("Disabled web browser opening")
        print("Connection established. Please use the following URL to connect to the job.")
        print("\t http://localhost:8888/?token=" + token)
    else:
        if VERBOSE:
            print("Opening the browser: " + arguments.web_browser)
        if is_windows():
            cmd = ['cmd', '/c', 'start', arguments.web_browser]
        else:
            cmd = [arguments.web_browser]
        cmd = cmd + ["http://localhost:8888/?token=" + token]
        return_code, stdout, stderr = _command_runner(cmd, remote=False)
        if return_code != 0:
            message = ERROR_BROWSER + '\n\n' \
                      + "Alternatively, please use the following URL to connect to the job.\n" \
                      + "\t http://localhost:8888/?token=" + token
            display_error(message, return_code, stdout, stderr)
    if VERBOSE:
        print("Connected to job")

    # Finally, wait for the CTRL+C signal
    print("Ready to work!")
    print("To force quit: CTRL + C")
    if is_windows():
        while True:
            # Waiting for signal
            try:
                time.sleep(5)
            except IOError:
                pass
    else:
        signal.pause()
    # The signal is captured and everything cleaned and canceled (if needed)


def _cancel_job(scripts_path, job_ids):
    """
    Cancel a list of notebook jobs running in the supercomputer.
    :param scripts_path: Path where the remote helper scripts are
    :param job_ids: List of job identifiers
    :return: None
    """
    if VERBOSE:
        print("Cancelling jobs:")
        for job in job_ids:
            print(" - " + str(job))
    cmd = [INTERPRETER,
           str(scripts_path + '/' + CANCEL_SCRIPT)] + job_ids
    return_code, stdout, stderr = _command_runner(cmd)
    if return_code != 0:
        display_error(ERROR_CANCELLING_JOB, return_code, stdout, stderr)

    # Parse the output
    out = stdout.splitlines()
    if out[0] == SUCCESS_KEYWORD:
        print("Jobs successfully cancelled.")
    else:
        display_error(ERROR_CANCELLING_JOB, return_code, stdout, stderr)

    if VERBOSE:
        print("Finished cancelling.")


def _command_runner(cmd, blocking=True, remote=True):
    """
    Run the command defined in the cmd list.
    Decodes the stdout and stderr following the DECODING_FORMAT.
    :param cmd: Command to execute as list.
    :param blocking: blocks until the subprocess finishes. Otherwise,
                     does not wait and appends the process to the global
                     alive processes list
    :param remote: Enable/Disable the execution in the remote supercomputer.
                   By default, prepend the needed SSH command.
                   (Uses the globals SSH, SESSION, USER_NAME and SUPERCOMPUTER).
    :return: return code, stdout, stderr | None if non blocking
    """
    global ALIVE_PROCESSES

    if remote:
        # Prepend the needed ssh command
        if is_windows():
            cmd = SSH.split() + [SESSION] + cmd
        else:
            cmd = SSH.split() + [USER_NAME + '@' + SUPERCOMPUTER] + cmd
    else:
        # Execute the command as requested
        pass

    if VERBOSE:
        print("Executing command: ")
        print(' '.join(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if blocking:
        stdout, stderr = p.communicate()   # blocks until cmd is done
        stdout = stdout.decode(DECODING_FORMAT)
        stderr = stderr.decode(DECODING_FORMAT)
        return p.returncode, stdout, stderr
    else:
        ALIVE_PROCESSES.append(p)


def main():
    """
    Main client code. Does the following steps:
        - Parses the arguments,
        - Checks connectivity.
        - Gets the remote COMPSs installation path.
        - Implements the action selector.
    :return: None
    """
    # Globals
    global VERBOSE
    global USER_NAME
    global SUPERCOMPUTER
    global SESSION

    # Parse command line arguments
    arguments = _argument_parser()

    # Do any argument check/update if needed
    arguments = _argument_checks(arguments)

    # Extract the most used arguments
    USER_NAME = arguments.user_name
    SUPERCOMPUTER = arguments.supercomputer
    SESSION = arguments.session
    VERBOSE = arguments.verbose

    if arguments.connectivity_check:
        # Check connectivity - default=True
        _check_connectivity()

    # Check remote COMPSs
    compss_path = _check_remote_compss()

    # Infer remote scripts directory
    scripts_path = _infer_scripts_path(compss_path)

    # Action selection
    if arguments.action == 'submit':
        # Submit a new job to the supercomputer.
        print("Submitting a new job...")
        _submit_job(scripts_path, arguments)
    elif arguments.action == 'status':
        # Check the status of a notebook.
        print("Checking the status...")
        _job_status(scripts_path, arguments.job_id)
    elif arguments.action == 'list':
        # Check the notebooks submitted to the supercomputer and their status.
        print("Looking for available jobs...")
        _job_list(scripts_path)
    elif arguments.action == 'connect':
        # Connect to a running notebook.
        print("Connecting...")
        _connect_job(scripts_path, arguments)
    elif arguments.action == 'cancel':
        # Cancel a notebook.
        print("Cancelling jobs...")
        _cancel_job(scripts_path, arguments.job_id)
    else:
        # Unreachable code since it is checked with argparse
        # but kept for completion.
        display_error(ERROR_UNRECOGNIZED_ACTION + arguments.action)


if __name__ == '__main__':
    main()
