#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
This file contains all common functions that the backend scripts that provide
support for PyCOMPSs and Jupyter within Supercomputers.
"""

import os
import shlex
import subprocess

VERBOSE = False   # General Boolean to print detailed information through stdout - For debugging purposes.
DECODING_FORMAT = 'utf-8'
SUCCESS_KEYWORD = 'SUCCESS'
NOT_RUNNING_KEYWORD = 'NOT_RUNING'
ERROR_KEYWORD = 'ERROR'
DISABLED_VALUE = 'undefined'
JOB_NAME_KEYWORD = '-PyCOMPSsInteractive'


def command_runner(cmd, exception=True, cwd=None):
    """
    Run the command defined in the cmd list (for single commands).
    Decodes the stdout and stderr following the DECODING_FORMAT.
    :param cmd: Command to execute as list.
    :param exception: Throw exception if failed. Otherwise, the caller will handle the error.
    :param cwd: Directory where to execute the command
    :return: return code, stdout, stderr
    """
    if VERBOSE:
        print("Executing command: " + ' '.join(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
    stdout, stderr = p.communicate()   # blocks until cmd is done
    return_code = p.returncode
    stdout = stdout.decode(DECODING_FORMAT)
    stderr = stderr.decode(DECODING_FORMAT)
    if exception and return_code != 0:
        _raise_command_exception(cmd, return_code, stdout, stderr)
    return return_code, stdout, stderr


def command_runner_shell(cmd, exception=True, cwd=None):
    """
    Run the command defined in the cmd string.
    Enables to run multiple commands at once (e.g. using &&).
    Decodes the stdout and stderr following the DECODING_FORMAT.
    :param cmd: Command to execute as string.
    :param exception: Throw exception if failed. Otherwise, the caller will handle the error.
    :param cwd: Directory where to execute the command
    :return: return code, stdout, stderr
    """
    if VERBOSE:
        print("Executing command: " + ' '.join(cmd))
    current_env = os.environ.copy()
    p = subprocess.Popen(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         cwd=cwd,
                         env=current_env,
                         shell=True)
    stdout, stderr = p.communicate()   # blocks until cmd is done
    return_code = p.returncode
    stdout = stdout.decode(DECODING_FORMAT)
    stderr = stderr.decode(DECODING_FORMAT)
    if exception and return_code != 0:
        _raise_command_exception(cmd, return_code, stdout, stderr)
    return return_code, stdout, stderr

def get_jupyter_environment_variables():
    """
    Get the jupyter environment variables as dictionary.
    # export JUPYTER_RUNTIME_DIR=$HOME/jupyter-runtime
    :return: Dictionary with the required environment variables.
    """
    return {"JUPYTER_RUNTIME_DIR": os.environ["HOME"] + "/jupyter-runtime"}

def setup_jupyter_environment_only():
    """
    Setup the jupyter environment variables.
    :return: None
    """
    if VERBOSE:
        print("Setting up the jupyter environment...")
    include = get_jupyter_environment_variables()
    _export_environment_variables(None, include)

def setup_supercomputer_configuration():
    """
    Setup the supercomputer configuration.
    Reads the supercomputer cfg and queuing system cfg and exports
    the variables defined in them within the current environment.
    This is useful to get the appropriate commands for the submission,
    cancel, status of jobs, etc. which depends on the specific supercomputer
    relying on the COMPSs configuration.
    Alternatively, it is possible to include a keyword in the command for
    the later substitution from the python code before using it (e.g.
    QUEUE_JOB_NAME_CMD - which is used multiple times).
    :return: None
    """
    if VERBOSE:
        print("Setting up the supercomputer configuration...")
    # Include the jupyter environment variables
    include = get_jupyter_environment_variables()
    # Get the scripts directory
    sc_cfg_name = 'default.cfg'
    sc_cfg = os.path.join(os.environ["COMPSS_HOME"],
                          'Runtime', 'scripts', 'queues', 'supercomputers', sc_cfg_name)
    if VERBOSE:
        print("* Loading SC cfg file")
    _export_environment_variables(sc_cfg, include)
    # Export the environment variables
    qs_cfg_name = str(os.environ['QUEUE_SYSTEM'].strip()) + '.cfg'
    qs_cfg = os.path.join(os.environ["COMPSS_HOME"],
                          'Runtime', 'scripts', 'queues', 'queue_systems', qs_cfg_name)
    if VERBOSE:
        print("* Loading QS cfg file")
    _export_environment_variables(qs_cfg, include)


def is_notebook_job(job_id):
    """
    Checks if the given job id is running a PyCOMPSs notebook.
    To this end, checks the job name to see if it matches the JOB_NAME_KEYWORD.
    :param job_id: Job id to check
    :return: True if is a notebook. False on the contrary
    """
    name = get_job_name(job_id)
    if verify_job_name(name):
        if VERBOSE:
            print("Found notebook id: " + str(job_id))
        return True
    else:
        if VERBOSE:
            print("Job " + str(job_id) + " is not a PyCOMPSs notebook job.")
        return False


def update_command(command, job_id):
    """
    Updates the given command with the necessary job_id.
    Does the replacement of %JOBID% with the job identifier
    :param command: Command to update
    :param job_id: Job identifier
    :return: The updated command as list splitter by spaces
    """
    updated_command = command.replace('%JOBID%', str(job_id)).split()
    return updated_command


def get_job_name(job_id):
    """
    Get the job name of a given job identifier.
    :param job_id: Job identifier
    :return: Job name
    """
    raw_job_name_command = os.environ['QUEUE_JOB_NAME_CMD']
    job_name_command = update_command(raw_job_name_command, str(job_id))
    _, name, _ = command_runner(job_name_command)
    return name.strip()


def verify_job_name(name):
    """
    Verifies if the name provided includes the keyword
    :param name: Name to check
    :return: Boolean
    """
    if JOB_NAME_KEYWORD in name:
        return True
    else:
        return False


def get_job_status(job_id):
    """
    Retrieves the status for the given job identifier
    :param job_id: Job identifier
    :return: The status as string, the return code
    """
    # Get the command to check the status the job
    raw_job_status_command = os.environ['QUEUE_JOB_STATUS_CMD']
    job_status_command = update_command(raw_job_status_command, job_id)
    # Check the status of the job
    return_code, stdout, stderr = command_runner(job_status_command, exception=False)
    # Get the Running tag and check if matches
    running_tag = os.environ['QUEUE_JOB_RUNNING_TAG']
    # Print to provide status to the client
    if return_code != 0:
        job_status = "CHECK FAILED"
    elif stdout.strip() == running_tag:
        job_status = "RUNNING"
    else:
        job_status = str(stdout).strip()
    return job_status, return_code


def not_a_notebook(job_id):
    """
    Prints the not a notebook job message and exit(1).
    :param job_id: Job id
    :return: None
    """
    print(ERROR_KEYWORD)
    print(" - Job Id: " + str(job_id) + " does not belong to a PyCOMPSs interactive job.")
    exit(1)


def _export_environment_variables(env_file=None, include=None):
    """
    Export the environment variables defined in "file".
    Uses bash to parse the file and gets the variables from a clean environment
    to read them and set them in the current environment.
    Includes the "include" variables in the environment if defined, so that
    the sourced configuration files can take them as inputs (e.g. job_id).
    :param env_file: File with the environment variables
    :param include: Dictionary with environment variables to consider in the
                    environment so that the cfgs can complete their contents.
    :return: None
    """
    if include:
        exports = []
        for k, v in include.items():
            exports.append('export ' + k.strip() + '=' + v.strip())
        exports = ' && '.join(exports)
        if env_file:
            command = shlex.split("env -i bash -c 'set -a && " + exports + " && source " + str(env_file) + " && env'")
        else:
            command = shlex.split("env -i bash -c 'set -a && " + exports + " && env'")
    else:
        if env_file:
            command = shlex.split("env -i bash -c 'set -a && source " + str(env_file) + " && env'")
        else:
            raise Exception("Missing at least one parameter: env_file or include")
    if VERBOSE:
        if env_file:
            print("Exporting environment variables from: " + env_file)
        if include:
            print("Exporting environment variables: " + include)
    return_code, stdout, stderr = command_runner(command)
    for line in stdout.splitlines():
        (key, _, value) = line.partition("=")
        if key != '_':
            os.environ[key] = value.strip()


def _raise_command_exception(command, return_code, stdout, stderr):
    """
    Generic exception raiser for command execution.
    :param command: Command that threw the exception
    :param return_code: Return code of the command that failed
    :param stdout: Standard output
    :param stderr: Standard error
    :return: None
    :raises: Exception
    """
    raise Exception(ERROR_KEYWORD + ": Failed execution: " + str(command)
                    + "\nRETURN CODE:" + str(return_code)
                    + "\nSTDOUT:\n" + str(stdout)
                    + "\nSTDERR:\n" + str(stderr))
