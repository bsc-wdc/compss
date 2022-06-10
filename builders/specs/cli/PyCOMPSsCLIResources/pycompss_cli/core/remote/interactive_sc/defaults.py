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
import os
import getpass

# --------------------------------------------------
# PROGRAM
# --------------------------------------------------
VERSION = '0.1.0'

# --------------------------------------------------
# BACKEND SCRIPTS
# --------------------------------------------------
INTERPRETER = 'python'
SUBMIT_SCRIPT = 'submit.py'
STATUS_SCRIPT = 'status.py'
INFO_SCRIPT = 'info.py'
FIND_SCRIPT = 'find.py'
CANCEL_SCRIPT = 'cancel.py'

# --------------------------------------------------
# COMMON TAGS (backend and client)
# CAUTION! These tags must match with the ones
#          defined in backend commons.py
# --------------------------------------------------
DECODING_FORMAT = 'utf-8'
SUCCESS_KEYWORD = 'SUCCESS'
NOT_RUNNING_KEYWORD = 'NOT_RUNING'
ERROR_KEYWORD = 'ERROR'
DISABLED_VALUE = 'undefined'

# --------------------------------------------------
# ARGUMENTS
# --------------------------------------------------

LOG_LEVEL_DEBUG = 'debug'
LOG_LEVEL_INFO = 'info'
LOG_LEVEL_OFF = 'off'
DEFAULT_VERBOSE = False
DEFAULT_CONNECTIVITY_CHECK = True

DEFAULT_USER_NAME = str(getpass.getuser())

# Matches the same key as dest in arguments parsers
DEFAULT_PROJECT = {'exec_time': 5,  # 5 minute
                   'job_name': 'Jupyter',
                   'num_nodes': 2,
                   'qos': 'debug',
                   'log_level': LOG_LEVEL_OFF,
                   'tracing': False,
                   'classpath': '.',
                   'pythonpath': '.',
                   'notebook_path': DISABLED_VALUE,  # if not defined, use remote HOME
                   'storage_home': DISABLED_VALUE,
                   'storage_props': DISABLED_VALUE,
                   'storage': 'None'}

DEFAULT_CREDENTIALS = {'user_name': DEFAULT_USER_NAME,
                       'password': False,
                       'supercomputer': 'mn2.bsc.es',
                       'session': DISABLED_VALUE,
                       'port_forward': '3333',
                       'web_browser': 'firefox',
                       'no_web_browser': False}

# --------------------------------------------------
# SCRIPT TOOLS
# --------------------------------------------------
DEFAULT_SSH = 'ssh'  # When password provided, use sshpass -e before ssh
DEFAULT_SSHPASS = 'sshpass -e ' + DEFAULT_SSH
DEFAULT_SSH_WINDOWS = 'plink -batch'

# --------------------------------------------------
# SCRIPT VARIABLES
# --------------------------------------------------
CONNECTION_URL = "http://localhost:8888/?token="

# --------------------------------------------------
# INFO CONSTANTS DECLARATION
# --------------------------------------------------
INFO_CONNECTION_ESTABLISHED = "Connection established. Please use the following URL to connect to the job."

# --------------------------------------------------
# WARNING CONSTANTS DECLARATION
# --------------------------------------------------
WARNING_USER_NAME_NOT_PROVIDED = "Username not provided. Using: " + DEFAULT_USER_NAME
WARNING_NOTEBOOK_NOT_RUNNING = "The notebook is not running!"
WARNING_NO_BROWSER = "Unexpected no_web_broser value found.\nPlease use True or False. Now using False."

# --------------------------------------------------
# ERROR CONSTANTS DECLARATION
# --------------------------------------------------
ERROR_UNEXPECTED_PARAMETER = "Unexpected parameter in project file: "
ERROR_UNRECOGNIZED_ACTION = "Unrecognized action: "
ERROR_SESSION_NOT_PROVIDED = "Session not provided. Please provide one to connect."
ERROR_CONNECTING = "Could not connect to the supercomputer.\n" \
                   "Please check the connectivity, user name and password."
ERROR_COMPSS_NOT_DEFINED = "COMPSs is not available in the supercomputer.\n" \
                           "Please check that you have properly configured the module load in your .bashrc."
ERROR_SUBMITTING_JOB = "Could not submit the job to the supercomputer.\n" \
                       "Please check that you are able to submit jobs to the supercomputer."
ERROR_STATUS_JOB = "Could not check the status of the job."
ERROR_INFO_JOB = "Could not get the information of the job."
ERROR_STORAGE_PROPS = "--storage_props flag not defined."
ERROR_UNSUPPORTED_STORAGE_SHORTCUT = "Non supported external storage shortcut."
ERROR_JUPYTER_SERVER = "The notebook server is not running."
ERROR_BROWSER = "Could not open the browser. Please check that you have defined properly the browser to use."
ERROR_NO_BROWSER = "Wrong no_web_browser flag type!"
ERROR_CANCELLING_JOB = "Could not cancel the job. Please, cancel it manually."
ERROR_FORWARD_PORT = "Could not forward the port. Please check that you have defined properly the port to forward."

# --------------------------------------------------
# FUNCTIONS
# --------------------------------------------------

def is_windows():
    """ Checks if running in windows """
    return os.name == 'nt'
