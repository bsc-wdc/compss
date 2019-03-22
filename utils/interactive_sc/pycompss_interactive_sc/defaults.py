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

DEFAULT_USER_NAME = str(getpass.getuser())

# Matches the same key as dest in arguments parsers
DEFAULT_PROJECT = {'exec_time': '5',  # 5 minute
                   'job_name': 'Jupyter',
                   'num_nodes': 2,
                   'qos': 'debug',
                   'log_level': LOG_LEVEL_OFF,
                   'tracing': False,
                   'classpath': '.',
                   'pythonpath': '.',
                   'storage_home': DISABLED_VALUE,
                   'storage_props': DISABLED_VALUE,
                   'storage': 'None'}

DEFAULT_CREDENTIALS = {'user_name': DEFAULT_USER_NAME,
                       'password': False,
                       'supercomputer': 'mn2.bsc.es',
                       'port_forward': '3333',
                       'web_browser': 'firefox'}

# --------------------------------------------------
# SCRIPT TOOLS
# --------------------------------------------------
DEFAULT_SSH = 'ssh'  # When password provided, use sshpass -e before ssh
DEFAULT_SSHPASS = 'sshpass -e ' + DEFAULT_SSH

# --------------------------------------------------
# WARNING CONSTANTS DECLARATION
# --------------------------------------------------
WARNING_USER_NAME_NOT_PROVIDED = "Username not provided. Using: " + DEFAULT_USER_NAME
WARNING_NOTEBOOK_NOT_RUNNING = "The notebook is not running!"

# --------------------------------------------------
# ERROR CONSTANTS DECLARATION
# --------------------------------------------------
ERROR_UNEXPECTED_PARAMETER = "Unexpected parameter in project file: "
ERROR_UNRECOGNIZED_ACTION = "Unrecognized action: "
ERROR_CONNECTING = "Could not connect to the supercomputer.\n" \
                   "Please check the connectivity, user name and password."
ERROR_COMPSS_NOT_DEFINED = "COMPSs is not available in the supercomputer.\n" \
                           "Please check that you have propperly configured the module load in your .bashrc."
ERROR_SUBMITTING_JOB = "Could not submit the job to the supercomputer.\n" \
                       "Please check that you are able to submit jobs to the supercomputer."
ERROR_STATUS_JOB = "Could not check the status of the job."
ERROR_INFO_JOB = "Could not get the information of the job."

ERROR_STORAGE_PROPS = "--storage_props flag not defined."
ERROR_UNSUPPORTED_STORAGE_SHORTCUT = "Non supported external storage shortcut."
ERROR_BROWSER = "Could not open the browser. Please check that you have defined properly the browser to use."
ERROR_CANCELLING_JOB = "Could not cancel the job. Please, cancel it manually."
