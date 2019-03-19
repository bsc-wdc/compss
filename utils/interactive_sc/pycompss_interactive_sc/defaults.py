
# --------------------------------------------------
# PROGRAM
# --------------------------------------------------
VERSION = '0.1.0'
DECODING_FORMAT = 'utf-8'
SUBMIT_SCRIPT = 'submit_jupyter_job.sh'
CANCEL_SCRIPT = 'cancel_jupyter_job.sh'

# --------------------------------------------------
# ARGUMENTS
# --------------------------------------------------
DEFAULT_EXEC_TIME = '1'        # 1 minute
DEFAULT_JOB_NAME = 'Jupyter'
DEFAULT_NUM_NODES = 2
DEFAULT_USER_NAME = 'undefined'
DEFAULT_GET_PASSWORD = False   # default is using passwordless
DEFAULT_SUPERCOMPUTER = 'mn2.bsc.es'
DEFAULT_WEB_BROWSER = 'firefox'
DEFAULT_QOS = 'debug'
DEFAULT_TRACING = False
DEFAULT_PORT_FORWARDING = '3333'
DEFAULT_CLASSPATH = '.'
DEFAULT_PYTHONPATH = '.'
DEFAULT_STORAGE_HOME = 'undefined'
DISABLED_STORAGE_HOME = 'undefined'
DEFAULT_STORAGE_PROPS = 'undefined'
DEFAULT_STORAGE = 'None'
DEFAULT_LOG_LEVEL = 'off'
# DEFAULT_LOG_LEVEL_ARGUMENT = 'debug'
LOG_LEVEL_DEBUG = 'debug'
LOG_LEVEL_INFO = 'info'
LOG_LEVEL_OFF = 'off'
DEFAULT_VERBOSE = False

# --------------------------------------------------
# SCRIPT TOOLS
# --------------------------------------------------
DEFAULT_SSH = "ssh"  # When password provided, use sshpass -e before ssh

# --------------------------------------------------
# WARNING CONSTANTS DECLARATION
# --------------------------------------------------
WARNING_USER_NAME_NOT_PROVIDED = "Username not provided. Using default: " + DEFAULT_USER_NAME

# --------------------------------------------------
# ERROR CONSTANTS DECLARATION
# --------------------------------------------------
ERROR_CONNECTING = "Could not connect to the supercomputer. Please check the connectivity, user name and password."
ERROR_COMPSS_NOT_DEFINED = "COMPSs is not available in the supercomputer. Please check that you have propperly configured the module load in your .bashrc."
ERROR_SUBMITTING_JOB = "Could not submit the job to the supercomputer. Please check that you are able to submit jobs to the supercomputer."
ERROR_STORAGE_PROPS = "--storage_props flag not defined."
ERROR_UNSUPPORTED_STORAGE_SHORTCUT = "Non supported external storage shortcut."
ERROR_PORT_FORWARDING = "Could not establish the port forwarding. Please check the ports and contact with the supercomputer administrators."
ERROR_BROWSER = "Could not open the browser. Please check that you have defined propperly the browser to use."
ERROR_CANCELLING_JOB = "Could not cancel the job. Please, cancel it manually."
