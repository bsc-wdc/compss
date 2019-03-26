"""
This script is aimed at submitting a PyCOMPSs job to the queuing system
with Jupyter notebook.
"""

import os
import sys
from commons import VERBOSE
from commons import SUCCESS_KEYWORD
from commons import ERROR_KEYWORD
from commons import JOB_NAME_KEYWORD
from commons import DISABLED_VALUE
from commons import command_runner
from commons import get_installation_path


def submit():
    """
    Main submit function.
    :return: None
    """
    if VERBOSE:
        print("Submitting PyCOMPSs interactive job...")

    # Get command line arguments
    job_name = sys.argv[1]        # Job name
    notebook_path = sys.argv[2]   # Path where the notebook is
    exec_time = sys.argv[3]       # Walltime in minutes
    num_nodes = sys.argv[4]       # Number of nodes
    qos = sys.argv[5]             # Quality of service
    log_level = sys.argv[6]       # Log level
    tracing = sys.argv[7]         # Tracing
    classpath = sys.argv[8]       # Classpath
    pythonpath = sys.argv[9]      # Pythonpath
    storage_home = sys.argv[10]    # Storage home path
    storage_props = sys.argv[11]  # Storage properties file
    storage = sys.argv[12]        # Storage shortcut to use
    if VERBOSE:
        print("Submission arguments:")
        print(" - Job name     : " + str(job_name))
        print(" - Notebook path: " + str(notebook_path))
        print(" - Exec time    : " + str(exec_time))
        print(" - Num nodes    : " + str(num_nodes))
        print(" - QoS          : " + str(qos))
        print(" - Log level    : " + str(log_level))
        print(" - Tracing      : " + str(tracing))
        print(" - Classpath    : " + str(classpath))
        print(" - Pythonpath   : " + str(pythonpath))
        print(" - Storage home : " + str(storage_home))
        print(" - Storage props: " + str(storage_props))
        print(" - Storage      : " + str(storage))

    # Check storage shortcut
    if storage == 'None':
        # No storage shortcut defined - continue as normal
        storage_home = DISABLED_VALUE
        storage_props = DISABLED_VALUE
    elif storage == 'redis':
        # The user wants to use redis - expand/prepend some variables
        storage_home = os.path.join(get_installation_path(),
                                    'Tools', 'storage', 'redis')
        classpath = os.path.join(storage_home, 'compsss-redisPSCO.jar') + ':' + classpath
        pythonpath = os.path.join(storage_home, 'python') + ':' + pythonpath
        default_storage_props = os.path.join(os.path.expanduser('~'), 'storage_props.cfg')
        if not os.path.exists(default_storage_props):
            with open(default_storage_props, 'w'):
                pass
        storage_props = default_storage_props
    else:
        # Not supported storage shortcut
        raise Exception("Unsupported storage parameter: " + storage)

    # Extend classpath and pythonpath
    classpath = classpath + ':' + os.environ['CLASSPATH']
    if notebook_path != DISABLED_VALUE:
        # Include notebook_path into pythonpath for the workers
        pythonpath = notebook_path + ':' + pythonpath + ':' + os.environ['PYTHONPATH']
    else:
        # Include home
        pythonpath = os.path.expanduser('~') + ':' + pythonpath + ':' + os.environ['PYTHONPATH']

    # Append keyword to the job name
    job_name = job_name + JOB_NAME_KEYWORD

    # Submit the PyCOMPSs job with the notebook enabled
    if VERBOSE:
        print("Submitting the job...")
    if storage_home == DISABLED_VALUE:
        # Storage disabled
        cmd = ['enqueue_compss',
               '--job_name=' + job_name,
               '--exec_time=' + exec_time,
               '--num_nodes=' + num_nodes,
               '--log_level=' + log_level,
               '--qos=' + qos,
               '--tracing=' + tracing,
               '--classpath=' + classpath,
               '--pythonpath=' + pythonpath,
               '--lang=python',
               '--jupyter_notebook']
    else:
        # Storage enabled
        cmd = ['enqueue_compss',
               '--job_name=' + job_name,
               '--exec_time=' + exec_time,
               '--num_nodes=' + num_nodes,
               '--log_level=' + log_level,
               '--qos=' + qos,
               '--tracing=' + tracing,
               '--classpath=' + classpath,
               '--pythonpath=' + pythonpath,
               '--storage_home=' + storage_home,
               '--storage_props=' + storage_props,
               '--lang=python',
               '--jupyter_notebook']
    return_code, stdout, stderr = command_runner(cmd)

    if VERBOSE:
        print("Return code:" + str(return_code))
        print("Standard output:")
        print(stdout)
        print("Standard error:")
        print(stderr)
        print("Finished submitting job.")

    # Print to notify the submission result
    if return_code != 0:
        print(ERROR_KEYWORD)
    else:
        print(SUCCESS_KEYWORD)


if __name__ == '__main__':
    submit()
