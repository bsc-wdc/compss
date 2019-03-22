"""
This script is aimed at checking the status of the running PyCOMPSs jobs
with Jupyter notebook.
"""

import os
import sys
from commons import VERBOSE
from commons import SUCCESS_KEYWORD
from commons import command_runner
from commons import setup_supercomputer_configuration
from commons import is_notebook_job
from commons import not_a_notebook


def status():
    """
    Main status function.
    :return: None
    """
    if VERBOSE:
        print("Checking status PyCOMPSs interactive job...")

    # Get command line arguments
    job_id = sys.argv[1]

    if VERBOSE:
        print(" - Job: " + str(job_id))

    # Since we need to export the job_id in the environment before loading the
    # supercomputer configuration (so that it can take it to build properly the
    # commands), we build the include dictionary:
    include = dict()
    include['job_id'] = str(job_id)

    # Load the Supercomputer configuration to get the appropriate status command
    setup_supercomputer_configuration(include)

    # Check if the job_id belongs to a notebook before continuing
    if not is_notebook_job(job_id):
        not_a_notebook(job_id)

    # Get the command to check the status the job
    command = os.environ['QUEUE_JOB_STATUS_CMD'].split()

    # Check the status of the job
    return_code, stdout, stderr = command_runner(command, exception=False)

    # Get the Running tag and check if matches
    running_tag = os.environ['QUEUE_JOB_RUNNING_TAG']

    # Print to provide status to the client
    if return_code != 0:
        job_status = "FAILED"
    elif stdout == running_tag:
        job_status = "RUNNING"
    else:
        job_status = str(stdout).strip()

    if VERBOSE:
        print("Finished checking status.")

    # Print to notify that the submission has finished without errors
    print(SUCCESS_KEYWORD)  # Success message
    print("STATUS:" + job_status)


if __name__ == '__main__':
    status()


