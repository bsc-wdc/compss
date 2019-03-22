"""
This script is aimed at getting the list of running PyCOMPSs jobs
with Jupyter notebook.
"""

import os
from commons import VERBOSE
from commons import SUCCESS_KEYWORD
from commons import command_runner
from commons import setup_supercomputer_configuration
from commons import is_notebook_job


def find():
    """
    Main find function.
    :return: None
    """
    if VERBOSE:
        print("Looking for PyCOMPSs interactive jobs...")

    # Load the Supercomputer configuration to get the appropriate status command
    setup_supercomputer_configuration()

    # Get the list of running job ids
    job_list_command = os.environ['QUEUE_JOB_LIST']
    _, raw_job_ids, _ = command_runner(job_list_command.split())
    job_ids = raw_job_ids.splitlines()

    # Filter the jobs (keep only the notebook related ones)
    notebook_ids = []
    for job_id in job_ids:
        if is_notebook_job(job_id):
            notebook_ids.append(job_id)

    if VERBOSE:
        print("Finished looking for jobs.")

    # Print to provide the list of jobs to the client
    print(SUCCESS_KEYWORD)  # Success message
    for notebook_id in notebook_ids:
        print(notebook_id)


if __name__ == '__main__':
    find()


