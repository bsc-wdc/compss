"""
This script is aimed at cancelling a running PyCOMPSs job with Jupyter notebook.
"""

import os
import sys
from commons import VERBOSE
from commons import SUCCESS_KEYWORD
from commons import setup_supercomputer_configuration
from commons import command_runner
from commons import is_notebook_job
from commons import not_a_notebook


def cancel():
    """
    Main cancel function.
    :return: None
    """
    if VERBOSE:
        print("Cancelling PyCOMPSs interactive job...")

    # Get command line arguments
    job_ids = sys.argv[1:]

    # There might be more than one to cancel
    for job_id in job_ids:
        if VERBOSE:
            print(" - Job: " + str(job_id))

        # Since we need to export the job_id in the environment before loading the
        # supercomputer configuration (so that it can take it to build properly the
        # commands), we build the include dictionary:
        include = dict()
        include['job_id'] = str(job_id)

        # Load the Supercomputer configuration to get the appropriate cancel command
        setup_supercomputer_configuration(include)

        # Check if the job_id belongs to a notebook before continuing
        if not is_notebook_job(job_id):
            not_a_notebook(job_id)

        # Get the command to cancel the job
        command = os.environ['QUEUE_JOB_CANCEL_CMD'].split()

        # Cancel the job
        _, _, _ = command_runner(command)

        if VERBOSE:
            print("Finished cancellation.")

    # Print to notify that the submission has finished without errors
    print(SUCCESS_KEYWORD)  # Success message


if __name__ == '__main__':
    cancel()
