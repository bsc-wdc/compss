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
This script is aimed at checking the status of the running PyCOMPSs jobs
with Jupyter notebook.
"""

import sys
from commons import VERBOSE
from commons import SUCCESS_KEYWORD
from commons import ERROR_KEYWORD
from commons import get_job_status
from commons import setup_supercomputer_configuration


def status():
    """
    Main status function.
    Gets the status of the requested job identifier and prints it
    through stdout.
    :return: None
    """
    if VERBOSE:
        print("Checking status PyCOMPSs interactive job...")

    # Get command line arguments
    job_id = sys.argv[1]

    if VERBOSE:
        print(" - Job: " + str(job_id))

    # Load the Supercomputer configuration to get the appropriate status command
    setup_supercomputer_configuration()

    # Get the job status
    job_status, return_code = get_job_status(job_id)

    if VERBOSE:
        print("Finished checking status.")

    # Print to notify the status result
    if return_code != 0:
        print(ERROR_KEYWORD)
        exit(1)
    else:
        print(SUCCESS_KEYWORD)
        print("STATUS:" + job_status)


if __name__ == '__main__':
    status()


