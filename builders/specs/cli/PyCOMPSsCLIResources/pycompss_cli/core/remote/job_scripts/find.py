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
This script is aimed at getting the list of running PyCOMPSs jobs
with Jupyter notebook.
"""

import os
from commons import VERBOSE
from commons import SUCCESS_KEYWORD
from commons import ERROR_KEYWORD
from commons import command_runner
from commons import setup_supercomputer_configuration
from commons import get_job_name
from commons import get_job_status
from commons import verify_job_name


def find():
    """
    Main find function.
    Gets the jobs list command from the environment variable (defined in supercomputer cfgs)
    and runs it. It also gets each job identifier name and status to provide more information
    to the users.
    :return: None
    """
    if VERBOSE:
        print("Looking for PyCOMPSs interactive jobs...")

    # Load the Supercomputer configuration to get the appropriate status command
    setup_supercomputer_configuration()

    # Get the list of running job ids
    job_list_command = os.environ['QUEUE_JOB_LIST_CMD']
    return_code, raw_job_ids, _ = command_runner(job_list_command.split())
    job_ids = raw_job_ids.splitlines()

    # Filter the jobs (keep only the notebook related ones)
    jobs = []
    for job_id in job_ids:
        name = get_job_name(job_id).strip()
        status, _ = get_job_status(job_id)
        jobs.append((job_id, status, name))

    if VERBOSE:
        print("Finished looking for jobs.")

    # Print to notify the find result
    if return_code != 0:
        print(ERROR_KEYWORD)
        exit(1)
    else:
        # Print to provide the list of jobs to the client
        print(SUCCESS_KEYWORD)
        for job_id, status, name in jobs:
            print(str(job_id) + '\t- ' + str(status) + '\t- ' + str(name))


if __name__ == '__main__':
    find()
