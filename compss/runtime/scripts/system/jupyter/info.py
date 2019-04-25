#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
This script is aimed at checking the information of a running PyCOMPSs jobs
with Jupyter notebook.
"""

import os
import sys
from commons import VERBOSE
from commons import SUCCESS_KEYWORD
from commons import ERROR_KEYWORD
from commons import NOT_RUNNING_KEYWORD
from commons import setup_supercomputer_configuration
from commons import update_command
from commons import command_runner
from commons import is_notebook_job
from commons import not_a_notebook


def info():
    """
    Main info function.
    Gets the information command from the jupyter server (master node and url)
    and prints it through stdout. So that the client can get it, parse and
    connect to the running notebook.
    :return: None
    """
    if VERBOSE:
        print("Checking information of PyCOMPSs interactive job...")

    # Get command line arguments
    job_id = sys.argv[1]

    if VERBOSE:
        print(" - Job: " + str(job_id))

    # Load the Supercomputer configuration to get the appropriate status command
    setup_supercomputer_configuration()

    # Check if the job_id belongs to a notebook before continuing
    if not is_notebook_job(job_id):
        not_a_notebook(job_id)

    # Get the list of nodes
    raw_nodes_command = os.environ['QUEUE_JOB_NODES_CMD']
    nodes_command = update_command(raw_nodes_command, job_id)
    return_code, nodes, _ = command_runner(nodes_command)
    if return_code != 0:
        print(ERROR_KEYWORD)
        exit(1)

    # Look for the master node
    if nodes.strip():
        nodes_expansor = os.environ['HOSTLIST_CMD'] + ' ' + nodes
        _, expanded_nodes, _ = command_runner(nodes_expansor.split())
        expanded_nodes = expanded_nodes.splitlines()
        master = expanded_nodes[0]

        if VERBOSE:
            print(" - Nodes: " + str(expanded_nodes))
            print(" - Found master: " + str(master))

        # Get the command to contact with the node where the job is running
        server_list = os.environ['CONTACT_CMD'] + ' ' + master + " jupyter-notebook list"
        return_code, jupyter_server_list, _ = command_runner(server_list.split())

        if VERBOSE:
            print("Finished checking the information.")

        # Print to notify the info result
        if return_code != 0:
            print(ERROR_KEYWORD)
            exit(1)
        else:
            print(SUCCESS_KEYWORD)
            print("MASTER:" + str(master))
            print("SERVER: " + str(jupyter_server_list.replace('\n', ' ')))
    else:
        # Print to provide information to the client
        print(NOT_RUNNING_KEYWORD)  # The notebook is not ready yet
        exit(1)


if __name__ == '__main__':
    info()


