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
This script is aimed at checking the information of a running PyCOMPSs jobs
with Jupyter notebook.
"""

import os
import sys
import time
from commons import VERBOSE
from commons import SUCCESS_KEYWORD
from commons import ERROR_KEYWORD
from commons import NOT_RUNNING_KEYWORD
from commons import setup_supercomputer_configuration
from commons import get_jupyter_environment_variables
from commons import update_command
from commons import command_runner
from commons import command_runner_shell
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
    app_path = sys.argv[2]

    if VERBOSE:
        print(" - Job: " + str(job_id))

    # Load the Supercomputer configuration to get the appropriate status command
    setup_supercomputer_configuration()
    
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

        jpy_server_out = ''
        timeout = time.time() + 60*2
        jup_out_file = f'{app_path}/compss-{job_id}.err'
        while 'The Jupyter Notebook is running at' not in jpy_server_out:
            if time.time() > timeout:
                print(ERROR_KEYWORD)
                exit(1)
            if os.path.isfile(jup_out_file):
                with open(jup_out_file, 'r') as of:
                    jpy_server_out = of.read()

        if VERBOSE:
            print("Finished checking the information.")

        # Print to notify the info result
        print(SUCCESS_KEYWORD)
        print("MASTER:" + str(master))
        print("SERVER: " + str(jpy_server_out.replace('\n', ' ')))
    else:
        # Print to provide information to the client
        print(NOT_RUNNING_KEYWORD)  # The notebook is not ready yet
        exit(1)


if __name__ == '__main__':
    info()
