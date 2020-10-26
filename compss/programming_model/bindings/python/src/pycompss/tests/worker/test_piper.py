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

# -*- coding: utf-8 -*-

import os
import sys
import multiprocessing

from pycompss.tests.worker.common_piper_tester import create_files
from pycompss.tests.worker.common_piper_tester import STD_OUT_FILE
from pycompss.tests.worker.common_piper_tester import STD_ERR_FILE
from pycompss.tests.worker.common_piper_tester import evaluate_worker


def worker_thread(argv, current_path):
    from pycompss.worker.piper.piper_worker import main

    # Start the piper worker
    sys.argv = argv
    sys.path.append(current_path)
    sys.stdout = open(current_path + STD_OUT_FILE, "w")
    sys.stderr = open(current_path + STD_ERR_FILE, "w")
    main()


def test_piper_worker():
    # Override sys.argv to mimic runtime call
    sys_argv_backup = list(sys.argv)
    sys_path_backup = list(sys.path)

    files = create_files()
    temp_folder, executor_outbound, executor_inbound, control_worker_outbound, control_worker_inbound = files  # noqa: E501

    sys.argv = [
        "piper_worker.py",
        temp_folder,
        "false",
        "true",
        0,
        "null",
        "NONE",
        "localhost",
        "49049",
        "1",
        executor_outbound,
        executor_inbound,
        control_worker_outbound,
        control_worker_inbound,
    ]  # noqa: E501
    pipes = sys.argv[-4:]
    # Create pipes
    for pipe in pipes:
        if os.path.exists(pipe):
            os.remove(pipe)
        os.mkfifo(pipe)
    # Open pipes
    executor_out = os.open(pipes[0], os.O_RDWR)
    executor_in = os.open(pipes[1], os.O_RDWR)
    worker_out = os.open(pipes[2], os.O_RDWR)
    worker_in = os.open(pipes[3], os.O_RDWR)

    current_path = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(current_path)
    # Start the piper worker in a separate thread
    worker = multiprocessing.Process(
        target=worker_thread, args=(sys.argv, current_path)
    )

    evaluate_worker(worker, "test_piper", pipes, files, current_path,
                    executor_out, executor_in,
                    worker_out, worker_in)

    # Restore sys.argv and sys.path
    sys.argv = sys_argv_backup
    sys.path = sys_path_backup
