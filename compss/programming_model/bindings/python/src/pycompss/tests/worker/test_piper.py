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
import time
import multiprocessing

from pycompss.api.task import task


@task()
def simple():
    pass


@task(returns=1)
def increment(value):
    return value + 1


def worker_thread(argv, current_path):
    import coverage
    coverage.process_startup()
    from pycompss.worker.piper.piper_worker import main
    # Start the piper worker
    sys.argv = argv
    sys.path.append(current_path)
    main()


def test_piper_worker():
    # Override sys.argv to mimic runtime call
    sys_argv_backup = list(sys.argv)
    sys_path_backup = list(sys.path)

    sys.argv = ['piper_worker.py',
                '/tmp/',
                'false', 'true', 0, 'null', 'NONE', 'localhost', '49049', '1',
                '/tmp/pipe_-504901196_executor0.outbound',
                '/tmp/pipe_-504901196_executor0.inbound',
                '/tmp/pipe_-504901196_control_worker.outbound',  # noqa: E501
                '/tmp/pipe_-504901196_control_worker.inbound']  # noqa: E501
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
    worker = multiprocessing.Process(target=worker_thread, args=(sys.argv,
                                                                 current_path))
    print("Starting")
    worker.start()
    # Wait 4 seconds to start the worker.
    print("Sleeping")
    time.sleep(4)
    # Run a simple task
    simple_task_message = ['EXECUTE_TASK', '1',
                           '/tmp/job1_NEW.out',
                           '/tmp/job1_NEW.err',
                           '0', '1', 'true', 'null', 'METHOD', 'test_piper',
                           'simple', '0', '1', 'localhost', '1', 'false',
                           'null', '0', '0']
    # Run an increment task

    # Send quit message
    os.write(executor_out, "QUIT\n")
    os.write(worker_out, "QUIT\n")

    # Cleanup
    # os.remove("/tmp/job1_NEW.out")
    # os.remove("/tmp/job1_NEW.err")
    # Close pipes
    os.close(executor_out)
    os.close(executor_in)
    os.close(worker_out)
    os.close(worker_in)
    # Remove pipes
    for pipe in pipes:
        os.unlink(pipe)
    # Remove logs
    os.remove("log/binding_worker.err")
    os.remove("log/binding_worker.out")
    # Restore sys.argv and sys.path
    sys.argv = sys_argv_backup
    sys.path = sys_path_backup
