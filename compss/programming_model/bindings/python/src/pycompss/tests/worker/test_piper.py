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
from pycompss.util.serialization.serializer import deserialize_from_file

from pycompss.api.task import task


@task()
def simple():
    pass


@task(returns=1)
def increment(value):
    return value + 1


def worker_thread(argv, current_path):
    from pycompss.worker.piper.piper_worker import main
    # Start the piper worker
    sys.argv = argv
    sys.path.append(current_path)
    sys.stdout = open(current_path + "/../../../../std.out", 'w')
    sys.stderr = open(current_path + "/../../../../std.err", 'w')
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
    print("Starting piper worker")
    worker.start()
    # Wait 2 seconds to start the worker.
    print("Waiting 2 seconds to send a task request")
    time.sleep(2)
    # Run a simple task
    job1_out = '/tmp/job1_NEW.out'
    job1_err = '/tmp/job1_NEW.err'
    simple_task_message = ['EXECUTE_TASK', '1',
                           job1_out,
                           job1_err,
                           '0', '1', 'true', 'null', 'METHOD', 'test_piper',
                           'simple', '0', '1', 'localhost', '1', 'false',
                           'None', '0', '0',
                           '-', '0', '0']
    simple_task_message_str = " ".join(simple_task_message)
    print("Requesting: " + simple_task_message_str)
    os.write(executor_out, simple_task_message_str + '\n')  # noqa
    time.sleep(2)
    # Run a increment task
    job2_out = '/tmp/job2_NEW.out'
    job2_err = '/tmp/job2_NEW.err'
    job2_result = '/tmp/job2.IT'
    increment_task_message = ['EXECUTE_TASK', '2',
                              job2_out,
                              job2_err,
                              '0', '1', 'true', 'null', 'METHOD', 'test_piper',
                              'increment', '0', '1', 'localhost', '1', 'false',
                              '9', '1', '2', '4', '3', 'null', 'value', 'null',
                              '1', '9', '3', '#', '$return_0', 'null',
                              job2_result + ':d1v2_1599560599402.IT:false:true:' + job2_result,
                              '-', '0', '0']
    increment_task_message_str = " ".join(increment_task_message)
    print("Requesting: " + increment_task_message_str)
    os.write(executor_out, increment_task_message_str + '\n')  # noqa
    time.sleep(2)
    # Send quit message
    os.write(executor_out, b"QUIT\n")
    os.write(worker_out, b"QUIT\n")
    # Wait for the worker to finish
    worker.join()
    # Cleanup
    # Close pipes
    os.close(executor_out)
    os.close(executor_in)
    os.close(worker_out)
    os.close(worker_in)
    # Remove pipes
    for pipe in pipes:
        os.unlink(pipe)
        if os.path.isfile(pipe):
            os.remove(pipe)
    # Check logs
    out_log = "log/binding_worker.out"
    err_log = "log/binding_worker.err"
    if os.path.exists(err_log):
        raise Exception("An error happened. Please check " + err_log)
    with open(out_log, 'r') as f:
        if 'ERROR' in f.read():
            raise Exception("An error happened. Please check " + out_log)
        if 'Traceback' in f.read():
            raise Exception("An error happened. Please check " + out_log)
    # Check task 1
    check_task(job1_out, job1_err)
    # Check task 2
    check_task(job2_out, job2_err)
    result = deserialize_from_file(job2_result)
    if result != 2:
        raise Exception("Wrong result obtained for increment task. Expected 2, received: " + str(result))  # noqa

    # Remove logs
    os.remove(job1_out)
    os.remove(job1_err)
    os.remove(job2_out)
    os.remove(job2_err)
    os.remove(job2_result)
    if os.path.isfile(err_log):
        os.remove(err_log)
    if os.path.isfile(out_log):
        os.remove(out_log)
    if os.path.isfile(current_path + "/../../../../std.out"):
        os.remove(current_path + "/../../../../std.out")
    if os.path.isfile(current_path + "/../../../../std.err"):
        os.remove(current_path + "/../../../../std.err")
    # Restore sys.argv and sys.path
    sys.argv = sys_argv_backup
    sys.path = sys_path_backup


def check_task(job_out, job_err):
    if os.path.exists(job_err) and os.path.getsize(job_err) > 0:  # noqa
        # Non empty file exists
        raise Exception("An error happened in the task. Please check " + job_err)  # noqa
    with open(job_out, 'r') as f:
        content = f.read()
        if 'ERROR' in content:
            raise Exception("An error happened in the task. Please check " + job_out)  # noqa
        if 'EXCEPTION' in content or 'Exception' in content:
            raise Exception("An exception happened in the task. Please check " + job_out)  # noqa
        if 'End task execution. Status: Ok' not in content:
            raise Exception("The task was supposed to be OK. Please check " + job_out)  # noqa
