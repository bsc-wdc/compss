#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
import tempfile
import shutil
import multiprocessing
import subprocess

from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.serialization.serializer import deserialize_from_file
from pycompss.api.task import task

if sys.version_info >= (3, 0):
    IS_PYTHON3 = True
else:
    IS_PYTHON3 = False


STD_OUT_FILE = "/../../../../std.out"
STD_ERR_FILE = "/../../../../std.err"
ERROR_MESSAGE = "An error happened. Please check: "


@task()
def simple():
    # Do nothing task
    pass


@task(returns=1)
def increment(value):
    return value + 1


def setup_argv(argv, current_path):
    sys.argv = argv
    sys.path.append(current_path)
    sys.stdout = open(current_path + STD_OUT_FILE, "w")
    sys.stderr = open(current_path + STD_ERR_FILE, "w")


def evaluate_piper_worker_common(worker_thread, mpi_worker=False):
    # Override sys.argv to mimic runtime call
    sys_argv_backup = list(sys.argv)
    sys_path_backup = list(sys.path)

    files = create_files()
    temp_folder, executor_outbound, executor_inbound, control_worker_outbound, control_worker_inbound = files  # noqa: E501

    current_path = os.path.dirname(os.path.abspath(__file__))
    python_path = (
            current_path + "/../../tests/worker/:" + os.environ["PYTHONPATH"]
    )

    if mpi_worker:
        sys.argv = [
            "mpirun",
            "-np",
            "2",
            "-x",
            "PYTHONPATH=" + python_path,
            "python",
            current_path + "/../../worker/piper/mpi_piper_worker.py",
            temp_folder,
            "false",
            "true",
            "0",
            "null",
            "NONE",
            "localhost",
            "49049",
            "false",
            "1",
            executor_outbound,
            executor_inbound,
            control_worker_outbound,
            control_worker_inbound,
        ]  # noqa: E501
    else:
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
            "false",
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

    sys.path.append(current_path)
    # Start the piper worker in a separate thread
    worker = multiprocessing.Process(
        target=worker_thread, args=(sys.argv, current_path)
    )

    if mpi_worker:
        evaluate_worker(worker, "test_mpi_piper", pipes, files, current_path,
                        executor_out, executor_in,
                        worker_out, worker_in)
    else:
        evaluate_worker(worker, "test_piper", pipes, files, current_path,
                        executor_out, executor_in,
                        worker_out, worker_in)

    # Restore sys.argv and sys.path
    sys.argv = sys_argv_backup
    sys.path = sys_path_backup


def create_files():
    temp_folder = tempfile.mkdtemp()
    executor_outbound = tempfile.NamedTemporaryFile(delete=False).name
    executor_inbound = tempfile.NamedTemporaryFile(delete=False).name
    control_worker_outbound = tempfile.NamedTemporaryFile(delete=False).name
    control_worker_inbound = tempfile.NamedTemporaryFile(delete=False).name
    return temp_folder, executor_outbound, executor_inbound, control_worker_outbound, control_worker_inbound  # noqa: E501


def evaluate_worker(worker, name, pipes, files, current_path,
                    executor_out, executor_in,
                    worker_out, worker_in):
    temp_folder, executor_outbound, executor_inbound, control_worker_outbound, control_worker_inbound = files  # noqa: E501
    print("Starting " + name + " worker")
    worker.start()
    print("Temp folder: " + temp_folder)
    # Wait 2 seconds to start the worker.
    print("Waiting 2 seconds to send a task request")
    time.sleep(2)
    # Run a simple task
    job1_out = tempfile.NamedTemporaryFile(delete=False).name
    job1_err = tempfile.NamedTemporaryFile(delete=False).name
    simple_task_message = [
        "EXECUTE_TASK",
        "1",
        job1_out,
        job1_err,
        "0",
        "1",
        "true",
        "null",
        "METHOD",
        "common_piper_tester",
        "simple",
        "0",
        "1",
        "localhost",
        "1",
        "false",
        "None",
        "0",
        "0",
        "-",
        "0",
        "0",
    ]
    simple_task_message_str = " ".join(simple_task_message)
    print("Requesting: " + simple_task_message_str)
    if IS_PYTHON3:
        os.write(executor_out, (simple_task_message_str + "\n").encode())  # noqa
    else:
        os.write(executor_out, simple_task_message_str + "\n")  # noqa
    time.sleep(2)
    # Run a increment task
    job2_out = tempfile.NamedTemporaryFile(delete=False).name
    job2_err = tempfile.NamedTemporaryFile(delete=False).name
    job2_result = tempfile.NamedTemporaryFile(delete=False).name
    increment_task_message = [
        "EXECUTE_TASK",
        "2",
        job2_out,
        job2_err,
        "0",
        "1",
        "true",
        "null",
        "METHOD",
        "common_piper_tester",
        "increment",
        "0",
        "1",
        "localhost",
        "1",
        "false",
        "9",
        "1",
        "2",
        "4",
        "3",
        "null",
        "value",
        "null",
        "1",
        "9",
        "3",
        "#",
        "$return_0",
        "null",
        job2_result + ":d1v2_1599560599402.IT:false:true:" + job2_result,
        "-",
        "0",
        "0",
    ]
    increment_task_message_str = " ".join(increment_task_message)
    print("Requesting: " + increment_task_message_str)
    if IS_PYTHON3:
        os.write(executor_out, (increment_task_message_str + "\n").encode())  # noqa
    else:
        os.write(executor_out, increment_task_message_str + "\n")  # noqa
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
    out_log = os.path.join(temp_folder, "binding_worker.out")
    err_log = os.path.join(temp_folder, "binding_worker.err")
    if os.path.exists(err_log):
        raise PyCOMPSsException(ERROR_MESSAGE + err_log)
    with open(out_log, "r") as f:
        if "ERROR" in f.read():
            raise PyCOMPSsException(ERROR_MESSAGE + out_log)
        if "Traceback" in f.read():
            raise PyCOMPSsException(ERROR_MESSAGE + out_log)
    # Check task 1
    check_task(job1_out, job1_err)
    # Check task 2
    check_task(job2_out, job2_err)
    result = deserialize_from_file(job2_result)
    if result != 2:
        raise PyCOMPSsException(
            "Wrong result obtained for increment task. Expected 2, received: " +  # noqa: E501
            str(result)
        )

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
    if os.path.isfile(current_path + STD_OUT_FILE):
        os.remove(current_path + STD_OUT_FILE)
    if os.path.isfile(current_path + STD_ERR_FILE):
        os.remove(current_path + STD_ERR_FILE)
    shutil.rmtree(temp_folder)
    if os.path.isfile(executor_outbound):
        os.remove(executor_outbound)
    if os.path.isfile(executor_inbound):
        os.remove(executor_inbound)
    if os.path.isfile(control_worker_outbound):
        os.remove(control_worker_outbound)
    if os.path.isfile(control_worker_inbound):
        os.remove(control_worker_inbound)


def check_task(job_out, job_err):
    if os.path.exists(job_err) and os.path.getsize(job_err) > 0:  # noqa
        # Non empty file exists
        raise PyCOMPSsException(
            "An error happened in the task. Please check " + job_err
        )  # noqa
    with open(job_out, "r") as f:
        content = f.read()
        if "ERROR" in content:
            raise PyCOMPSsException(
                "An error happened in the task. Please check " + job_out
            )  # noqa
        if "EXCEPTION" in content or "Exception" in content:
            raise PyCOMPSsException(
                "An exception happened in the task. Please check " + job_out
            )  # noqa
        if "END TASK execution. Status: Ok" not in content:
            raise PyCOMPSsException(
                "The task was supposed to be OK. Please check " + job_out
            )  # noqa
