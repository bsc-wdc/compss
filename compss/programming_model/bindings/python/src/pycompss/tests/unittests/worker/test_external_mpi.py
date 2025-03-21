#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
import tempfile

from pycompss.api.task import task
from pycompss.util.exceptions import PyCOMPSsException

using_mypy = False
try:
    from pycompss.worker.external.mpi_executor import main
except AttributeError:
    using_mypy = True


@task()
def simple():
    # Do nothing task
    pass


@task(returns=1)
def increment(value):
    return value + 1


def test_external_mpi_worker_simple_task():
    if using_mypy:
        raise Exception("UNSUPPORTED WITH MYPY")
    # Override sys.argv to mimic runtime call
    sys_argv_backup = list(sys.argv)
    sys_path_backup = list(sys.path)
    job1_out = tempfile.NamedTemporaryFile(delete=False).name
    job1_err = tempfile.NamedTemporaryFile(delete=False).name
    working_dir = os.getcwd()
    sys.argv = [
        "test_external_mpi.py",
        " ".join(
            [
                "EXECUTE_TASK",
                "1",
                working_dir,
                job1_out,
                job1_err,
                "0",
                "1",
                "true",
                "null",
                "METHOD",
                "test_external_mpi",
                "simple",
                "0",
                "1",
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
        ),
    ]
    current_path = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(current_path)
    main()
    sys.argv = sys_argv_backup
    sys.path = sys_path_backup
    check_task(job1_out, job1_err)
    os.remove(job1_out)
    os.remove(job1_err)


def test_external_mpi_worker_increment_task():
    if using_mypy:
        raise Exception("UNSUPPORTED WITH MYPY")
    # Override sys.argv to mimic runtime call
    sys_argv_backup = list(sys.argv)
    sys_path_backup = list(sys.path)
    job2_out = tempfile.NamedTemporaryFile(delete=False).name
    job2_err = tempfile.NamedTemporaryFile(delete=False).name
    job2_result = tempfile.NamedTemporaryFile(delete=False).name
    working_dir = os.getcwd()
    sys.argv = [
        "test_external_mpi.py",
        " ".join(
            [
                "EXECUTE_TASK",
                "2",
                working_dir,
                job2_out,
                job2_err,
                "0",
                "1",
                "true",
                "null",
                "METHOD",
                "test_external_mpi",
                "increment",
                "0",
                "1",
                "1",
                "localhost",
                "1",
                "false",
                "10",
                "1",
                "2",
                "4",
                "3",
                "null",
                "value",
                "null",
                "1",
                "10",
                "3",
                "#",
                "$return_0",
                "null",
                job2_result
                + ":d1v2_1599560599402.IT:false:true:"
                + job2_result,  # noqa: E501
                "-",
                "0",
                "0",
            ]
        ),
    ]
    current_path = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(current_path)
    main()
    sys.argv = sys_argv_backup
    sys.path = sys_path_backup
    check_task(job2_out, job2_err)
    os.remove(job2_out)
    os.remove(job2_err)
    os.remove(job2_result)


def check_task(job_out, job_err):
    if os.path.exists(job_err) and os.path.getsize(job_err) > 0:  # noqa
        # Non empty file exists
        raise PyCOMPSsException(
            "An error happened in the task. Please check " + job_err
        )
    with open(job_out, "r") as f:
        content = f.read()
        if "ERROR" in content:
            raise PyCOMPSsException(
                "An error happened in the task. Please check " + job_out
            )
        if "EXCEPTION" in content or "Exception" in content:
            raise PyCOMPSsException(
                "An exception happened in the task. Please check " + job_out
            )
        if "END TASK execution. Status: Ok" not in content:
            raise PyCOMPSsException(
                "The task was supposed to be OK. Please check " + job_out
            )
