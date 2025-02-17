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

from pycompss.api.exceptions import COMPSsException
from pycompss.api.task import task
from pycompss.tests.outlog import create_logger
from pycompss.util.serialization.serializer import serialize_to_file
from pycompss.worker.container.container_worker import main

CONTAINER_WORKER = "container_worker.py"
LOGGER = create_logger()


@task()
def simple():
    # Do nothing task
    pass


@task()
def simple_compss_exception():
    raise COMPSsException("On purpose exception")


@task()
def simple_exception():
    nom = 10
    denom = 0
    _ = nom / denom  # throws exception


@task(returns=1)
def increment(value):
    return value + 1


def test_container_worker_simple_task():
    # Override sys.argv to mimic runtime call
    sys_argv_backup = list(sys.argv)
    sys_path_backup = list(sys.path)
    sys.argv = [
        CONTAINER_WORKER,
        "test_container_worker",
        "simple",
        "debug",
        "false",
        "null",
        "0",
        "0",
        "0",
        "0",
    ]
    current_path = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(current_path)
    main()
    sys.argv = sys_argv_backup
    sys.path = sys_path_backup


def test_container_worker_increment_task():
    # Override sys.argv to mimic runtime call
    sys_argv_backup = list(sys.argv)
    sys_path_backup = list(sys.path)
    temp_file = tempfile.NamedTemporaryFile(delete=False).name
    temp_file_name = os.path.basename(temp_file)
    serialize_to_file(1, temp_file, logger=LOGGER)
    sys.argv = [
        CONTAINER_WORKER,
        "test_container_worker",
        "increment",
        "true",
        "false",
        "false",
        "10",
        "1",
        "2",
        "10",
        "3",
        "#",
        "$return_0",
        "FILE",
        "null:" + temp_file_name + ":false:true:" + temp_file,
        "4",
        "3",
        "null",
        "value",
        "#UNDEFINED#:#UNDEFINED#",
        "1",
    ]
    current_path = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(current_path)
    main()
    sys.argv = sys_argv_backup
    sys.path = sys_path_backup
    os.remove(temp_file)


def test_container_worker_simple_task_compss_exception():
    # Override sys.argv to mimic runtime call
    sys_argv_backup = list(sys.argv)
    sys_path_backup = list(sys.path)
    sys.argv = [
        CONTAINER_WORKER,
        "test_container_worker",
        "simple_compss_exception",
        "debug",
        "false",
        "null",
        "0",
        "0",
        "0",
        "0",
    ]
    current_path = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(current_path)
    exit_code = main()
    sys.argv = sys_argv_backup
    sys.path = sys_path_backup
    assert (
        exit_code == 2
    ), "Wrong exit code received (expected 2, received %s)" % str(exit_code)


def test_container_worker_simple_task_exception():
    # Override sys.argv to mimic runtime call
    sys_argv_backup = list(sys.argv)
    sys_path_backup = list(sys.path)
    sys.argv = [
        CONTAINER_WORKER,
        "test_container_worker",
        "simple_exception",
        "debug",
        "false",
        "null",
        "0",
        "0",
        "0",
        "0",
    ]
    current_path = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(current_path)
    exit_code = main()
    sys.argv = sys_argv_backup
    sys.path = sys_path_backup
    assert (
        exit_code == 1
    ), "Wrong exit code received (expected 1, received %s)" % str(exit_code)
