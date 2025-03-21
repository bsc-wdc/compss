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
from pycompss.worker.gat.worker import main


@task()
def simple():
    # Do nothing task
    pass


@task(returns=1)
def increment(value):
    return value + 1


def test_gat_worker_simple_task():
    # Override sys.argv to mimic runtime call
    sys_argv_backup = list(sys.argv)
    sys_path_backup = list(sys.path)
    sys.argv = [
        "worker.py",
        "false",
        "1",
        "true",
        "null",
        "NONE",
        "localhost",
        "49049",
        "METHOD",
        "test_gat",
        "simple",
        "1",
        "0",
        "0",
        "1",
        "false",
        "null",
        "0",
        "0",
    ]
    current_path = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(current_path)
    main()
    sys.argv = sys_argv_backup
    sys.path = sys_path_backup


def test_gat_worker_increment_task():
    # Override sys.argv to mimic runtime call
    sys_argv_backup = list(sys.argv)
    sys_path_backup = list(sys.path)
    temp_file = tempfile.NamedTemporaryFile(delete=False).name
    sys.argv = [
        "worker.py",
        "false",
        "1",
        "true",
        "null",
        "NONE",
        "localhost",
        "49049",
        "METHOD",
        "test_gat",
        "increment",
        "1",
        "0",
        "0",
        "1",
        "false",
        "null",
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
        temp_file,
    ]
    current_path = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(current_path)
    main()
    sys.argv = sys_argv_backup
    sys.path = sys_path_backup
    os.remove(temp_file)
