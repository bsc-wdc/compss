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

from pycompss.worker.gat.worker import main
from pycompss.api.task import task


@task()
def simple():
    pass


@task(returns=1)
def increment(value):
    return value + 1


def test_gat_worker_simple_task():
    # Override sys.argv to mimic runtime call
    sys_argv_backup = list(sys.argv)
    sys_path_backup = list(sys.path)
    sys.argv = ["worker.py", "false", 1, "true",
                "null", "NONE", "localhost", "49049", "METHOD",
                "test_gat", "simple",
                "0", "0", "1", "false", "null", "0", "0"]
    current_path = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(current_path)
    main()
    sys.argv = sys_argv_backup
    sys.path = sys_path_backup


def test_gat_worker_increment_task():
    # Override sys.argv to mimic runtime call
    sys_argv_backup = list(sys.argv)
    sys_path_backup = list(sys.path)
    sys.argv = ["worker.py", "false", 1, "true",
                "null", "NONE", "localhost", "49049", "METHOD",
                "test_gat", "increment",
                "0", "0", "1", "false", "null", "1", "2", "4", "3",
                "null", "value", "null", "1", "9", "3", "#", "$return_0",
                "null", "/tmp/d1v1_1234.IT"
                ]
    current_path = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(current_path)
    main()
    sys.argv = sys_argv_backup
    sys.path = sys_path_backup
