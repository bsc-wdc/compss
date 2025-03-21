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
import subprocess
import sys


def test_runcompss_increment():
    current_path = os.path.dirname(os.path.abspath(__file__))

    # Call to runcompss for increment application
    app = os.path.join(
        current_path, "..", "launch", "resources", "increment.py"
    )
    if sys.version_info < (3, 0):
        raise Exception("Unsupported python version. Required Python 3.X")
    else:
        cmd = [
            "runcompss",
            "--log_level=debug",
            "--python_interpreter=python3",
            app,
        ]
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    process.wait()
    outs, errs = process.communicate()


def test_runcompss_increment_with_gat():
    current_path = os.path.dirname(os.path.abspath(__file__))

    # Call to runcompss for increment application
    app = os.path.join(
        current_path, "..", "launch", "resources", "increment.py"
    )
    if sys.version_info < (3, 0):
        raise Exception("Unsupported python version. Required Python 3.X")
    else:
        cmd = [
            "runcompss",
            "--log_level=debug",
            "--python_interpreter=python3",
            "--comm=es.bsc.compss.gat.master.GATAdaptor",
            app,
        ]
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    process.wait()
    outs, errs = process.communicate()
