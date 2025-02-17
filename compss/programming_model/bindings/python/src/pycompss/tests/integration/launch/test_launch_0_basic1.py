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


def test_launch_test_0_basic1():
    from pycompss.runtime.launch import launch_pycompss_application

    current_path = os.path.dirname(os.path.abspath(__file__))
    app_path = os.path.join(
        current_path,
        "..",
        "..",
        "..",
        "..",
        "..",
        "..",
        "..",
        "..",
        "..",
        "tests",
        "sources",
        "local",
        "python",
        "0_basic1",
        "src",
    )
    app = os.path.join(app_path, "test_mp.py")
    sys.path.insert(0, app_path)
    launch_pycompss_application(
        app, "main_program", debug=True, app_name="test_0_basic1"
    )
    sys.path.pop(0)
    if os.path.exists("infile"):
        os.remove("infile")
    if os.path.exists("outfile"):
        os.remove("outfile")
    if os.path.exists("inoutfile"):
        os.remove("inoutfile")
