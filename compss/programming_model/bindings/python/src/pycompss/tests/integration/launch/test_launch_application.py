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

APPLICATION_NAME = "increment"
APPLICATION = "increment.py"
FOLDER = "resources"
PACKAGE = "main"


def test_launch_increment():
    from pycompss.runtime.launch import launch_pycompss_application

    current_path = os.path.dirname(os.path.abspath(__file__))
    app = os.path.join(current_path, FOLDER, APPLICATION)
    launch_pycompss_application(
        app, PACKAGE, debug=True, trace=False, app_name=APPLICATION_NAME
    )


def test_launch_application():
    from pycompss.runtime.launch import launch_pycompss_application

    current_path = os.path.dirname(os.path.abspath(__file__))
    app = os.path.join(current_path, FOLDER, "api_tester.py")
    launch_pycompss_application(
        app, PACKAGE, debug=True, trace=False, app_name=APPLICATION_NAME
    )


# def test_launch_increment_with_cache():
#     if sys.version_info >= (3, 8):
#         from pycompss.runtime.launch import launch_pycompss_application
#         current_path = os.path.dirname(os.path.abspath(__file__))
#         app = os.path.join(current_path, "..", FOLDER, APPLICATION)
#         launch_pycompss_application(
#             app, PACKAGE, debug=True, trace=False, app_name=APPLICATION_NAME,
#             worker_cache=True
#         )
#     else:
#         print("WARNING: Cache not tested because python version is not "
#               ">= 3.8")
#
#
# def test_launch_increment_mpi_worker():
#     from pycompss.runtime.launch import launch_pycompss_application
#
#     current_path = os.path.dirname(os.path.abspath(__file__))
#     app = os.path.join(current_path, "..", FOLDER, APPLICATION)
#     launch_pycompss_application(
#         app, PACKAGE, debug=True, trace=False, app_name=APPLICATION_NAME,
#         mpi_worker=True
#     )
