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

# CAUTION: Unittest for real api functions are in integration tests.


def test_api_calling_dummy():
    from pycompss.util.context import CONTEXT

    CONTEXT.set_master()
    # Load exposed api, not dummy
    from pycompss.api.api import compss_start
    from pycompss.api.api import compss_stop
    from pycompss.api.api import compss_file_exists
    from pycompss.api.api import compss_open
    from pycompss.api.api import compss_delete_file
    from pycompss.api.api import compss_wait_on_file
    from pycompss.api.api import compss_wait_on_directory
    from pycompss.api.api import compss_delete_object
    from pycompss.api.api import compss_barrier
    from pycompss.api.api import compss_barrier_group
    from pycompss.api.api import compss_wait_on
    from pycompss.api.api import compss_get_number_of_resources
    from pycompss.api.api import compss_request_resources
    from pycompss.api.api import compss_free_resources
    from pycompss.api.api import TaskGroup

    # Force to use dummies
    CONTEXT.set_out_of_scope()

    file_name = "simulated_file.txt"
    file_names = ["simulated_file1.txt", "simulated_file2.txt"]
    directory_name = "simulated_directory"
    directory_names = ["simulated_directory1", "simulated_directory2"]
    group_name = "simulated_group"
    obj = [1, 2, 3]
    num_resources = 1

    with open(file_name, "w") as f:
        f.write("some content")
    os.mkdir(directory_name)

    for f_name in file_names:
        with open(f_name, "w") as f:
            f.write("some content")
    for d_name in directory_names:
        os.mkdir(d_name)

    compss_start(log_level="off", interactive=False)
    compss_stop(code=0)
    compss_file_exists(file_name)
    compss_file_exists(*file_names)
    compss_open(file_name, mode="r")
    compss_delete_file(file_name)
    compss_delete_file(*file_names)
    compss_wait_on_file(file_name)
    compss_wait_on_file(*file_names)
    compss_wait_on_directory(directory_name)
    compss_wait_on_directory(*directory_names)
    compss_delete_object(obj)
    compss_delete_object(*obj)
    compss_barrier(no_more_tasks=False)
    compss_barrier_group(group_name)
    compss_wait_on(obj)
    compss_wait_on(*obj)
    compss_get_number_of_resources()
    compss_request_resources(num_resources, group_name)
    compss_free_resources(num_resources, group_name)

    with TaskGroup(group_name, implicit_barrier=True):
        # Empty task group check
        pass

    os.remove(file_name)
    os.rmdir(directory_name)

    for f_name in file_names:
        os.remove(f_name)
    for d_name in directory_names:
        os.rmdir(d_name)


def test_dummy_api():
    from pycompss.api.dummy.api import compss_start
    from pycompss.api.dummy.api import compss_stop
    from pycompss.api.dummy.api import compss_file_exists
    from pycompss.api.dummy.api import compss_open
    from pycompss.api.dummy.api import compss_delete_file
    from pycompss.api.dummy.api import compss_wait_on_file
    from pycompss.api.dummy.api import compss_wait_on_directory
    from pycompss.api.dummy.api import compss_delete_object
    from pycompss.api.dummy.api import compss_barrier
    from pycompss.api.dummy.api import compss_barrier_group
    from pycompss.api.dummy.api import compss_wait_on
    from pycompss.api.dummy.api import compss_get_number_of_resources
    from pycompss.api.dummy.api import compss_request_resources
    from pycompss.api.dummy.api import compss_free_resources
    from pycompss.api.dummy.api import TaskGroup

    file_name = "simulated_file.txt"
    file_names = ["simulated_file1.txt", "simulated_file2.txt"]
    directory_name = "simulated_directory"
    directory_names = ["simulated_directory1", "simulated_directory2"]
    group_name = "simulated_group"
    obj = [1, 2, 3]
    num_resources = 1

    with open(file_name, "w") as f:
        f.write("some content")
    os.mkdir(directory_name)

    for f_name in file_names:
        with open(f_name, "w") as f:
            f.write("some content")
    for d_name in directory_names:
        os.mkdir(d_name)

    compss_start(log_level="off", interactive=False)
    compss_stop(code=0)
    compss_file_exists(file_name)
    compss_file_exists(*file_names)
    compss_open(file_name, mode="r")
    compss_delete_file(file_name)
    compss_delete_file(*file_names)
    compss_wait_on_file(file_name)
    compss_wait_on_file(*file_names)
    compss_wait_on_directory(directory_name)
    compss_wait_on_directory(*directory_names)
    compss_delete_object(obj)
    compss_delete_object(*obj)
    compss_barrier(no_more_tasks=False)
    compss_barrier_group(group_name)
    compss_wait_on(obj)
    compss_wait_on(*obj)
    compss_get_number_of_resources()
    compss_request_resources(num_resources, group_name)
    compss_free_resources(num_resources, group_name)

    with TaskGroup(group_name, implicit_barrier=True):
        # Empty task group check
        pass

    os.remove(file_name)
    os.rmdir(directory_name)

    for f_name in file_names:
        os.remove(f_name)
    for d_name in directory_names:
        os.rmdir(d_name)
