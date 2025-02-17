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

from pycompss.runtime.management.COMPSs import COMPSs
from pycompss.util.exceptions import PyCOMPSsException


def test_is_redirected():
    # Get a copy of the initial status
    old_stdout = COMPSs.stdout
    old_stderr = COMPSs.stderr
    # First case: Both not initialized -> False
    COMPSs.stdout = ""
    COMPSs.stderr = ""
    none_none = COMPSs.is_redirected()
    # Second case: Both initialized -> True
    COMPSs.stdout = "file.out"
    COMPSs.stderr = "file.err"
    something_something = COMPSs.is_redirected()
    # Third case: One not initialized -> Raise exception
    COMPSs.stderr = ""
    is_ok = False
    try:
        _ = COMPSs.is_redirected()
    except PyCOMPSsException:
        is_ok = True
    assert (
        none_none is False
    ), "ERROR: Failed first case of is_redirected. Must return False."
    assert (
        something_something
    ), "ERROR: Failed second case of is_redirected. Must return True."
    assert (
        is_ok
    ), "ERROR: Failed third case of is_redirected. Must raise an Exception."
    # Restore status
    COMPSs.stdout = old_stdout
    COMPSs.stderr = old_stderr


def test_get_redirection():
    # Get a copy of the initial status
    old_stdout = COMPSs.stdout
    old_stderr = COMPSs.stdout
    # First case: Both not initialized -> Raise exception
    COMPSs.stdout = ""
    COMPSs.stderr = ""
    is_ok = False
    try:
        _, _ = COMPSs.get_redirection_file_names()
    except PyCOMPSsException:
        is_ok = True
    # Second case: Both initialized -> out, err
    out_name = "file.out"
    err_name = "file.err"
    COMPSs.stdout = out_name
    COMPSs.stderr = err_name
    new_stdout, new_stderr = COMPSs.get_redirection_file_names()
    assert (
        is_ok
    ), "ERROR: Failed first case of get_redirection_file_names. Must raise an Exception."  # noqa: E501
    assert (
        new_stdout == out_name
    ), "ERROR: Failed second case of get_redirection_file_names. Must return stdout file name."  # noqa: E501
    assert (
        new_stderr == err_name
    ), "ERROR: Failed second case of get_redirection_file_names. Must return stdout file name."  # noqa: E501
    # Restore status
    COMPSs.stdout = old_stdout
    COMPSs.stderr = old_stderr
