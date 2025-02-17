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

from pycompss.api.commons.error_msgs import cast_env_to_int_error
from pycompss.api.commons.error_msgs import cast_string_to_int_error
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.api.commons.error_msgs import wrong_value

DECORATOR_NAME = "@unittest"


def test_err_msgs_not_in_pycompss():
    decorator_name = DECORATOR_NAME
    expected = (
        "The %s decorator only works within PyCOMPSs framework."
        % decorator_name
    )
    error = not_in_pycompss(decorator_name=decorator_name)
    assert error == expected, "Received wrong NOT IN PYCOMPSS error message."


def test_err_msgs_cast_env_to_int_error():
    what = DECORATOR_NAME
    expected = "ERROR: %s value cannot be cast from ENV variable to int" % what
    error = cast_env_to_int_error(what=what)
    assert error == expected, "Received wrong Cast env to int error message."


def test_err_msgs_cast_string_to_int_error():
    what = DECORATOR_NAME
    expected = "ERROR: %s value cannot be cast from string to int" % what
    error = cast_string_to_int_error(what=what)
    assert (
        error == expected
    ), "Received wrong Cast string to int error message."


def test_err_msgs_wrong_value():
    value_name = "unittest"
    decorator_name = DECORATOR_NAME
    expected = "ERROR: Wrong %s value at %s decorator." % (
        value_name,
        decorator_name,
    )
    error = wrong_value(value_name=value_name, decorator_name=decorator_name)
    assert error == expected, "Received wrong error message."
