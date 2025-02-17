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

import sys

from pycompss.util.exceptions import PyCOMPSsException

if sys.version_info <= (3, 0):
    from cStringIO import StringIO
else:
    from io import StringIO

ERROR_UNEXPECTED_WARNING = "ERROR: Unexpected warning message received."
ERROR_UNEXPECTED_ERROR = "ERROR: Unexpected error message received."
ERROR_MISSING_WARNING = "ERROR: Could not find warning message."
ERROR_MISSING_ERROR = "ERROR: Could not find error message."
ERROR_EXCEPTION = "ERROR: Exception has not been raised"


def test_check_arguments_fine():
    from pycompss.util.arguments import check_arguments

    mandatory_arguments = {"mandatory_argument_1", "mandatoryArgument2"}
    deprecated_arguments = {"deprecated_argument_1", "deprecated_argument_2"}
    supported_arguments = {
        "mandatory_argument_1",
        "mandatoryArgument2",
        "optional_argument",
    }
    argument_names = [
        "mandatory_argument_1",
        "mandatoryArgument2",
        "optional_argument",
    ]
    decorator = "Unittest"

    old_stderr = sys.stderr
    sys.stderr = my_stderr = StringIO()
    check_arguments(
        mandatory_arguments,
        deprecated_arguments,
        supported_arguments,
        argument_names,
        decorator,
    )
    sys.stderr = old_stderr
    assert "WARNING" not in my_stderr.getvalue(), ERROR_UNEXPECTED_WARNING
    assert "ERROR" not in my_stderr.getvalue(), ERROR_UNEXPECTED_ERROR


def test_check_arguments_using_deprecated():
    from pycompss.util.arguments import check_arguments

    mandatory_arguments = {"mandatory_argument_1", "mandatoryArgument2"}
    deprecated_arguments = {"deprecated_argument_1", "deprecated_argument_2"}
    supported_arguments = {
        "mandatory_argument_1",
        "mandatoryArgument2",
        "optional_argument",
    }
    argument_names = [
        "mandatory_argument_1",
        "mandatoryArgument2",
        "deprecated_argument_1",
    ]
    decorator = "Unittest"

    old_stderr = sys.stderr
    sys.stderr = my_stderr = StringIO()
    check_arguments(
        mandatory_arguments,
        deprecated_arguments,
        supported_arguments,
        argument_names,
        decorator,
    )
    sys.stderr = old_stderr
    assert "WARNING" in my_stderr.getvalue(), ERROR_MISSING_WARNING
    assert "ERROR" not in my_stderr.getvalue(), ERROR_UNEXPECTED_ERROR


def test_check_arguments_missing_mandatory():
    from pycompss.util.arguments import check_arguments

    result = False
    mandatory_arguments = {"mandatory_argument_1", "mandatoryArgument2"}
    deprecated_arguments = {"deprecated_argument_1", "deprecated_argument_2"}
    supported_arguments = {
        "mandatory_argument_1",
        "mandatoryArgument2",
        "optional_argument",
    }
    argument_names = ["mandatoryArgument2"]
    decorator = "Unittest"

    try:
        check_arguments(
            mandatory_arguments,
            deprecated_arguments,
            supported_arguments,
            argument_names,
            decorator,
        )
    except PyCOMPSsException:
        # This is ok
        result = True
    assert result, ERROR_EXCEPTION


def test_check_arguments_missing_mandatory_no_underscore():
    from pycompss.util.arguments import check_arguments

    result = False
    mandatory_arguments = {"mandatory_argument_1", "mandatoryArgument2"}
    deprecated_arguments = {"deprecated_argument_1", "deprecated_argument_2"}
    supported_arguments = {
        "mandatory_argument_1",
        "mandatoryArgument2",
        "optional_argument",
    }
    argument_names = ["mandatory_argument_1"]
    decorator = "Unittest"

    try:
        check_arguments(
            mandatory_arguments,
            deprecated_arguments,
            supported_arguments,
            argument_names,
            decorator,
        )
    except PyCOMPSsException:
        # This is ok
        result = True
    assert result, ERROR_EXCEPTION


def test_check_arguments_unexpected():
    from pycompss.util.arguments import check_arguments

    mandatory_arguments = {"mandatory_argument_1", "mandatoryArgument2"}
    deprecated_arguments = {"deprecated_argument_1", "deprecated_argument_2"}
    supported_arguments = {
        "mandatory_argument_1",
        "mandatoryArgument2",
        "optional_argument",
    }
    argument_names = [
        "mandatory_argument_1",
        "mandatoryArgument2",
        "unexpected_argument",
    ]
    decorator = "Unittest"

    old_stderr = sys.stderr
    sys.stderr = my_stderr = StringIO()
    check_arguments(
        mandatory_arguments,
        deprecated_arguments,
        supported_arguments,
        argument_names,
        decorator,
    )
    sys.stderr = old_stderr
    assert "WARNING" in my_stderr.getvalue(), ERROR_MISSING_WARNING
    assert "ERROR" not in my_stderr.getvalue(), ERROR_UNEXPECTED_ERROR


def test_check_arguments_using_old_is_modifier():
    from pycompss.util.arguments import check_arguments

    result = False
    mandatory_arguments = {"mandatory_argument_1", "mandatoryArgument2"}
    deprecated_arguments = {"deprecated_argument_1", "deprecated_argument_2"}
    supported_arguments = {
        "mandatory_argument_1",
        "mandatoryArgument2",
        "optional_argument",
    }
    argument_names = [
        "mandatory_argument_1",
        "mandatoryArgument2",
        "isModifier",
    ]
    decorator = "Unittest"

    old_stderr = sys.stderr
    sys.stderr = my_stderr = StringIO()
    try:
        check_arguments(
            mandatory_arguments,
            deprecated_arguments,
            supported_arguments,
            argument_names,
            decorator,
        )
    except PyCOMPSsException:
        sys.stderr = old_stderr
        assert "WARNING" not in my_stderr.getvalue(), ERROR_UNEXPECTED_WARNING
        assert "ERROR" in my_stderr.getvalue(), ERROR_MISSING_ERROR
        result = True
    assert result, ERROR_EXCEPTION
