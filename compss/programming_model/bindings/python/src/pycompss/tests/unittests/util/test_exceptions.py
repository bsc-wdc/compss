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

from pycompss.util.exceptions import DDSException
from pycompss.util.exceptions import MissingImplementedException
from pycompss.util.exceptions import NotImplementedException
from pycompss.util.exceptions import NotInPyCOMPSsException
from pycompss.util.exceptions import PyCOMPSsException

GENERIC_MESSAGE = "Message to show"
GENERIC_MESSAGE_ERROR = "ERROR: Received unexpected exception message."


def test_pycompss_exception():
    try:
        raise PyCOMPSsException(GENERIC_MESSAGE)
    except Exception as e:  # NOSONAR
        is_ok = True
        assert (
            str(e) == f"PyCOMPSs Exception: {GENERIC_MESSAGE}"
        ), GENERIC_MESSAGE_ERROR
    else:
        is_ok = False
    assert is_ok, "ERROR: The PyCOMPSsException has not been correctly raised"


def test_not_in_pycompss_exception():
    try:
        raise NotInPyCOMPSsException(GENERIC_MESSAGE)
    except Exception as e:  # NOSONAR
        is_ok = True
        assert (
            str(e) == "Outside PyCOMPSs scope: " + GENERIC_MESSAGE
        ), GENERIC_MESSAGE_ERROR
    else:
        is_ok = False
    assert (
        is_ok
    ), "ERROR: The NotInPyCOMPSsException has not been correctly raised"


def test_not_implemented_exception():
    try:
        raise NotImplementedException(GENERIC_MESSAGE)
    except Exception as e:  # NOSONAR
        is_ok = True
        assert (
            str(e)
            == "Functionality "
            + GENERIC_MESSAGE
            + " not implemented yet."  # noqa: E501
        ), GENERIC_MESSAGE_ERROR
    else:
        is_ok = False
    assert (
        is_ok
    ), "ERROR: The NotImplementedException has not been correctly raised"


def test_missing_implemented_exception():
    try:
        raise MissingImplementedException(GENERIC_MESSAGE)
    except Exception as e:  # NOSONAR
        is_ok = True
        assert (
            str(e)
            == "Missing "
            + GENERIC_MESSAGE
            + ". Needs to be overridden."  # noqa: E501
        ), GENERIC_MESSAGE_ERROR
    else:
        is_ok = False
    assert (
        is_ok
    ), "ERROR: The MissingImplementedException has not been correctly raised"


def test_dds_exception():
    try:
        raise DDSException(GENERIC_MESSAGE)
    except Exception as e:  # NOSONAR
        is_ok = True
        assert (
            str(e) == f"DDS Exception: {GENERIC_MESSAGE}"
        ), GENERIC_MESSAGE_ERROR
    else:
        is_ok = False
    assert is_ok, "ERROR: The DDSException has not been correctly raised"
