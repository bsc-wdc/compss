#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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

"""
PyCOMPSs Exceptions
====================
    This file defines the internal PyCOMPSs exceptions.
"""

from pycompss.util.typing_helper import typing


class SerializerException(Exception):
    """
    Exception on serialization
    """

    pass


class PyCOMPSsException(Exception):
    """
    Generic PyCOMPSs exception
    """

    def __init__(self, message):
        # type: (str) -> None
        super(PyCOMPSsException, self).__init__(message)


class NotInPyCOMPSsException(Exception):
    """
    Not within PyCOMPSs scope exception.
    """

    def __init__(self, message):
        # type: (str) -> None
        msg = "Outside PyCOMPSs scope: %s" % message
        super(NotInPyCOMPSsException, self).__init__(msg)


class NotImplementedException(Exception):
    """
    Not implemented exception.
    """

    def __init__(self, functionality):
        # type: (str) -> None
        msg = "Functionality %s not implemented yet." % functionality
        super(NotImplementedException, self).__init__(msg)


class MissingImplementedException(Exception):
    """
    Not implemented exception
    """

    def __init__(self, functionality):
        # type: (str) -> None
        msg = "Missing %s. Needs to be overridden." % functionality
        super(MissingImplementedException, self).__init__(msg)


class TimeOutError(Exception):
    """
    Time out error exception
    """

    pass


class CancelError(Exception):
    """
    Cancel error exception
    """

    pass


def task_timed_out(signum, frame):  # noqa
    # type: (int, typing.Any) -> None
    """Task time out signal handler

    Do not remove the parameters.

    :param signum: Signal number.
    :param frame: Frame.
    :return: None
    :raises: TimeOutError exception.
    """
    raise TimeOutError


def task_cancel(signum, frame):  # noqa
    # type: (int, typing.Any) -> None
    """Task cancel signal handler.

    Do not remove the parameters.

    :param signum: Signal number.
    :param frame: Frame.
    :return: None
    :raises: CancelError exception.
    """
    raise CancelError


class DDSException(Exception):
    """
    Generic DDS exception
    """

    def __init__(self, message):
        # type: (str) -> None
        super(DDSException, self).__init__(message)
