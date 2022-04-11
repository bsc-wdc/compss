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
PyCOMPSs Util - Exceptions.

This file defines the internal PyCOMPSs exceptions.
"""

from pycompss.util.typing_helper import typing


class SerializerException(Exception):
    """Exception on serialization class."""

    pass


class PyCOMPSsException(Exception):
    """Generic PyCOMPSs exception class."""

    def __init__(self, message: str) -> None:
        """Create a new PyCOMPSsException instance.

        :param message: Exception message.
        """
        super(PyCOMPSsException, self).__init__(message)


class NotInPyCOMPSsException(Exception):
    """Not within PyCOMPSs scope exception class."""

    def __init__(self, message: str) -> None:
        """Create a new NotInPyCOMPSsException instance.

        :param message: Exception message.
        """
        msg = "Outside PyCOMPSs scope: %s" % message
        super(NotInPyCOMPSsException, self).__init__(msg)


class NotImplementedException(Exception):
    """Not implemented exception class."""

    def __init__(self, functionality: str) -> None:
        """Create a new NotImplementedException instance.

        :param message: Exception message.
        """
        msg = "Functionality %s not implemented yet." % functionality
        super(NotImplementedException, self).__init__(msg)


class MissingImplementedException(Exception):
    """Missing implemented exception class."""

    def __init__(self, functionality: str) -> None:
        """Create a new MissingImplementedException instance.

        :param message: Exception message.
        """
        msg = "Missing %s. Needs to be overridden." % functionality
        super(MissingImplementedException, self).__init__(msg)


class TimeOutError(Exception):
    """Time out error exception class."""

    pass


class CancelError(Exception):
    """Cancel error exception class."""

    pass


class DDSException(Exception):
    """Generic DDS exception class."""

    def __init__(self, message: str) -> None:
        """Create a new DDSException instance.

        :param message: Exception message.
        """
        super(DDSException, self).__init__(message)


def task_timed_out(signum: int, frame: typing.Any) -> None:
    """Task time out signal handler.

    Do not remove the parameters.

    :param signum: Signal number.
    :param frame: Frame.
    :return: None
    :raises: TimeOutError exception.
    """
    raise TimeOutError


def task_cancel(signum: int, frame: typing.Any) -> None:
    """Task cancel signal handler.

    Do not remove the parameters.

    :param signum: Signal number.
    :param frame: Frame.
    :return: None
    :raises: CancelError exception.
    """
    raise CancelError
