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

"""
PyCOMPSs Util - Exceptions.

This file defines the internal PyCOMPSs exceptions.
"""

from pycompss.util.typing_helper import typing


class SerializerException(Exception):
    """Exception on serialization class."""


class PyCOMPSsException(Exception):
    """Generic PyCOMPSs exception class."""

    def __init__(self, message: str) -> None:
        """Create a new PyCOMPSsException instance.

        :param message: Exception message.
        """
        msg = f"PyCOMPSs Exception: {message}"
        super().__init__(msg)


class NotInPyCOMPSsException(Exception):
    """Not within PyCOMPSs scope exception class."""

    def __init__(self, message: str) -> None:
        """Create a new NotInPyCOMPSsException instance.

        :param message: Exception message.
        """
        msg = f"Outside PyCOMPSs scope: {message}"
        super().__init__(msg)


class NotImplementedException(Exception):
    """Not implemented exception class."""

    def __init__(self, functionality: str) -> None:
        """Create a new NotImplementedException instance.

        :param functionality: Exception message.
        """
        msg = f"Functionality {functionality} not implemented yet."
        super().__init__(msg)


class MissingImplementedException(Exception):
    """Missing implemented exception class."""

    def __init__(self, functionality: str) -> None:
        """Create a new MissingImplementedException instance.

        :param functionality: Exception message.
        """
        msg = f"Missing {functionality}. Needs to be overridden."
        super().__init__(msg)


class DDSException(Exception):
    """Generic DDS exception class."""

    def __init__(self, message: str) -> None:
        """Create a new DDSException instance.

        :param message: Exception message.
        """
        msg = f"DDS Exception: {message}"
        super().__init__(msg)


class TimeOutError(Exception):
    """Time out error exception class."""


class CancelError(Exception):
    """Cancel error exception class."""


class StandardOutputError(Exception):
    """Stdout error exception class."""


class StandardErrorError(Exception):
    """Stderr error exception class."""


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
