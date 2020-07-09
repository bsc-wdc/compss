#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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


class NotImplementedException(Exception):
    """
    Not implemented exception
    """

    def __init__(self, functionality):
        super(self.__class__, self).__init__("Functionality " + functionality +
                                             " not implemented yet.")


class MissingImplementedException(Exception):
    """
    Not implemented exception
    """

    def __init__(self, functionality):
        super(self.__class__, self).__init__("Missing " + functionality +
                                             ". Needs to be overridden.")


class TimeOutError(BaseException):
    """
    Time out error exception
    """
    pass


class CancelError(BaseException):
    """
    Cancel error exception
    """
    pass


def task_timed_out(signum, frame):  # noqa
    """ Task time out signal handler.

    :param signum: Signal number.
    :param frame: Frame.
    :raises: TimeOutError exception.
    """
    raise TimeOutError


def task_cancel(signum, frame):  # noqa
    """ Task cancel signal handler.

    :param signum: Signal number.
    :param frame: Frame.
    :raises: CancelError exception.
    """
    raise CancelError
