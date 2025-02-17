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

"""This file contains the logger level functions."""

from pycompss.util.exceptions import PyCOMPSsException


class _LogLevel:  # pylint: disable=too-few-public-methods
    """Supported logging levels."""

    INFO = "info"
    TRACE = "debug"
    DEBUG = "debug"
    API = "off"
    OFF = "off"


LOG_LEVEL = _LogLevel()


def get_log_level(level: str) -> str:
    """Translate the given level to the real log level tag.

    :param level: Logging level.
    :return: Real log level tag.
    :raise PyCOMPSsException: Unsupported logging level
    """
    level = level.lower()
    if level == "info":
        return LOG_LEVEL.INFO
    if level == "trace":
        return LOG_LEVEL.TRACE
    if level == "debug":
        return LOG_LEVEL.DEBUG
    if level == "api":
        return LOG_LEVEL.API
    if level == "off":
        return LOG_LEVEL.OFF
    raise PyCOMPSsException("Unsupported logging level (get).")


def check_log_level(level: str) -> bool:
    """Check that the log level is supported.

    :param level: Log level.
    return: If the log level is supported
    """
    if level in [
        LOG_LEVEL.API,
        LOG_LEVEL.DEBUG,
        LOG_LEVEL.INFO,
        LOG_LEVEL.OFF,
        LOG_LEVEL.TRACE,
    ]:
        return True
    raise PyCOMPSsException("Unsupported logging level (check).")
