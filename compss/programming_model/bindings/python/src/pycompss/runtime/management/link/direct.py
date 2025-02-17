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
PyCOMPSs Binding - Management - Link Direct.

This file contains the functions to link with the binding-commons directly.
"""

import logging
from pycompss.util.typing_helper import typing

if __debug__:
    link_logger = logging.getLogger(__name__)


def establish_link(
    logger: typing.Optional[logging.Logger] = None,
) -> typing.Any:
    """Load the compss C extension within the same process.

    Does not implement support for stdout and stderr redirecting as the
    establish_interactive_link.

    :param logger: Use this logger instead of the module logger.
    :return: The COMPSs C extension link.
    """
    if __debug__:
        message = "Loading compss extension"
        if logger:
            logger.debug(message)
        else:
            link_logger.debug(message)
    import compss  # pylint: disable=import-outside-toplevel

    if __debug__:
        message = "Loaded compss extension"
        if logger:
            logger.debug(message)
        else:
            link_logger.debug(message)
    return compss
