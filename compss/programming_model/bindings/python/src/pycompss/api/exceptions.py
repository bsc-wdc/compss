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
PyCOMPSs API - Exception.

This file defines the public PyCOMPSs exception.
"""


class COMPSsException(Exception):
    """COMPSs generic exception.

    Raised by the user code.
    """

    __slots__ = ["message", "target_direction"]

    def __init__(self, message: str, target_direction: int = -1) -> None:
        """Store the arguments passed to the constructor.

        :param message: Exception message.
        :param target_direction: Target direction.
        """
        super().__init__(message)  # show traceback
        self.message = message
        self.target_direction = target_direction
