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
PyCOMPSs runtime - Task - Features.

This file contains the task features.
"""


class TaskFeatures:  # pylint: disable=too-few-public-methods
    """Features that any task may have."""

    __slots__ = ["prepend_strings", "register_only", "object_conversion"]

    def __init__(self) -> None:
        """Instantiate task features class."""
        # Determine if strings should have a sharp symbol prepended or not
        self.prepend_strings = True
        # Only register the task
        self.register_only = False
        # Convert small objects to string if enabled
        self.object_conversion = False

    def get_prepend_strings(self) -> bool:
        """Get PREPEND_STRINGS value.

        :return: Prepend strings value.
        """
        return self.prepend_strings

    def get_register_only(self) -> bool:
        """Get REGISTER_ONLY value.

        :return: Register only value.
        """
        return self.register_only

    def get_object_conversion(self) -> bool:
        """Get OBJECT_CONVERSION value.

        :return: Object conversion value.
        """
        return self.object_conversion

    def set_prepend_strings(self, prepend_strings: bool) -> None:
        """Set PREPEND_STRINGS value.

        :param: prepend_strings: New prepend_strings value.
        :return: None
        """
        self.prepend_strings = prepend_strings

    def set_register_only(self, register_only: bool) -> None:
        """Set REGISTER_ONLY value.

        :param: register_only: New register only value.
        :return: None
        """
        self.register_only = register_only

    def set_object_conversion(self, object_conversion: bool) -> None:
        """Set OBJECT_CONVERSION value.

        :param: object_conversion: New object conversion value.
        :return: None
        """
        self.object_conversion = object_conversion


TASK_FEATURES = TaskFeatures()
