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
PyCOMPSs Binding - Interactive Initialization.

This file contains the extra interactive initialization functionalities.
"""


class ExtraLaunchStatus:
    """This class contains the global status variables used in interactive."""

    __slots__ = ["log_path", "graphing", "line_separator", "disable_external"]

    def __init__(self) -> None:
        """Create a new state object."""
        self.log_path = "undefined"
        self.graphing = False
        self.line_separator = 56 * "*"
        self.disable_external = False

    def get_graphing(self) -> bool:
        """Get GRAPHING state value.

        :return: Graphing status value.
        """
        return self.graphing

    def get_line_separator(self) -> str:
        """Get LINE_SEPARATOR.

        :return: LINE SEPARATOR string.
        """
        return self.line_separator

    def get_disable_external(self) -> bool:
        """Get DISABLE_EXTERNAL value.

        :return: Disable external value.
        """
        return self.disable_external

    def set_graphing(self, graphing: bool) -> None:
        """Set GRAPHING value.

        :param: graphing: New graphing value.
        :return: None
        """
        self.graphing = graphing

    def set_line_separator(self, line_separator: str) -> None:
        """Set LINE SEPARATOR value.

        :param: line_separator: New line separator value.
        :return: None
        """
        self.line_separator = line_separator

    def set_disable_external(self, disable_external: bool) -> None:
        """Set DISABLE_EXTERNAL value.

        :param: disable_external: New disable_external value.
        :return: None
        """
        self.disable_external = disable_external


EXTRA_LAUNCH_STATUS = ExtraLaunchStatus()
