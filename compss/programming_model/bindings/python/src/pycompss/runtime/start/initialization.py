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
PyCOMPSs Binding - Initialization.

This file contains the initialization functionalities.
"""

import logging


class LaunchStatus:
    """This class contains the global status variables used all around."""

    __slots__ = ["app_path", "streaming", "persistent_storage", "logger"]

    def __init__(self) -> None:
        """Create a new state object."""
        self.app_path = "undefined"
        self.streaming = False
        self.persistent_storage = False
        self.logger = logging.getLogger(__name__)  # type: logging.Logger

    def get_app_path(self) -> str:
        """Get APP_PATH value.

        :return: App path value.
        """
        return self.app_path

    def get_streaming(self) -> bool:
        """Get STREAMING state value.

        :return: Streaming status value.
        """
        return self.streaming

    def get_persistent_storage(self) -> bool:
        """Get PERSISTENT_STORAGE state value.

        :return: Persistent storage status value.
        """
        return self.persistent_storage

    def get_logger(self) -> logging.Logger:
        """Get LOGGER value.

        :return: Logger value.
        """
        return self.logger

    def set_app_path(self, app_path: str) -> None:
        """Set APP_PATH value.

        :param: app_path: New app path value.
        :return: None
        """
        self.app_path = app_path

    def set_streaming(self, streaming: bool) -> None:
        """Set STREAMING value.

        :param: streaming: New streaming value.
        :return: None
        """
        self.streaming = streaming

    def set_persistent_storage(self, persistent_storage: bool) -> None:
        """Set PERSISTENT STORAGE value.

        :param: persistent_storage: New persistent storage value.
        :return: None
        """
        self.persistent_storage = persistent_storage

    def set_logger(self, logger: logging.Logger) -> None:
        """Set LOGGER value.

        :param: logger: New logger value.
        :return: None
        """
        self.logger = logger


LAUNCH_STATUS = LaunchStatus()
