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
PyCOMPSs Binding - Worker shared arguments.

This file contains the global variables used in the Python binding.
"""

from pycompss.util.typing_helper import typing


class SharedArguments:
    """Class that contains shared arguments between the worker and master.

    It is used to know things that are known in the worker side from the
    master side (e.g. synchronization).

    IMPORTANT: Required for nesting purposes.
    """

    __slots__ = ("worker_args",)

    def __init__(self) -> None:
        """Instantiate Shared Arguments class."""
        # Worker arguments received on the task call
        self.worker_args = tuple()  # type: tuple

    def set_worker_args(self, worker_args: tuple) -> None:
        """Worker arguments to save.

        :param worker_args: Worker arguments.
        :return: None.
        """
        self.worker_args = worker_args

    def get_worker_args(self) -> tuple:
        """Retrieve the worker arguments.

        :return: Worker arguments.
        """
        return self.worker_args

    def update_worker_argument_parameter_content(
        self, name: typing.Optional[str], content: typing.Any
    ) -> None:
        """Update the Parameter's content for the given name.

        :param name: Parameter name.
        :param content: New content.
        :return: None.
        """
        if name:
            for param in self.worker_args:
                if (
                    not param.is_collection()
                    and not param.is_dict_collection()
                    and param.name == name
                ):
                    param.content = content
                    param.is_future = False
                    return

    def delete_worker_args(self) -> None:
        """Remove the worker args variable contents.

        :return: None.
        """
        self.worker_args = tuple()


# ############################################################# #
# ##################### SHARED ARGUMENTS ###################### #
# ############################################################# #

SHARED_ARGUMENTS = SharedArguments()
