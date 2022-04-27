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
PyCOMPSs Util - Context.

This file contains the methods to detect the origin of the call stack.
Useful to detect if we are in the master or in the worker.
"""

import inspect
from contextlib import contextmanager

# Typing imports
from pycompss.util.typing_helper import typing

#################
# CONTEXT CLASS #
#################


class Context:
    """Keep the context variables"""

    __slots__ = [
        "master",
        "worker",
        "out_of_scope",
        "who",
        "where",
        "nesting",
        "loading",
        "to_register",
    ]

    def __init__(self):
        """Instantiate the context class."""
        # Final variables
        self.master = "MASTER"
        self.worker = "WORKER"
        self.out_of_scope = "OUT_OF_SCOPE"
        # Context definition variables
        self.who = self.out_of_scope
        self.where = self.out_of_scope
        # Context status or features
        self.nesting = False
        self.loading = False
        self.to_register = []  # type: typing.List[typing.Any]

    def in_master(self) -> bool:
        """Determine if the execution is being performed in the master node.

        :return: True if in master. False on the contrary.
        """
        return self.where == self.master

    def in_worker(self) -> bool:
        """Determine if the execution is being performed in a worker node.

        :return: True if in worker. False on the contrary.
        """
        return self.where == self.worker

    def in_pycompss(self) -> bool:
        """Determine if the execution is being performed within the PyCOMPSs scope.

        :return: True if under scope. False on the contrary.
        """
        return self.where != self.out_of_scope

    def set_pycompss_context(self, where: str) -> None:
        """Set the Python Binding context (MASTER or WORKER or OUT_OF_SCOPE).

        :param where: New context (MASTER or WORKER or OUT_OF_SCOPE).
        :return: None.
        """
        assert where in [
            self.master,
            self.worker,
            self.out_of_scope,
        ], f"PyCOMPSs context must be {self.master}, {self.worker} or {self.out_of_scope}"
        self.where = where
        caller_stack = inspect.stack()[1]
        caller_module = inspect.getmodule(caller_stack[0])
        self.who = str(caller_module)

    def get_pycompss_context(self) -> str:
        """Return the PyCOMPSs context name.

        * For debugging purposes.

        :return: PyCOMPSs context name.
        """
        return self.where

    def get_who_contextualized(self) -> str:
        """Return the PyCOMPSs contextualization caller.

        * For debugging purposes.

        :return: PyCOMPSs contextualization caller name
        """
        return self.who

    def is_nesting_enabled(self) -> bool:
        """Check if nesting is enabled.

        :returns: None.
        """
        return self.nesting is True

    def enable_nesting(self) -> None:
        """Enable nesting.

        :returns: None.
        """
        self.nesting = True

    def disable_nesting(self) -> None:
        """Disable nesting.

        :returns: None.
        """
        self.nesting = False

    def is_loading(self) -> bool:
        """Check if is loading.

        :returns: None
        """
        return self.loading is True

    def enable_loading(self) -> None:
        """Enable loading.

        :returns: None.
        """
        self.loading = True

    def disable_loading(self) -> None:
        """Enable loading.

        :returns: None.
        """
        self.loading = False

    def add_to_register_later(
        self, core_element: typing.Tuple[typing.Any, str]
    ) -> None:
        """Accumulate core elements to be registered later.

        :param core_element: Core element to be registered.
        :return: None.
        """
        self.to_register.append(core_element)

    def get_to_register(self) -> typing.List[typing.Tuple[typing.Any, str]]:
        """Retrieve the to register list.

        :return: To register list.
        """
        return self.to_register


CONTEXT = Context()


#############
# FUNCTIONS #
#############


@contextmanager
def loading_context() -> typing.Iterator[None]:
    """Context which sets the loading mode.

    Intended to be used only with the @implements decorators, since they
    try to register on loading.

    :return: None.
    """
    CONTEXT.enable_loading()
    yield
    CONTEXT.disable_loading()
