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
PyCOMPSs API - DECAF
====================
    This file contains the class decaf, needed for the @decaf task
    definition through the decorator.
"""

import typing
from functools import wraps

import pycompss.util.context as context
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.util.exceptions import NotInPyCOMPSsException
from pycompss.util.arguments import check_arguments
from pycompss.api.commons.decorator import PyCOMPSsDecorator
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.runtime.task.core_element import CE

if __debug__:
    import logging
    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {"df_script"}
SUPPORTED_ARGUMENTS = {"computing_nodes",
                       "working_dir",
                       "runner",
                       "df_executor",
                       "df_lib",
                       "df_script",
                       "fail_by_exit_value"}
DEPRECATED_ARGUMENTS = {"computingNodes",
                        "workingDir",
                        "dfExecutor",
                        "dfLib",
                        "dfScript"}


class Decaf(object):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on mpi task creation.
    """

    def __init__(self, *args, **kwargs):
        # type: (*typing.Any, **typing.Any) -> None
        """ Store arguments passed to the decorator

        self = itself.
        args = not used.
        kwargs = dictionary with the given constraints.

        :param args: Arguments.
        :param kwargs: Keyword arguments.
        """
        decorator_name = "".join(("@", Decaf.__name__.lower()))
        # super(Decaf, self).__init__(decorator_name, *args, **kwargs)
        # Instantiate superclass explicitly to support mypy.
        pd = PyCOMPSsDecorator(decorator_name, *args, **kwargs)
        self.decorator_name = decorator_name
        self.args = args
        self.kwargs = kwargs
        self.scope = context.in_pycompss()
        self.core_element = None  # type: typing.Any
        self.core_element_configured = False
        self.__configure_core_element__ = pd.__configure_core_element__
        self.__resolve_working_dir__ = pd.__resolve_working_dir__
        self.__resolve_fail_by_exit_value__ = pd.__resolve_fail_by_exit_value__
        self.__process_computing_nodes__ = pd.__process_computing_nodes__
        if self.scope:
            # Check the arguments
            check_arguments(MANDATORY_ARGUMENTS,
                            DEPRECATED_ARGUMENTS,
                            SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                            list(kwargs.keys()),
                            decorator_name)

            # Get the computing nodes
            self.__process_computing_nodes__(decorator_name)

    def __call__(self, user_function):
        # type: (typing.Any) -> typing.Any
        """ Parse and set the decaf parameters within the task core element.

        :param user_function: Function to decorate.
        :return: Decorated function.
        """
        @wraps(user_function)
        def decaf_f(*args, **kwargs):
            # type: (*typing.Any, **typing.Any) -> typing.Any
            if not self.scope:
                raise NotInPyCOMPSsException(not_in_pycompss("decaf"))

            if __debug__:
                logger.debug("Executing decaf_f wrapper.")

            if (context.in_master() or context.is_nesting_enabled()) \
                    and not self.core_element_configured:
                # master code - or worker with nesting enabled
                self.__configure_core_element__(kwargs, user_function)

            # Set the computing_nodes variable in kwargs for its usage
            # in @task decorator
            kwargs["computing_nodes"] = self.kwargs["computing_nodes"]

            with keep_arguments(args, kwargs, prepend_strings=False):
                # Call the method
                ret = user_function(*args, **kwargs)

            return ret

        decaf_f.__doc__ = user_function.__doc__
        return decaf_f

    def __configure_core_element__(self, kwargs, user_function):
        # type: (dict, typing.Any) -> None
        """ Include the registering info related to @decaf.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :param user_function: Decorated function.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @decaf core element.")

        # Resolve @decaf specific parameters
        if "runner" in self.kwargs:
            runner = self.kwargs["runner"]
        else:
            runner = "mpirun"

        if "dfScript" in self.kwargs:
            df_script = self.kwargs["dfScript"]
        else:
            df_script = self.kwargs["df_script"]

        if "df_executor" in self.kwargs:
            df_executor = self.kwargs["df_executor"]
        elif "dfExecutor" in self.kwargs:
            df_executor = self.kwargs["dfExecutor"]
        else:
            df_executor = "[unassigned]"  # Empty or "[unassigned]"

        if "df_lib" in self.kwargs:
            df_lib = self.kwargs["df_lib"]
        elif "dfLib" in self.kwargs:
            df_lib = self.kwargs["dfLib"]
        else:
            df_lib = "[unassigned]"  # Empty or "[unassigned]"

        # Resolve the working directory
        self.__resolve_working_dir__()
        # Resolve the fail by exit value
        self.__resolve_fail_by_exit_value__()

        impl_type = "DECAF"
        impl_signature = ".".join((impl_type, df_script))
        impl_args = [df_script,
                     df_executor,
                     df_lib,
                     self.kwargs["working_dir"],
                     runner,
                     self.kwargs["fail_by_exit_value"]]

        if CORE_ELEMENT_KEY in kwargs:
            # Core element has already been created in a higher level decorator
            # (e.g. @constraint)
            kwargs[CORE_ELEMENT_KEY].set_impl_type(impl_type)
            kwargs[CORE_ELEMENT_KEY].set_impl_signature(impl_signature)
            kwargs[CORE_ELEMENT_KEY].set_impl_type_args(impl_args)
        else:
            # @binary is in the top of the decorators stack.
            # Instantiate a new core element object, update it and include
            # it into kwarg
            core_element = CE()
            core_element.set_impl_type(impl_type)
            core_element.set_impl_signature(impl_signature)
            core_element.set_impl_type_args(impl_args)
            kwargs[CORE_ELEMENT_KEY] = core_element

        # Set as configured
        self.core_element_configured = True


# ########################################################################### #
# #################### DECAF DECORATOR ALTERNATIVE NAME ##################### #
# ########################################################################### #

decaf = Decaf
DECAF = Decaf
