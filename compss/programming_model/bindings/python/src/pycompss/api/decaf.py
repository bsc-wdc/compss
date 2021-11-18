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

from pycompss.util.typing_helper import typing
from functools import wraps

import pycompss.util.context as context
from pycompss.api.commons.constants import DF_SCRIPT
from pycompss.api.commons.constants import WORKING_DIR
from pycompss.api.commons.constants import FAIL_BY_EXIT_VALUE
from pycompss.api.commons.constants import RUNNER
from pycompss.api.commons.constants import DF_EXECUTOR
from pycompss.api.commons.constants import DF_LIB
from pycompss.api.commons.constants import COMPUTING_NODES
from pycompss.api.commons.constants import LEGACY_COMPUTING_NODES
from pycompss.api.commons.constants import LEGACY_WORKING_DIR
from pycompss.api.commons.constants import LEGACY_DF_EXECUTOR
from pycompss.api.commons.constants import LEGACY_DF_LIB
from pycompss.api.commons.constants import LEGACY_DF_SCRIPT
from pycompss.api.commons.constants import UNASSIGNED
from pycompss.api.commons.implementation_types import IMPL_DECAF
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.util.exceptions import NotInPyCOMPSsException
from pycompss.util.arguments import check_arguments
from pycompss.api.commons.decorator import resolve_working_dir
from pycompss.api.commons.decorator import resolve_fail_by_exit_value
from pycompss.api.commons.decorator import process_computing_nodes
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.runtime.task.core_element import CE

if __debug__:
    import logging
    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {DF_SCRIPT}
SUPPORTED_ARGUMENTS = {COMPUTING_NODES,
                       WORKING_DIR,
                       RUNNER,
                       DF_EXECUTOR,
                       DF_LIB,
                       DF_SCRIPT,
                       FAIL_BY_EXIT_VALUE}
DEPRECATED_ARGUMENTS = {LEGACY_COMPUTING_NODES,
                        LEGACY_WORKING_DIR,
                        LEGACY_DF_EXECUTOR,
                        LEGACY_DF_LIB,
                        LEGACY_DF_SCRIPT}


class Decaf(object):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on mpi task creation.
    """

    __slots__ = ["decorator_name", "args", "kwargs", "scope",
                 "core_element", "core_element_configured"]

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
        self.decorator_name = decorator_name
        self.args = args
        self.kwargs = kwargs
        self.scope = context.in_pycompss()
        self.core_element = None  # type: typing.Any
        self.core_element_configured = False
        if self.scope:
            # Check the arguments
            check_arguments(MANDATORY_ARGUMENTS,
                            DEPRECATED_ARGUMENTS,
                            SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                            list(kwargs.keys()),
                            decorator_name)

            # Get the computing nodes
            process_computing_nodes(decorator_name, self.kwargs)

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
            kwargs[COMPUTING_NODES] = self.kwargs[COMPUTING_NODES]

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
        if RUNNER in self.kwargs:
            runner = self.kwargs[RUNNER]
        else:
            runner = "mpirun"

        if LEGACY_DF_SCRIPT in self.kwargs:
            df_script = self.kwargs[LEGACY_DF_SCRIPT]
        else:
            df_script = self.kwargs[DF_SCRIPT]

        if DF_EXECUTOR in self.kwargs:
            df_executor = self.kwargs[DF_EXECUTOR]
        elif LEGACY_DF_EXECUTOR in self.kwargs:
            df_executor = self.kwargs[LEGACY_DF_EXECUTOR]
        else:
            df_executor = UNASSIGNED  # Empty or UNASSIGNED

        if DF_LIB in self.kwargs:
            df_lib = self.kwargs[DF_LIB]
        elif LEGACY_DF_LIB in self.kwargs:
            df_lib = self.kwargs[LEGACY_DF_LIB]
        else:
            df_lib = UNASSIGNED  # Empty or UNASSIGNED

        # Resolve the working directory
        resolve_working_dir(self.kwargs)
        # Resolve the fail by exit value
        resolve_fail_by_exit_value(self.kwargs)

        impl_type = IMPL_DECAF
        impl_signature = ".".join((impl_type, df_script))
        impl_args = [df_script,
                     df_executor,
                     df_lib,
                     self.kwargs[WORKING_DIR],
                     runner,
                     self.kwargs[FAIL_BY_EXIT_VALUE]]

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
