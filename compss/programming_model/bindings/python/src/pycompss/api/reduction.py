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
PyCOMPSs API - Reduction
==================
    This file contains the class Reduction, needed for the reduction
    of data elements.
"""

import os
import typing
from functools import wraps

import pycompss.util.context as context
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.api.commons.error_msgs import cast_env_to_int_error
from pycompss.api.commons.error_msgs import cast_string_to_int_error
from pycompss.api.commons.decorator import PyCOMPSsDecorator
from pycompss.api.commons.decorator import keep_arguments
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import PyCOMPSsException

if __debug__:
    import logging
    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = set()   # type: typing.Set[str]
SUPPORTED_ARGUMENTS = {"chunk_size",
                       "is_reduce"}
DEPRECATED_ARGUMENTS = set()  # type: typing.Set[str]


class Reduction(object):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on Reduction task creation.
    """

    def __init__(self, *args, **kwargs):
        # type: (*typing.Any, **typing.Any) -> None
        """
        Store arguments passed to the decorator
        # self = itself.
        # args = not used.
        # kwargs = dictionary with the given Reduce parameters

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        decorator_name = "".join(("@", self.__class__.__name__.lower()))
        # super(self.__class__, self).__init__(decorator_name, *args, **kwargs)
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
            self.__process_reduction_params__()

    def __call__(self, func):
        # type: (typing.Any) -> typing.Any
        """ Parse and set the reduce parameters within the task core element.

        :param func: Function to decorate
        :return: Decorated function.
        """
        @wraps(func)
        def reduce_f(*args, **kwargs):
            # type: (*typing.Any, **typing.Any) -> typing.Any
            if not self.scope:
                raise PyCOMPSsException(not_in_pycompss("reduction"))

            if __debug__:
                logger.debug("Executing reduce_f wrapper.")

            # Set the chunk size and is_reduce variables in kwargs for their
            # usage in @task decorator
            kwargs["chunk_size"] = self.kwargs["chunk_size"]
            kwargs["is_reduce"] = self.kwargs["is_reduce"]

            with keep_arguments(args, kwargs, prepend_strings=False):
                # Call the method
                ret = func(*args, **kwargs)

            return ret

        reduce_f.__doc__ = func.__doc__
        return reduce_f

    def __process_reduction_params__(self):
        # type: () -> None
        """ Processes the chunk size and is reduce from the decorator.

        :return: None
        """
        # Resolve @reduce specific parameters
        if "chunk_size" not in self.kwargs:
            chunk_size = 0
        else:
            chunk_size = self.kwargs["chunk_size"]
            if isinstance(chunk_size, int):
                # Nothing to do, it is already an integer
                pass
            elif isinstance(chunk_size, str):
                # Convert string to int
                chunk_size = self.__parse_chunk_size__(chunk_size)
            else:
                raise PyCOMPSsException(
                    "ERROR: Wrong chunk_size value at @reduction decorator.")

        if "is_reduce" not in self.kwargs:
            is_reduce = True
        else:
            is_reduce = self.kwargs["is_reduce"]

        if __debug__:
            logger.debug("The task is_reduce flag is set to: %s" %
                         str(is_reduce))
            logger.debug("This Reduction task will have %s sized chunks" %
                         str(chunk_size))

        # Set the chunk_size variable in kwargs for its usage in @task
        self.kwargs["chunk_size"] = chunk_size
        self.kwargs["is_reduce"] = is_reduce

    @staticmethod
    def __parse_chunk_size__(chunk_size):
        # type: (str) -> int
        """ Parses chunk size as string and returns its value as integer.

        :param chunk_size: Chunk size as string.
        :return: Chunk size as integer.
        :raises PyCOMPSsException: Can not cast string to int error.
        """
        # Check if it is an environment variable to be loaded
        if chunk_size.strip().startswith("$"):
            # Chunk size is an ENV variable, load it
            env_var = chunk_size.strip()[1:]  # Remove $
            if env_var.startswith("{"):
                env_var = env_var[1:-1]  # remove brackets
            try:
                parsed_chunk_size = int(os.environ[env_var])
            except ValueError:
                raise PyCOMPSsException(cast_env_to_int_error("chunk_size"))
        else:
            # ChunkSize is in string form, cast it
            try:
                parsed_chunk_size = int(chunk_size)
            except ValueError:
                raise PyCOMPSsException(cast_string_to_int_error("chunk_size"))
        return parsed_chunk_size


# ########################################################################### #
# ################# REDUCTION DECORATOR ALTERNATIVE NAME #################### #
# ########################################################################### #

reduction = Reduction
REDUCTION = Reduction
