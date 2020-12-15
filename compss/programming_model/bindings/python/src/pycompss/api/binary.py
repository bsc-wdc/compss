#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs API - BINARY
=====================
    This file contains the class constraint, needed for the binary task
    definition through the decorator.
"""

from functools import wraps
import pycompss.util.context as context
from pycompss.util.arguments import check_arguments
from pycompss.api.commons.decorator import PyCOMPSsDecorator
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.decorator import run_command
from pycompss.runtime.task.core_element import CE

if __debug__:
    import logging
    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {'binary'}
SUPPORTED_ARGUMENTS = {'binary',
                       'working_dir',
                       'fail_by_exit_value'}
DEPRECATED_ARGUMENTS = {'workingDir',
                        'engine',
                        'image'}


class Binary(PyCOMPSsDecorator):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on mpi task creation.
    """

    __slots__ = []

    def __init__(self, *args, **kwargs):
        """ Store arguments passed to the decorator.

        self = itself.
        args = not used.
        kwargs = dictionary with the given constraints.

        :param args: Arguments.
        :param kwargs: Keyword arguments.
        """
        decorator_name = "".join(('@', Binary.__name__.lower()))
        super(Binary, self).__init__(decorator_name, *args, **kwargs)
        if self.scope:
            # Check the arguments
            check_arguments(MANDATORY_ARGUMENTS,
                            DEPRECATED_ARGUMENTS,
                            SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                            list(kwargs.keys()),
                            decorator_name)

    def __call__(self, user_function):
        """ Parse and set the binary parameters within the task core element.

        :param user_function: Function to decorate
        :return: Decorated function.
        """

        @wraps(user_function)
        def binary_f(*args, **kwargs):
            if not self.scope:
                # Execute the binary as with PyCOMPSs so that sequential
                # execution performs as parallel.
                # To disable: raise Exception(not_in_pycompss("binary"))
                # TODO: Intercept @task parameters to get stream redirection
                return self.__run_binary__(args, kwargs)

            if __debug__:
                logger.debug("Executing binary_f wrapper.")

            if (context.in_master() or context.is_nesting_enabled()) \
                    and not self.core_element_configured:
                # master code - or worker with nesting enabled
                self.__configure_core_element__(kwargs, user_function)

            with keep_arguments(args, kwargs, prepend_strings=False):
                # Call the method
                ret = user_function(*args, **kwargs)

            return ret

        binary_f.__doc__ = user_function.__doc__
        return binary_f

    def __run_binary__(self, *args, **kwargs):
        # type: (..., dict) -> int
        """ Runs the binary defined in the decorator when used as dummy.

        :param args: Arguments received from call.
        :param kwargs: Keyword arguments received from call.
        :return: Execution return code.
        """
        cmd = [self.kwargs['binary']]
        return_code = run_command(cmd, args, kwargs)
        return return_code

    def __configure_core_element__(self, kwargs, user_function):
        # type: (dict, ...) -> None
        """ Include the registering info related to @binary.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :param user_function: Decorated function.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @binary core element.")

        # Resolve the working directory
        self.__resolve_working_dir__()
        _working_dir = self.kwargs['working_dir']

        # Resolve the fail by exit value
        self.__resolve_fail_by_exit_value__()
        _fail_by_ev = self.kwargs['fail_by_exit_value']

        # Resolve binary
        _binary = str(self.kwargs['binary'])

        if CORE_ELEMENT_KEY in kwargs and \
                kwargs[CORE_ELEMENT_KEY].get_impl_type() == 'CONTAINER':
            # @container decorator sits on top of @binary decorator
            # Note: impl_type and impl_signature are NOT modified
            # ('CONTAINER' and 'CONTAINER.function_name' respectively)

            impl_args = kwargs[CORE_ELEMENT_KEY].get_impl_type_args()

            _engine = impl_args[0]
            _image = impl_args[1]

            impl_args = [_engine,  # engine
                         _image,  # image
                         'CET_BINARY',  # internal_type
                         _binary,  # internal_binary
                         '[unassigned]',  # internal_func
                         _working_dir,  # working_dir
                         _fail_by_ev]  # fail_by_ev

            kwargs[CORE_ELEMENT_KEY].set_impl_type_args(impl_args)
        else:
            # @container decorator does NOT sit on top of @binary decorator

            _binary = str(self.kwargs['binary'])

            impl_type = 'BINARY'
            impl_signature = '.'.join((impl_type, _binary))

            impl_args = [_binary,  # internal_binary
                         _working_dir,  # working_dir
                         _fail_by_ev]  # fail_by_ev

            if CORE_ELEMENT_KEY in kwargs:
                # Core element has already been created in a higher level
                # decorator (e.g. @constraint)
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
# ################### BINARY DECORATOR ALTERNATIVE NAME ##################### #
# ########################################################################### #

binary = Binary
BINARY = Binary
