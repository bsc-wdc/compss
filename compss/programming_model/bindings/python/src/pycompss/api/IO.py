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
PyCOMPSs API - IO
=====================
    This file contains the class constraint, needed for the IO task
    definition through the decorator.
"""

from functools import wraps
import pycompss.util.context as context
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.util.arguments import check_arguments
from pycompss.api.commons.decorator import PyCOMPSsDecorator
from pycompss.api.commons.decorator import get_module
from pycompss.api.commons.decorator import keep_arguments

if __debug__:
    import logging
    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {}
SUPPORTED_ARGUMENTS = {''}
DEPRECATED_ARGUMENTS = {''}


class IO(PyCOMPSsDecorator):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on IO task creation.
    """

    def __init__(self, *args, **kwargs):
        """
        Store arguments passed to the decorator
        # self = itself.
        # args = not used.
        # kwargs = dictionary with the given constraints.

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        decorator_name = '@' + self.__class__.__name__.lower()
        super(self.__class__, self).__init__(decorator_name, *args, **kwargs)
        if self.scope:
            # Check the arguments
            check_arguments(MANDATORY_ARGUMENTS,
                            DEPRECATED_ARGUMENTS,
                            SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                            list(kwargs.keys()),
                            decorator_name)

    def __call__(self, func):
        """
        Parse and set the IO parameters within the task core element.

        :param func: Function to decorate
        :return: Decorated function.
        """
        @wraps(func)
        def io_f(*args, **kwargs):
            if not self.scope:
                # from pycompss.api.dummy.binary import binary as dummy_binary
                # d_b = dummy_binary(self.args, self.kwargs)
                # return d_b.__call__(func)
                raise Exception(not_in_pycompss("IO"))

            if __debug__:
                logger.debug("Executing IO_f wrapper.")

            if context.in_master():
                # master code
                self.module = get_module(func)

                if not self.registered:
                    # Register

                    # Retrieve the base core_element established at @task
                    # decorator and update the core element information with
                    # the @IO argument information
                    from pycompss.api.task import current_core_element as cce
                    cce.set_impl_io(True)
                    # Set as registered
                    self.registered = True
            else:
                # worker code
                pass

            with keep_arguments(args, kwargs, prepend_strings=True):
                # Call the method
                ret = func(*args, **kwargs)

            return ret

        io_f.__doc__ = func.__doc__
        return io_f


# ########################################################################### #
# ###################### IO DECORATOR ALTERNATIVE NAME ###################### #
# ########################################################################### #

io = IO
