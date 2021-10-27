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
PyCOMPSs API - HTTP
==================
    HTTP Task decorator class.
"""
import json
from functools import wraps
import pycompss.util.context as context
from pycompss.api import binary, mpi
from pycompss.api.commons.decorator import PyCOMPSsDecorator
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import PyCOMPSsException

if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {"config_file"}
SUPPORTED_ARGUMENTS = set()
DEPRECATED_ARGUMENTS = set()

SUPPORTED_DECORATORS = {"mpi": (mpi, mpi.mpi),
                        "binary": (binary, binary.binary)}


class Software(PyCOMPSsDecorator):
    """
    """

    __slots__ = ['task_type', 'config_args', 'decor']

    def __init__(self, *args, **kwargs):
        """ Store arguments passed to the decorator.

        self = itself.
        args = not used.
        kwargs = dictionary with the given mpi parameters.

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        decorator_name = "".join(('@', Software.__name__.lower()))
        super(Software, self).__init__(decorator_name, *args, **kwargs)
        self.task_type = None
        self.config_args = None
        self.decor = None

        if self.scope:
            if __debug__:
                logger.debug("Init @software decorator..")

            # Check the arguments
            check_arguments(MANDATORY_ARGUMENTS,
                            DEPRECATED_ARGUMENTS,
                            SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                            list(kwargs.keys()),
                            decorator_name)
            self.parse_config_file()

    def __call__(self, user_function):
        """
        :param user_function: Function to decorate.
        :return: Decorated function.
        """

        # todo: add comments
        @wraps(user_function)
        def software_f(*args, **kwargs):
            decorator = self.decor

            def decor_f():
                def f():
                    ret = decorator(**self.config_args)
                    return ret(user_function)(args, kwargs)
                return f()
            return decor_f()

        software_f.__doc__ = user_function.__doc__
        return software_f

    def parse_config_file(self):

        file_path = self.kwargs['config_file']
        config = json.load(open(file_path, "r"))

        exec_type = config["type"]
        if not exec_type or exec_type.lower() not in SUPPORTED_DECORATORS:
            msg = "Error: Executor Type {} is not supported for software task."\
                .format(exec_type)
            raise PyCOMPSsException(msg)

        exec_type = exec_type.lower()
        self.task_type, self.decor = SUPPORTED_DECORATORS[exec_type]

        arguments = config["arguments"]
        mand_args = self.task_type.MANDATORY_ARGUMENTS
        if not all(arg in arguments for arg in mand_args):
            msg = "Error: Missing arguments for '{}'.".format(self.task_type)
            raise PyCOMPSsException(msg)

        self.config_args = arguments


# ########################################################################### #
# ##################### Software DECORATOR ALTERNATIVE NAME ################# #
# ########################################################################### #


software = Software
