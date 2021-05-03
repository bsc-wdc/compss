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
PyCOMPSs API - MultiNode
==================
    This file contains the class MultiNode, needed for the MultiNode
    definition through the decorator.
"""

import os
from functools import wraps
import pycompss.util.context as context
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.util.arguments import check_arguments
from pycompss.api.commons.decorator import PyCOMPSsDecorator
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.runtime.task.core_element import CE

if __debug__:
    import logging
    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {}
SUPPORTED_ARGUMENTS = {'computing_nodes'}
DEPRECATED_ARGUMENTS = {'computingNodes'}


class MultiNode(PyCOMPSsDecorator):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on MultiNode task creation.
    """

    __slots__ = []

    def __init__(self, *args, **kwargs):
        """ Store arguments passed to the decorator.

        self = itself.
        args = not used.
        kwargs = dictionary with the given constraints.

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        decorator_name = "".join(('@', self.__class__.__name__.lower()))
        super(self.__class__, self).__init__(decorator_name, *args, **kwargs)
        if self.scope:
            # Check the arguments
            check_arguments(MANDATORY_ARGUMENTS,
                            DEPRECATED_ARGUMENTS,
                            SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                            list(kwargs.keys()),
                            decorator_name)

            # Get the computing nodes
            self.__process_computing_nodes__(decorator_name)
        else:
            pass

    def __call__(self, func):
        """ Parse and set the multinode parameters within the task core element.

        :param func: Function to decorate.
        :return: Decorated function.
        """

        @wraps(func)
        def multinode_f(*args, **kwargs):
            if not self.scope:
                raise Exception(not_in_pycompss("MultiNode"))

            if __debug__:
                logger.debug("Executing multinode_f wrapper.")

            if context.in_master():
                # master code
                if not self.core_element_configured:
                    self.__configure_core_element__(kwargs)
            else:
                # worker code
                if context.is_nesting_enabled():
                    if not self.core_element_configured:
                        self.__configure_core_element__(kwargs)
                old_slurm_env = set_slurm_environment()

            # Set the computing_nodes variable in kwargs for its usage
            # in @task decorator
            kwargs['computing_nodes'] = self.kwargs['computing_nodes']

            with keep_arguments(args, kwargs, prepend_strings=True):
                # Call the method
                ret = func(*args, **kwargs)

            if context.in_worker():
                reset_slurm_environment(old_slurm_env)

            return ret

        multinode_f.__doc__ = func.__doc__
        return multinode_f

    def __configure_core_element__(self, kwargs):
        # type: (dict) -> None
        """ Include the registering info related to @multinode.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @multinode core element.")

        # Resolve @multinode specific parameters
        impl_type = "MULTI_NODE"

        if CORE_ELEMENT_KEY in kwargs:
            # Core element has already been created in a higher level decorator
            # (e.g. @constraint)
            kwargs[CORE_ELEMENT_KEY].set_impl_type(impl_type)
        else:
            # @binary is in the top of the decorators stack.
            # Instantiate a new core element object, update it and include
            # it into kwarg
            core_element = CE()
            core_element.set_impl_type(impl_type)
            kwargs[CORE_ELEMENT_KEY] = core_element

        # Set as configured
        self.core_element_configured = True


def set_slurm_environment():
    # type: () -> dict
    """ Set SLURM environment.

    :return: old Slurm environment
    """
    num_nodes = int(os.environ["COMPSS_NUM_NODES"])
    num_threads = int(os.environ["COMPSS_NUM_THREADS"])
    total_processes = num_nodes * num_threads
    hostnames = os.environ["COMPSS_HOSTNAMES"]
    nodes = set(hostnames.split(","))
    old_slurm_env = remove_slurm_environment()

    # set slurm environment with COMPSs variables
    os.environ["SLURM_NTASKS"] = str(total_processes)
    os.environ["SLURM_NNODES"] = str(num_nodes)
    os.environ["SLURM_JOB_NUM_NODES"] = str(num_nodes)
    os.environ["SLURM_NODELIST"] = ','.join(nodes)
    os.environ["SLURM_JOB_NODELIST"] = ','.join(nodes)
    os.environ["SLURM_TASKS_PER_NODE"] = "".join((str(num_threads),
                                                  "(x",
                                                  str(num_nodes),
                                                  ")"))
    os.environ["SLURM_CPUS_PER_NODE"] = "".join((str(num_threads),
                                                 "(x",
                                                 str(num_nodes),
                                                 ")"))
    return old_slurm_env


def remove_slurm_environment():
    # type: () -> dict
    """ Removes the Slurm vars from environment

    :return: removed Slurm vars
    """
    old_slurm_env = dict()
    for key, value in os.environ.items():
        if key.startswith("SLURM"):
            if not (key == "SLURM_JOBID" or key == "SLURM_JOB ID" or key == "SLURM_USER"):
                old_slurm_env[key] = value
                os.environ.pop(key)


def reset_slurm_environment(old_slurm_env=None):
    # type: (dict) -> None
    """ Reestablishes SLURM environment.

    :return: None
    """
    if old_slurm_env:
        for key, value in old_slurm_env:
            os.environ[key] = value


# ########################################################################### #
# ################## MultiNode DECORATOR ALTERNATIVE NAME ################### #
# ########################################################################### #

multinode = MultiNode
MULTINODE = MultiNode
