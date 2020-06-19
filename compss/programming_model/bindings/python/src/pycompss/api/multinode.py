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
from pycompss.api.commons.decorator import process_computing_nodes
from pycompss.api.commons.decorator import get_module
from pycompss.api.commons.decorator import keep_arguments

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

            # Get the computing nodes
            process_computing_nodes(decorator_name, self.kwargs)
        else:
            pass

    def __call__(self, func):
        """
        Parse and set the multinode parameters within the task core element.

        :param func: Function to decorate
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
                self.module = get_module(func)

                if not self.registered:
                    # Register

                    # Retrieve the base core_element established at @task
                    # decorator and pdate the core element information with
                    # the @MultiNode information
                    from pycompss.api.task import current_core_element as cce
                    cce.set_impl_type("MULTI_NODE")
                    # Signature and implementation args are set by the
                    # @task decorator

                    # Set as registered
                    self.registered = True
            else:
                # worker code
                set_slurm_environment()

            # Set the computing_nodes variable in kwargs for its usage
            # in @task decorator
            kwargs['computing_nodes'] = self.kwargs['computing_nodes']

            with keep_arguments(args, kwargs, prepend_strings=True):
                # Call the method
                ret = func(*args, **kwargs)

            if context.in_worker():
                reset_slurm_environment()

            return ret

        multinode_f.__doc__ = func.__doc__
        return multinode_f


def set_slurm_environment():
    # type: () -> None
    """ Set SLURM environment.

    :return: None
    """
    num_nodes = int(os.environ["COMPSS_NUM_NODES"])
    num_threads = int(os.environ["COMPSS_NUM_THREADS"])
    total_processes = num_nodes * num_threads
    hostnames = os.environ["COMPSS_HOSTNAMES"]
    nodes = set(hostnames.split(","))
    ntasks = os.getenv("SLURM_NTASKS", None)
    if ntasks is not None:
        os.environ["OCS_NTASKS"] = ntasks
        os.environ["SLURM_NTASKS"] = str(total_processes)
    nnodes = os.getenv("SLURM_NNODES", None)
    if nnodes is not None:
        os.environ["OCS_NNODES"] = nnodes
        os.environ["SLURM_NNODES"] = str(num_nodes)
    nodelist = os.getenv("SLURM_NODELIST", None)
    if nodelist is not None:
        os.environ["OCS_NODELIST"] = nodelist
        os.environ["SLURM_NODELIST"] = ','.join(nodes)
    tasks_per_node = os.getenv("SLURM_TASKS_PER_NODE", None)
    if tasks_per_node is not None:
        os.environ["OCS_TASKS_PER_NODE"] = tasks_per_node
        os.environ["SLURM_TASKS_PER_NODE"] = str(num_threads)+"(x"+str(num_nodes)+")"
    mem_per_node = os.getenv("SLURM_MEM_PER_NODE", None)
    if mem_per_node is not None:
        os.environ["OCS_MEM_PER_NODE"] = mem_per_node
        os.environ.pop("SLURM_MEM_PER_NODE", None)
    mem_per_cpu = os.getenv("SLURM_MEM_PER_CPU", None)
    if mem_per_cpu is not None:
        os.environ["OCS_MEM_PER_CPU"] = mem_per_cpu
        os.environ.pop("SLURM_MEM_PER_CPU", None)


def reset_slurm_environment():
    # type: () -> None
    """ Reestablishes SLURM environment.

    :return: None
    """
    ntasks = os.environ.get("OCS_NTASKS", None)
    if ntasks is not None:
        os.environ["SLURM_NTASKS"] = ntasks
    nnodes = os.environ.get("OCS_NNODES", None)
    if nnodes is not None:
        os.environ["SLURM_NNODES"] = nnodes
    nodelist = os.environ.get("OCS_NODELIST", None)
    if nodelist is not None:
        os.environ["SLURM_NODELIST"] = nodelist
    tasks_per_node = os.environ.get("OCS_TASKS_PER_NODE", None)
    if tasks_per_node is not None:
        os.environ["SLURM_TASKS_PER_NODE"] = tasks_per_node
    mem_per_node = os.environ.get("OCS_MEM_PER_NODE", None)
    if mem_per_node is not None:
        os.environ["SLURM_MEM_PER_NODE"] = mem_per_node
    mem_per_cpu = os.environ.get("OCS_MEM_PER_CPU", None)
    if mem_per_cpu is not None:
        os.environ["SLURM_MEM_PER_CPU"] = mem_per_cpu


# ########################################################################### #
# ################## MultiNode DECORATOR ALTERNATIVE NAME ################### #
# ########################################################################### #

multinode = MultiNode
MULTINODE = MultiNode
