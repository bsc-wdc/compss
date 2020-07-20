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

import inspect
import os
from functools import wraps
import pycompss.util.context as context
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.util.arguments import check_arguments

if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {}
SUPPORTED_ARGUMENTS = {'computing_nodes'}
DEPRECATED_ARGUMENTS = {'computingNodes'}


class MultiNode(object):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on MultiNode task creation.
    """

    def __init__(self, *args, **kwargs):
        """
        Store arguments passed to the decorator
        # self = itself.
        # args = not used.
        # kwargs = dictionary with the given MultiNode parameters

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        self.args = args
        self.kwargs = kwargs
        self.registered = False
        self.scope = context.in_pycompss()
        if self.scope:
            if __debug__:
                logger.debug("Init @multinode decorator...")

            # Check the arguments
            check_arguments(MANDATORY_ARGUMENTS,
                            DEPRECATED_ARGUMENTS,
                            SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                            list(kwargs.keys()),
                            "@multinode")

            # Replace the legacy annotation
            if 'computingNodes' in self.kwargs:
                self.kwargs['computing_nodes'] = self.kwargs.pop('computingNodes')

            # Set default value if it has not been defined
            if 'computing_nodes' not in self.kwargs:
                self.kwargs['computing_nodes'] = 1

            if __debug__:
                logger.debug(
                    "This MultiNode task will have " + str(self.kwargs['computing_nodes']) + " computing nodes.")
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
                # from pycompss.api.dummy.compss import COMPSs as dummy_compss
                # d_m = dummy_compss(self.args, self.kwargs)
                # return d_m.__call__(func)
                raise Exception(not_in_pycompss("MultiNode"))

            if context.in_master():
                # master code
                mod = inspect.getmodule(func)
                self.module = mod.__name__  # not func.__module__

                if self.module == '__main__' or \
                        self.module == 'pycompss.runtime.launch':
                    # The module where the function is defined was run as
                    # __main__, so we need to find out the real module name.

                    # Get the real module name from our launch.py variable
                    path = getattr(mod, "APP_PATH")
                    dirs = path.split(os.path.sep)
                    file_name = os.path.splitext(os.path.basename(path))[0]
                    mod_name = file_name

                    i = len(dirs) - 1
                    while i > 0:
                        new_l = len(path) - (len(dirs[i]) + 1)
                        path = path[0:new_l]
                        if "__init__.py" in os.listdir(path):
                            # directory is a package
                            i -= 1
                            mod_name = dirs[i] + '.' + mod_name
                        else:
                            break
                    self.module = mod_name

                # Include the registering info related to @compss

                # Retrieve the base core_element established at @task decorator
                from pycompss.api.task import current_core_element as cce
                if not self.registered:
                    self.registered = True
                    # Update the core element information with the
                    # @MultiNode information
                    cce.set_impl_type("MULTI_NODE")
                    # Signature and implementation args are set by the
                    # @task decorator
            else:
                # worker code
                set_slurm_environment()

            # This is executed only when called.
            if __debug__:
                logger.debug("Executing multinode_f wrapper.")

            # Set the computing_nodes variable in kwargs for its usage
            # in @task decorator
            kwargs['computing_nodes'] = self.kwargs['computing_nodes']

            if len(args) > 0:
                # The 'self' for a method function is passed as args[0]
                slf = args[0]

                # Replace and store the attributes
                saved = {}
                for k, v in self.kwargs.items():
                    if hasattr(slf, k):
                        saved[k] = getattr(slf, k)
                        setattr(slf, k, v)

            # Call the method
            ret = func(*args, **kwargs)

            if context.in_worker():
                reset_slurm_environment()

            if len(args) > 0:
                # Put things back
                for k, v in saved.items():
                    setattr(slf, k, v)

            return ret

        multinode_f.__doc__ = func.__doc__
        return multinode_f


def set_slurm_environment():
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
