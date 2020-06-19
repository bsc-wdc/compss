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
PyCOMPSs API - COMPSs
==================
    This file contains the class COMPSs, needed for the compss
    definition through the decorator.
"""

from functools import wraps
import pycompss.util.context as context
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.api.commons.decorator import PyCOMPSsDecorator
from pycompss.api.commons.decorator import process_computing_nodes
from pycompss.util.arguments import check_arguments
from pycompss.api.commons.decorator import get_module

if __debug__:
    import logging
    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {'app_name'}
SUPPORTED_ARGUMENTS = {'computing_nodes',
                       'runcompss',
                       'flags',
                       'worker_in_master',
                       'app_name',
                       'working_dir',
                       'fail_by_exit_value'}
DEPRECATED_ARGUMENTS = {'computingNodes',
                        'workerInMaster',
                        'appName',
                        'workingDir'}


class COMPSs(PyCOMPSsDecorator):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on compss task creation.
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
        Parse and set the compss parameters within the task core element.

        :param func: Function to decorate
        :return: Decorated function.
        """
        @wraps(func)
        def compss_f(*args, **kwargs):
            if not self.scope:
                # from pycompss.api.dummy.compss import COMPSs as dummy_compss
                # d_m = dummy_compss(self.args, self.kwargs)
                # return d_m.__call__(func)
                raise Exception(not_in_pycompss("compss"))

            if __debug__:
                logger.debug("Executing compss_f wrapper.")

            if context.in_master():
                # master code
                self.module = get_module(func)

                if not self.registered:
                    # Resolve @compss specific parameters
                    if 'runcompss' in self.kwargs:
                        runcompss = self.kwargs['runcompss']
                    else:
                        runcompss = '[unassigned]'  # Empty or '[unassigned]'

                    if 'flags' in self.kwargs:
                        flags = self.kwargs['flags']
                    else:
                        flags = '[unassigned]'  # Empty or '[unassigned]'

                    if 'worker_in_master' in self.kwargs:
                        worker_in_master = self.kwargs['worker_in_master']
                    elif 'workerInMaster' in self.kwargs:
                        worker_in_master = self.kwargs['workerInMaster']
                    else:
                        worker_in_master = 'true'  # Empty or '[unassigned]'

                    if 'appName' in self.kwargs:
                        app_name = self.kwargs['appName']
                    else:
                        app_name = self.kwargs['app_name']

                    # Resolve the working directory
                    self.__resolve_working_dir__()
                    # Resolve the fail by exit value
                    self.__resolve_fail_by_exit_value__()

                    impl_type = 'COMPSs'
                    impl_signature = '.'.join((impl_type, app_name))
                    impl_args = [runcompss,
                                 flags,
                                 app_name,
                                 worker_in_master,
                                 self.kwargs['working_dir'],
                                 self.kwargs['fail_by_exit_value']]

                    # Retrieve the base core_element established at @task
                    # decorator and update the core element information with
                    # the @compss decorator information
                    from pycompss.api.task import current_core_element as cce
                    cce.set_impl_type(impl_type)
                    cce.set_impl_signature(impl_signature)
                    cce.set_impl_type_args(impl_args)

                    # Set as registered
                    self.registered = True
            else:
                # worker code
                pass

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
            import pycompss.api.task as t
            t.prepend_strings = False
            ret = func(*args, **kwargs)
            t.prepend_strings = True

            if len(args) > 0:
                # Put things back
                for k, v in saved.items():
                    setattr(slf, k, v)

            return ret

        compss_f.__doc__ = func.__doc__
        return compss_f


# ########################################################################### #
# #################### COMPSs DECORATOR ALTERNATIVE NAME #################### #
# ########################################################################### #

compss = COMPSs
