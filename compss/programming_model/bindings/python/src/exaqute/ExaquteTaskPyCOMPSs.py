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

from pycompss.api.task import task
from pycompss.api.api import compss_wait_on
from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_delete_object
from pycompss.api.api import compss_delete_file
from pycompss.api.mpi import mpi as _mpi
from pycompss.api.parameter import *   # NOSONAR
from pycompss.api.implement import implement 
from pycompss.api.constraint import constraint
import pycompss.util.context as context
from functools import wraps


class Mpi(_mpi):

    def __call__(self, user_function):
        """ Parse and set the mpi parameters within the task core element.

        :param user_function: Function to decorate.
        :return: Decorated function.
        """
        @wraps(user_function)
        def mpi_f(*args, **kwargs):
            ret = self.__decorator_body__(user_function, args, kwargs)
            if context.in_master() and int(self.kwargs['processes']) == 1:
                return [ret]
            else:
                return ret

        mpi_f.__doc__ = user_function.__doc__
        return mpi_f


def barrier():  # Wait
    compss_barrier()


def get_value_from_remote(obj):  # Gather
    obj = compss_wait_on(obj)
    return obj


def delete_object(*objs):  # Release
    for obj in objs:
        compss_delete_object(obj)


def delete_file(file_path):
    compss_delete_file(file_path)


def compute(obj):  # Submit task
    return obj


ExaquteTask = task
MPI = Mpi
mpi = Mpi
CONSTRAINT = constraint
Constraint = constraint
IMPLEMENT = implement
Implement = implement
