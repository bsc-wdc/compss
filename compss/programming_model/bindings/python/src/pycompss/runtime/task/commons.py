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

try:
    from functools import lru_cache
except ImportError:
    from functools32 import lru_cache
import pycompss.api.parameter as parameter
from pycompss.runtime.task.parameter import get_new_parameter


class TaskCommons(object):
    """
    Code shared by the TaskMaster and TaskWorker.
    Both classes inherit from TaskCommons.
    """

    __slots__ = ['user_function', 'decorator_arguments',
                 'param_args', 'param_varargs']

    def __init__(self,
                 decorator_arguments,
                 user_function):
        self.user_function = user_function
        self.decorator_arguments = decorator_arguments
        self.param_args = None
        self.param_varargs = None

    @lru_cache(maxsize=128)
    def get_varargs_direction(self):
        # type: () -> ... # Parameter
        """ Returns the direction of the varargs arguments.

        Can be defined in the decorator in two ways:
            args = dir, where args is the name of the variadic args tuple.
            varargs_type = dir (for legacy reasons).

        :return: Direction of the varargs arguments.
        """
        if self.param_varargs not in self.decorator_arguments:
            if 'varargsType' in self.decorator_arguments:
                self.param_varargs = 'varargsType'
                return self.decorator_arguments['varargsType']
            else:
                return self.decorator_arguments['varargs_type']
        return self.decorator_arguments[self.param_varargs]

    @lru_cache(maxsize=128)
    def get_default_direction(self, var_name):
        # type: (str) -> ...  # Parameter
        """ Returns the default direction for a given parameter.

        :param var_name: Variable name.
        :return: An identifier of the direction.
        """
        # We are the 'self' or 'cls' in an instance or classmethod that
        # modifies the given class, so we are an INOUT, CONCURRENT or
        # COMMUTATIVE
        self_dirs = [parameter.DIRECTION.INOUT,
                     parameter.DIRECTION.CONCURRENT,
                     parameter.DIRECTION.COMMUTATIVE]
        if 'targetDirection' in self.decorator_arguments:
            target_label = 'targetDirection'
        else:
            target_label = 'target_direction'
        if self.decorator_arguments[target_label].direction in self_dirs and \
                var_name in ['self', 'cls'] and \
                self.param_args and \
                self.param_args[0] == var_name:
            return self.decorator_arguments[target_label]
        return get_new_parameter('IN')
