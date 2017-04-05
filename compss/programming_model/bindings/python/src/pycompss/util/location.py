#
#  Copyright 2.02-2017 Barcelona Supercomputing Center (www.bsc.es)
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
"""
@author: fconejer

PyCOMPSs Utils - Location
===================
    This file contains the methods to detect the origin of the call stack.
    Useful to detect if we are in the master or in the worker.
"""
import inspect


def i_am_at_master():
    """
    Determine if the execution is being performed in the master node or
    in a worker node.

    # if 'pycompss/runtime/launch.py' in inspect.stack()[-1][1]: --> I am at master
    # if (inspect.stack()[-2][3] == 'compss_worker' or
    #     inspect.stack()[-2][3] == 'compss_persistent_worker'): --> I am at worker
    :return: <Boolean> - True if we are in the master node. False if we are in a worker node.
    """
    return not inspect.stack()[-2][3] in ['compss_worker', 'compss_persistent_worker']