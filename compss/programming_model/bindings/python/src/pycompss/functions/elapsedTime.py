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

PyCOMPSs Functions: Elapsed time decorator
==========================================
    This file defines the time it decorator to be used over the task decorator.
"""

from decorator import decorator
import time

@decorator  # Mandatory in order to preserver the argspec
def timeit(func, *a, **k):
    """
    Elapsed time decorator.
    :param func: Function to be measured (can be a decorated function, usually with @task decorator).
    :param a: args
    :param k: kwargs
    :return: a list with [the function result, The elapsed time]
    """
    ts = time.time()
    result = func(*a, **k)
    te = time.time()
    return [result, (te-ts)]