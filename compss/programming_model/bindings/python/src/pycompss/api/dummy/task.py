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

PyCOMPSs Dummy API - task
=========================
    This file contains the dummy class task used as decorator.

    # How to use it:
    try:
        from pycompss.api.parameter import *
        from pycompss.api.task import task
        from pycompss.api.constraint import constraint
    except ImportError:
        from pycompss.api.dummy.parameter import *
        from pycompss.api.dummy.task import task
        from pycompss.api.dummy.constraint import constraint

    @constraint(ProcessorCoreCount=8)
    @task(returns=list, a=FILE_IN)
    def foo(a, b):
        return (a, b)

    def main():
        res = foo(1, 2)
        try:
            from pycompss.api.api import compss_wait_on
        except ImportError:
            from pycompss.api.dummy.api import compss_wait_on

        res = compss_wait_on(res)
        print res

    if __name__ == "__main__":
        main()
"""


class task(object):
    """
    Dummy task class (decorator style)
    """

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __call__(self, f):
        def wrapped_f(*args, **kwargs):
            return f(*args, **kwargs)
        return wrapped_f
