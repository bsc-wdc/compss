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


class ExaquteTask(object):

    def __init__(self, *args, **kwargs):
        # Required declaration to allow decorator parameters
        pass

    def __call__(self, f):
        def et_wrapper(*args, **kwargs):
            if "scheduling_constraints" in kwargs:
                del kwargs["scheduling_constraints"]
            return f(*args, **kwargs)

        return et_wrapper


def get_value_from_remote(obj):  # Gather
    return obj


def barrier():  # Wait
    pass


def delete_object(*objs):  # Release
    for obj in objs:
        del obj


def delete_file(file_path):
    import os
    os.remove(file_path)


def compute(obj):  # Submit task
    return obj


class Implement(object):

    def __init__(self, *args, **kwargs):
        # Required declaration to allow decorator parameters
        pass

    def __call__(self, f):
        return f


class Constraint(object):

    def __init__(self, *args, **kwargs):
        # Required declaration to allow decorator parameters
        pass

    def __call__(self, f):
        return f

class Mpi(object):

    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, f):
        return f

# Enable lower and capital decorators naming:
implement = Implement
IMPLEMENT = Implement
constraint = Constraint
CONSTRAINT = Constraint
mpi = Mpi
MPI = MPI
