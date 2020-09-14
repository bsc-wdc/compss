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
from pycompss.api.task import task
from pycompss.api.constraint import constraint
from pycompss.api.api import compss_wait_on


@task(returns=1)
def increment(value):
    return value + 2


@constraint(computing_units=1)
@task(returns=1)
def decrement(value):
    return value - 1


def main():
    initial = 1
    partial = increment(initial)
    result = decrement(partial)
    result = compss_wait_on(result)
    assert result == initial + 1, "ERROR: Unexpected increment result."


# if __name__ == '__main__':
#     main()
