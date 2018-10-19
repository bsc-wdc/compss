#
#  Copyright 2018 Barcelona Supercomputing Center (www.bsc.es)
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
from pycompss.api.api import compss_wait_on
from pycompss.api.parameter import INOUT, IN
from pycompss.api.task import task


@task(returns=1, first=INOUT)
def merge_dicts(first, second, merger_function):
    """
    """
    for key, val in second.items():
        first[key] = merger_function(first[key], val) if key in first else val


@task(returns=1)
def map_partition(f, loader):
    """
    """
    res = f(loader)
    return res


@task(returns=1)
def reduce_partition(f, partition):
    """
    TODO: Should it be a Tree Reducer as well?
    """
    iterator = iter(partition)
    try:
        res = next(iterator)
    except StopIteration:
        return

    for el in partition:
        res = f(res, el)

    return res


@task(returns=1)
def reduce_multiple(f, *args):
    """
    """
    partitions = iter(args)
    try:
        res = next(partitions)[0]
    except StopIteration:
        return

    for part in partitions:
        if part:
            res = f(res, part[0])

    return [res]
