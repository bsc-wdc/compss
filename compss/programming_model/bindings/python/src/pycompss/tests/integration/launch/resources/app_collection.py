#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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

"""PyCOMPSs Testbench for Collections."""

from pycompss.api.api import compss_wait_on
from pycompss.api.parameter import COLLECTION
from pycompss.api.parameter import COLLECTION_IN
from pycompss.api.parameter import COLLECTION_INOUT
from pycompss.api.parameter import DICTIONARY_IN
from pycompss.api.parameter import DICTIONARY_INOUT
from pycompss.api.reduction import reduction
from pycompss.api.task import task


class Polygon(object):
    """Polygon class."""

    def __init__(self, sides):
        """Create a new Polygon with the given sides.

        :param sides: Number of sides.
        :returns: None.
        """
        self.sides = sides

    def increment(self, amount):
        """Increment the number of sides.

        :param amount: Number of sides to be incremented.
        :returns: None
        """
        self.sides += amount

    def get_sides(self):
        """Retrieve the number of sides.

        :returns: The number of sides.
        """
        return self.sides


# COLLECTIONS


def generate_collection(value):
    """Populate the given list with three polygons.

    :param value: List to be populated with three polygons.
    :returns: None.
    """
    value.append(Polygon(2))
    value.append(Polygon(10))
    value.append(Polygon(20))


@task(value=COLLECTION_INOUT)
def update_collection(value):
    """Update the number of sides of all polygons in the given collection.

    :param value: List of polygons.
    :returns: None.
    """
    for c in value:
        c.increment(1)


@task(returns=1, value=COLLECTION_IN)
def sum_all_sides(value):
    """Sum all sides from all polygons from the given collection.

    :param value: List of polygons.
    :returns: Total number of sides of all polygons.
    """
    result = 0
    for c in value:
        result += c.get_sides()
    return result


# DICTIONARY COLLECTIONS


def generate_dictionary(value):
    """Populate the given dictionary with three polygons.

    :param value: Dictionary to be populated with three polygons.
    :returns: None.
    """
    value["a"] = Polygon(3)
    value["b"] = Polygon(10)
    value["c"] = Polygon(20)


@task(value=DICTIONARY_INOUT)
def update_dictionary(value):
    """Update the number of sides of all polygons in the given dictionary.

    :param value: Dictionary of polygons.
    :returns: None.
    """
    for key in value.keys():
        value[key].increment(1)


@task(returns=2, value=DICTIONARY_IN)
def sum_all_sides_of_dictionary(value):
    """Sum all sides from all polygons from the given dictionary.

    :param value: Dictionary of polygons.
    :returns: Total number of sides of all polygons.
    """
    keys = ""
    result = 0
    for k, v in value.items():
        keys += k
        result += v.get_sides()
    return keys, result


# REDUCE WITH COLLECTIONS


@reduction(chunk_size="2")
@task(returns=1, col=COLLECTION_IN)
def my_reduction(col):
    """Accumulate all elements in the given collection.

    :param col: Collection of integers.
    :returns: Accumulated value of all integers in the given collection.
    """
    r = 0
    for i in col:
        r += i
    return r


@task(returns=1)
def increment(v):
    """Increment the given value with 1.

    :param v: Integer to increment.
    :returns: Incremented value with 1.
    """
    return v + 1


@task(returns=COLLECTION)
def generate_collection_return():
    """Create a return collection with three polygons.

    :returns: A collection with three polygons.
    """
    value = [Polygon(2), Polygon(10), Polygon(20)]
    return value


def main():
    """Execute all collection tests.

    :returns: None.
    """
    initial = []
    generate_collection(initial)
    update_collection(initial)
    result = sum_all_sides(initial)
    result = compss_wait_on(result)
    assert result == 35, "ERROR: Unexpected result (%s != 35)." % str(result)

    initial = {}
    generate_dictionary(initial)
    update_dictionary(initial)
    keys, result = sum_all_sides_of_dictionary(initial)
    keys = compss_wait_on(keys)
    result = compss_wait_on(result)
    assert (
        len(keys) == 3 and "a" in keys and "b" in keys and "c" in keys
    ), "ERROR: Unexpected keys (%s != abc (in any order))." % str(keys)
    assert result == 36, "ERROR: Unexpected result (%s != 36)." % str(result)

    # Reduction
    num_tasks = 5
    a = [x for x in range(1, num_tasks + 1)]
    result = []
    for element in a:
        result.append(increment(element))
    # Reduction task
    final = my_reduction(result)
    final = compss_wait_on(final)
    assert final == 20, "ERROR: Unexpected result (%s != 20)." % str(result)

    # Collection return
    result = generate_collection_return()
    results = compss_wait_on(result)
    assert (
        len(results) == 3
    ), "ERROR: The generated collection does not have the expected length."  # noqa: E501
    assert (
        results[0].get_sides() == 2
        and results[1].get_sides() == 10
        and results[2].get_sides() == 20
    ), "ERROR: The collection contents are not as expected"  # noqa: E501


# Uncomment for command line check:
# if __name__ == '__main__':
#     main()
