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
@author: scorella

PyCOMPSs Functions: Data generators
===================================
    This file defines the common data producing functions.
"""


def sort(iterable, comp=None, key=None, reverse=False):
    """
    Apply function of two arguments cumulatively to the items of data, from left to right,
    so as to reduce the iterable to a single value.
    :param iterable: data.
    :param comp: specifies a custom comparison function of two arguments.
    :param key: specifies a function of one argument that is used to extract a comparison key from each list element.
    :param reverse: if set to True, then the list elements are sorted as if each comparison were reversed.
    :return: a new sorted list from the items in iterable.
    """
    try:
        return sorted(iterable, comp, key, reverse)
    except Exception, e:
        raise e
