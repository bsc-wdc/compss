#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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

from pycompss.dds.heapq3 import merge


GENERIC_ERROR = "ERROR: Unexpected result from heapq."


def test_heapq_1():
    result = list(
        merge(
            [[1, 3, 5, 7], [0, 2, 4, 8], [5, 10, 15, 20], [], [25]],
            key=None,
            reverse=False,
        )
    )
    assert result == [0, 1, 2, 3, 4, 5, 5, 7, 8, 10, 15, 20, 25], GENERIC_ERROR


def test_heapq_reverse():
    result = list(
        merge(
            [[1, 3, 5, 7], [0, 2, 4, 8], [5, 10, 15, 20], [], [25]],
            key=None,
            reverse=True,
        )
    )
    assert result == [25, 5, 10, 15, 20, 1, 3, 5, 7, 0, 2, 4, 8], GENERIC_ERROR


def test_heapq_key():
    result = list(
        merge([["dog", "horse"], ["cat", "fish", "kangaroo"]], key=len, reverse=False)
    )
    assert result == ["dog", "cat", "fish", "horse", "kangaroo"], GENERIC_ERROR


def test_heapq_key_reversed():
    result = list(
        merge([["dog", "horse"], ["cat", "fish", "kangaroo"]], key=len, reverse=True)
    )
    assert result == ["dog", "horse", "cat", "fish", "kangaroo"], GENERIC_ERROR
