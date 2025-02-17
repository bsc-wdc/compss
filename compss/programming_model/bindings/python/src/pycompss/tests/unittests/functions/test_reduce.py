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


def test_merge_reduce():
    from pycompss.functions.reduce import merge_reduce

    data = list(range(11))

    def accumulate(a, b):
        return a + b

    result = merge_reduce(accumulate, data)

    assert result == 55, "ERROR: Got unexpected result with merge_reduce."


def test_merge_n_reduce():
    from pycompss.functions.reduce import merge_n_reduce

    data = list(range(11))

    def accumulate(*args):
        return sum(args)

    result = merge_n_reduce(accumulate, 5, data)

    assert result == 55, "ERROR: Got unexpected result with merge_n_reduce."
