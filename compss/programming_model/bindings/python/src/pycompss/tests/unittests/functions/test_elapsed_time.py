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


def test_elapsed_time():
    from pycompss.functions.elapsed_time import timeit

    @timeit()
    def increment(value):
        import time

        time.sleep(0.1)
        return value + 1

    result = increment(1)
    assert len(result) == 2, "ERROR: Time it does not retrieve two elements."
    assert result[0] == 2, "ERROR: Got unexpected result."
    assert isinstance(result[1], float), "ERROR: Time is in incorrect format."
    assert result[1] > 0, "ERROR: Time can not be 0 or negative."
