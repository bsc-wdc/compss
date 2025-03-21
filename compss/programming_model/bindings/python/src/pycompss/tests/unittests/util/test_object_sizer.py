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

import sys


class MyClass(object):
    def __init__(self):
        self.value = 1
        self.content = [1, 2, 3, 4]
        self.message = "message"
        self.more = [
            {"a": 12345, "b": 54321, "c": 10000},
            1,
            True,
            [1, 2, 3, 4],
            "test",
        ]


def test_object_sizer():
    from pycompss.util.objects.sizer import total_sizeof

    o = MyClass()
    system_size = sys.getsizeof(o)
    real_size = total_sizeof(o, handlers={list: iter}, verbose=True)
    assert real_size > system_size, "Failed checking the object size"
