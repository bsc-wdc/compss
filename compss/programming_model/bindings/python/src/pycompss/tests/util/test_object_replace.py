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

# -*- coding: utf-8 -*-

import sys


class MyClass(object):
    def __init__(self, value, content, message):
        self.value = value
        self.content = content
        self.message = message


def test_object_replace():
    from pycompss.util.objects.replace import replace

    o = MyClass(1, [1, 2, 3, 4], "hello world!")
    p = MyClass(100, [40, 30, 20, 10], "goodbye world!")

    assert id(o) != id(p), "ERROR: The objects have the same identifier."

    replace(o, p)

    assert id(o) == id(p), "ERROR: The objects do not have the same identifier."
