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


class MyClass(object):
    def __init__(self, value, content, message):
        self.value = value
        self.content = content
        self.message = message


def test_object_replace():
    try:
        from pycompss.util.objects.replace import replace
    except ImportError:
        raise Exception("UNSUPPORTED WITH MYPY")

    o = MyClass(1, [1, 2, 3, 4], "hello world!")
    p = MyClass(100, [40, 30, 20, 10], "goodbye world!")

    assert id(o) != id(p), "ERROR: The objects have the same identifier."

    replace(o, p)

    assert id(o) == id(
        p
    ), "ERROR: The objects do not have the same identifier."


# # Commented out due to incompatibility with mypy
# def test_replace_main():
#     from pycompss.util.objects.replace import examine_vars
#     from pycompss.util.objects.replace import a
#     from pycompss.util.objects.replace import U
#     from pycompss.util.objects.replace import S
#     from pycompss.util.objects.replace import replace
#     from pycompss.util.objects.replace import b
#     from pycompss.util.objects.replace import V
#     from pycompss.util.objects.replace import T
#     # Does the same as __main__
#     examine_vars(id(a), id(U), id(S))
#     print("-" * 35)
#     replace(a, b)
#     replace(U, V)
#     replace(S, T)
#     print("-" * 35)
