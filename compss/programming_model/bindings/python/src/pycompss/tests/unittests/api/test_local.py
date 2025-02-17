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

from pycompss.util.context import CONTEXT

using_mypy = False
try:
    from pycompss.api.local import local
except ImportError:

    def local(func):
        return func

    using_mypy = True


def test_local_instantiation():
    if using_mypy:
        raise Exception("UNSUPPORTED WITH MYPY")
    CONTEXT.set_master()

    @local
    def dummy_function(*args, **kwargs):  # noqa
        return sum(args)

    result = dummy_function(1, 2, other=3)
    CONTEXT.set_out_of_scope()
    assert result == 3, "Wrong expected result (should be 3)."


def test_local_instantiation_outside():
    if using_mypy:
        raise Exception("UNSUPPORTED WITH MYPY")
    CONTEXT.set_out_of_scope()

    @local
    def dummy_function(*args, **kwargs):  # noqa
        return sum(args)

    result = dummy_function(1, 2)
    CONTEXT.set_out_of_scope()
    assert result == 3, "Wrong expected result (should be 3)."
