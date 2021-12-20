#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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

import pycompss.util.context as context
using_mypy = False
try:
    from pycompss.api.local import local
except ImportError:
    def local(func):
        return func
    using_mypy = True


@local
def dummy_function(*args, **kwargs):  # noqa
    return sum(args)


def test_local_instantiation():
    if using_mypy:
        raise Exception("UNSUPPORTED WITH MYPY")
    context.set_pycompss_context(context.MASTER)
    result = dummy_function(1, 2)
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert result == 3, "Wrong expected result (should be 3)."
