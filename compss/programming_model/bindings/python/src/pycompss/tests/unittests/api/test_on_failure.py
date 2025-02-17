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
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.on_failure import on_failure
from pycompss.runtime.task.definitions.core_element import CE


def dummy_function(*args, **kwargs):  # noqa
    return 1


def test_on_failure_instantiation():
    CONTEXT.set_master()
    my_on_failure = on_failure(management="IGNORE")
    assert (
        my_on_failure.decorator_name == "@onfailure"
    ), "The decorator name must be @onfailure: "


def test_on_failure_call():
    CONTEXT.set_master()
    my_on_failure = on_failure(management="IGNORE")
    f = my_on_failure(dummy_function)
    result = f()
    CONTEXT.set_out_of_scope()
    assert result == 1, "Wrong expected result (should be 1)."


def test_on_failure_unsupported_call():
    CONTEXT.set_master()
    thrown = False
    try:
        _ = on_failure(management="UNDEFINED")
    except Exception:  # noqa
        thrown = True
    CONTEXT.set_out_of_scope()
    assert (
        thrown
    ), "The on_failure decorator did not raised an exception with unsupported management value."  # noqa: E501


def test_on_failure_call_outside():
    CONTEXT.set_out_of_scope()
    my_on_failure = on_failure(management="IGNORE")
    f = my_on_failure(dummy_function)
    _ = f()
    CONTEXT.set_out_of_scope()


def test_on_failure_existing_core_element():
    CONTEXT.set_master()
    my_on_failure = on_failure(management="IGNORE")
    f = my_on_failure(dummy_function)
    # a higher level decorator would place the compss core element as follows:
    _ = f(compss_core_element=CE())
    CONTEXT.set_out_of_scope()
    assert (
        CORE_ELEMENT_KEY not in my_on_failure.kwargs
    ), "Core Element is not defined in kwargs dictionary."
