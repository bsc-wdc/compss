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
from pycompss.api.constraint import Constraint
from pycompss.runtime.task.definitions.core_element import CE


def dummy_function(*args, **kwargs):  # noqa
    return 1


def test_constraint_instantiation():
    CONTEXT.set_master()
    my_constraint = Constraint()
    CONTEXT.set_out_of_scope()
    assert (
        my_constraint.decorator_name == "@constraint"
    ), "The decorator name must be @constraint."


def test_constraint_call():
    CONTEXT.set_master()
    my_constraint = Constraint()
    f = my_constraint(dummy_function)
    result = f()
    CONTEXT.set_out_of_scope()
    assert result == 1, "Wrong expected result (should be 1)."


def test_constraint_call_outside():
    CONTEXT.set_out_of_scope()
    my_constraint = Constraint()
    f = my_constraint(dummy_function)
    _ = f()
    CONTEXT.set_out_of_scope()
    # Does not raise exception (dummy instead)


def test_constraint_existing_core_element():
    CONTEXT.set_master()
    my_constraint = Constraint()
    f = my_constraint(dummy_function)
    # a higher level decorator would place the compss core element as follows:
    _ = f(compss_core_element=CE())
    CONTEXT.set_out_of_scope()
    assert (
        CORE_ELEMENT_KEY not in my_constraint.kwargs
    ), "Core Element is not defined in kwargs dictionary."
