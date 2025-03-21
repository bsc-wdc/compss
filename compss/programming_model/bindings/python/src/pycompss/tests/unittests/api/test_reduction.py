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

import os

from pycompss.util.context import CONTEXT
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.reduction import reduction
from pycompss.runtime.task.definitions.core_element import CE
from pycompss.util.exceptions import PyCOMPSsException

CHUNK_SIZE_ERROR = "chunk_size is not defined in kwargs dictionary."
CHUNK_SIZE_NOT_INIT_ERROR = "chunk_size parameter has not been initialized."
EXPECTED_EXCEPTION_ERROR = "ERROR: Expected Exception not raised."


def dummy_function(*args, **kwargs):  # noqa
    return 1


def test_reduction_instantiation():
    CONTEXT.set_master()
    my_reduction = reduction()
    assert (
        my_reduction.decorator_name == "@reduction"
    ), "The decorator name must be @reduction."
    CONTEXT.set_out_of_scope()


def test_reduction_call():
    CONTEXT.set_master()
    my_reduction = reduction()
    f = my_reduction(dummy_function)
    result = f()
    CONTEXT.set_out_of_scope()
    assert result == 1, "Wrong expected result (should be 1)."


def test_reduction_call_outside():
    CONTEXT.set_out_of_scope()
    my_reduction = reduction()
    f = my_reduction(dummy_function)
    _ = f()
    CONTEXT.set_out_of_scope()


def test_reduction_chunk_size_parameter():
    CONTEXT.set_master()
    chunk_size = 4
    my_reduction = reduction(chunk_size=chunk_size)
    f = my_reduction(dummy_function)
    _ = f()
    CONTEXT.set_out_of_scope()
    assert "chunk_size" in my_reduction.kwargs, CHUNK_SIZE_ERROR
    assert (
        chunk_size == my_reduction.kwargs["chunk_size"]
    ), CHUNK_SIZE_NOT_INIT_ERROR


def test_reduction_chunk_size_str_parameter():
    CONTEXT.set_master()
    chunk_size = "4"
    my_reduction = reduction(chunk_size=chunk_size)
    f = my_reduction(dummy_function)
    _ = f()
    CONTEXT.set_out_of_scope()
    assert "chunk_size" in my_reduction.kwargs, CHUNK_SIZE_ERROR
    assert (
        int(chunk_size) == my_reduction.kwargs["chunk_size"]
    ), CHUNK_SIZE_NOT_INIT_ERROR


def test_reduction_chunk_size_str_exception_parameter():
    CONTEXT.set_master()
    chunk_size = "abc"
    ok = False
    try:
        _ = reduction(chunk_size=chunk_size)
    except PyCOMPSsException:
        ok = True
    CONTEXT.set_out_of_scope()
    assert ok, EXPECTED_EXCEPTION_ERROR


def test_reduction_chunk_size_other_exception_parameter():
    CONTEXT.set_master()
    chunk_size = []
    ok = False
    try:
        _ = reduction(chunk_size=chunk_size)
    except PyCOMPSsException:
        ok = True
    CONTEXT.set_out_of_scope()
    assert ok, EXPECTED_EXCEPTION_ERROR


def test_reduction_chunk_size_str_env_var_parameter():
    CONTEXT.set_master()
    os.environ["MY_CHUNK_SIZE"] = "4"
    chunk_size = "$MY_CHUNK_SIZE"
    my_reduction = reduction(chunk_size=chunk_size)
    f = my_reduction(dummy_function)
    _ = f()
    CONTEXT.set_out_of_scope()
    assert "chunk_size" in my_reduction.kwargs, CHUNK_SIZE_ERROR
    assert (
        int(os.environ[chunk_size[1:]]) == my_reduction.kwargs["chunk_size"]
    ), CHUNK_SIZE_NOT_INIT_ERROR


def test_reduction_chunk_size_str_env_var_brackets_parameter():
    CONTEXT.set_master()
    os.environ["MY_CHUNK_SIZE"] = "4"
    chunk_size = "${MY_CHUNK_SIZE}"
    my_reduction = reduction(chunk_size=chunk_size)
    f = my_reduction(dummy_function)
    _ = f()
    CONTEXT.set_out_of_scope()
    assert "chunk_size" in my_reduction.kwargs, CHUNK_SIZE_ERROR
    assert (
        int(os.environ[chunk_size[2:-1]]) == my_reduction.kwargs["chunk_size"]
    ), CHUNK_SIZE_NOT_INIT_ERROR


def test_reduction_chunk_size_str_env_var_exception_parameter():
    CONTEXT.set_master()
    os.environ["MY_CHUNK_SIZE"] = "abc"
    chunk_size = "$MY_CHUNK_SIZE"
    ok = False
    try:
        _ = reduction(chunk_size=chunk_size)
    except PyCOMPSsException:
        ok = True
    CONTEXT.set_out_of_scope()
    assert ok, EXPECTED_EXCEPTION_ERROR


def test_reduction_is_reduce_parameter():
    CONTEXT.set_master()
    is_reduce = False
    my_reduction = reduction(is_reduce=is_reduce)
    f = my_reduction(dummy_function)
    _ = f()
    CONTEXT.set_out_of_scope()
    assert (
        "is_reduce" in my_reduction.kwargs
    ), "is_reduce is not defined in kwargs dictionary."
    assert (
        is_reduce == my_reduction.kwargs["is_reduce"]
    ), "is_reduce parameter has not been initialized."


def test_reduction_existing_core_element():
    CONTEXT.set_master()
    my_reduction = reduction()
    f = my_reduction(dummy_function)
    # a higher level decorator would place the compss core element as follows:
    _ = f(compss_core_element=CE())
    CONTEXT.set_out_of_scope()
    assert (
        CORE_ELEMENT_KEY not in my_reduction.kwargs
    ), "Core Element is not defined in kwargs dictionary."
