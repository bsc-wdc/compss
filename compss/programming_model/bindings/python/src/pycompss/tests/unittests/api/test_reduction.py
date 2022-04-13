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

import os

import pycompss.util.context as context
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.reduction import reduction
from pycompss.runtime.task.core_element import CE
from pycompss.util.exceptions import PyCOMPSsException

CHUNK_SIZE_ERROR = "chunk_size is not defined in kwargs dictionary."
CHUNK_SIZE_NOT_INIT_ERROR = "chunk_size parameter has not been initialized."
EXPECTED_EXCEPTION_ERROR = "ERROR: Expected Exception not raised."


def dummy_function(*args, **kwargs):  # noqa
    return 1


def test_reduction_instantiation():
    context.set_pycompss_context(context.MASTER)
    my_reduction = reduction()
    assert (
        my_reduction.decorator_name == "@reduction"
    ), "The decorator name must be @reduction."
    context.set_pycompss_context(context.OUT_OF_SCOPE)


def test_reduction_call():
    context.set_pycompss_context(context.MASTER)
    my_reduction = reduction()
    f = my_reduction(dummy_function)
    result = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert result == 1, "Wrong expected result (should be 1)."


def test_reduction_call_outside():
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    my_reduction = reduction()
    f = my_reduction(dummy_function)
    thrown = False
    try:
        _ = f()
    except Exception:  # noqa
        thrown = True  # this is OK!
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        thrown
    ), "The compss decorator did not raise an exception when invoked out of scope."  # noqa: E501


def test_reduction_chunk_size_parameter():
    context.set_pycompss_context(context.MASTER)
    chunk_size = 4
    my_reduction = reduction(chunk_size=chunk_size)
    f = my_reduction(dummy_function)
    _ = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert "chunk_size" in my_reduction.kwargs, CHUNK_SIZE_ERROR
    assert chunk_size == my_reduction.kwargs["chunk_size"], CHUNK_SIZE_NOT_INIT_ERROR


def test_reduction_chunk_size_str_parameter():
    context.set_pycompss_context(context.MASTER)
    chunk_size = "4"
    my_reduction = reduction(chunk_size=chunk_size)
    f = my_reduction(dummy_function)
    _ = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert "chunk_size" in my_reduction.kwargs, CHUNK_SIZE_ERROR
    assert (
        int(chunk_size) == my_reduction.kwargs["chunk_size"]
    ), CHUNK_SIZE_NOT_INIT_ERROR


def test_reduction_chunk_size_str_exception_parameter():
    context.set_pycompss_context(context.MASTER)
    chunk_size = "abc"
    ok = False
    try:
        _ = reduction(chunk_size=chunk_size)
    except PyCOMPSsException:
        ok = True
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert ok, EXPECTED_EXCEPTION_ERROR


def test_reduction_chunk_size_other_exception_parameter():
    context.set_pycompss_context(context.MASTER)
    chunk_size = []
    ok = False
    try:
        _ = reduction(chunk_size=chunk_size)
    except PyCOMPSsException:
        ok = True
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert ok, EXPECTED_EXCEPTION_ERROR


def test_reduction_chunk_size_str_env_var_parameter():
    context.set_pycompss_context(context.MASTER)
    os.environ["MY_CHUNK_SIZE"] = "4"
    chunk_size = "$MY_CHUNK_SIZE"
    my_reduction = reduction(chunk_size=chunk_size)
    f = my_reduction(dummy_function)
    _ = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert "chunk_size" in my_reduction.kwargs, CHUNK_SIZE_ERROR
    assert (
        int(os.environ[chunk_size[1:]]) == my_reduction.kwargs["chunk_size"]
    ), CHUNK_SIZE_NOT_INIT_ERROR


def test_reduction_chunk_size_str_env_var_brackets_parameter():
    context.set_pycompss_context(context.MASTER)
    os.environ["MY_CHUNK_SIZE"] = "4"
    chunk_size = "${MY_CHUNK_SIZE}"
    my_reduction = reduction(chunk_size=chunk_size)
    f = my_reduction(dummy_function)
    _ = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert "chunk_size" in my_reduction.kwargs, CHUNK_SIZE_ERROR
    assert (
        int(os.environ[chunk_size[2:-1]]) == my_reduction.kwargs["chunk_size"]
    ), CHUNK_SIZE_NOT_INIT_ERROR


def test_reduction_chunk_size_str_env_var_exception_parameter():
    context.set_pycompss_context(context.MASTER)
    os.environ["MY_CHUNK_SIZE"] = "abc"
    chunk_size = "$MY_CHUNK_SIZE"
    ok = False
    try:
        _ = reduction(chunk_size=chunk_size)
    except PyCOMPSsException:
        ok = True
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert ok, EXPECTED_EXCEPTION_ERROR


def test_reduction_is_reduce_parameter():
    context.set_pycompss_context(context.MASTER)
    is_reduce = False
    my_reduction = reduction(is_reduce=is_reduce)
    f = my_reduction(dummy_function)
    _ = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        "is_reduce" in my_reduction.kwargs
    ), "is_reduce is not defined in kwargs dictionary."
    assert (
        is_reduce == my_reduction.kwargs["is_reduce"]
    ), "is_reduce parameter has not been initialized."


def test_reduction_existing_core_element():
    context.set_pycompss_context(context.MASTER)
    my_reduction = reduction()
    f = my_reduction(dummy_function)
    # a higher level decorator would place the compss core element as follows:
    _ = f(compss_core_element=CE())
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        CORE_ELEMENT_KEY not in my_reduction.kwargs
    ), "Core Element is not defined in kwargs dictionary."
