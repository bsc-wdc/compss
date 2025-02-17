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
from pycompss.api.container import Container
from pycompss.api.binary import Binary
from pycompss.runtime.task.definitions.core_element import CE


def dummy_function(*args, **kwargs):  # noqa
    return 1


def test_container_instantiation():
    CONTEXT.set_master()
    my_bin = Container(engine="docker", image="dummy")
    CONTEXT.set_out_of_scope()
    assert (
        my_bin.decorator_name == "@container"
    ), "The decorator name must be @container."


def test_container_call():
    CONTEXT.set_master()
    my_bin = Container(engine="docker", image="dummy")
    f = my_bin(dummy_function)
    result = f()
    CONTEXT.set_out_of_scope()
    assert result == 1, "Wrong expected result (should be 1)."


def test_container_call_binary():
    CONTEXT.set_master()
    my_cont = Container(engine="docker", image="dummy")
    my_bin = Binary(binary="date")
    f = my_cont(my_bin(dummy_function))
    result = f()
    CONTEXT.set_out_of_scope()
    assert result == 1, "Wrong expected result (should be 1)."


def test_container_call_outside():
    CONTEXT.set_out_of_scope()
    my_bin = Container(engine="docker", image="dummy")
    f = my_bin(dummy_function)
    thrown = False
    try:
        _ = f()
    except Exception:  # noqa
        thrown = True  # this is OK!
    CONTEXT.set_out_of_scope()
    assert (
        thrown
    ), "The container decorator did not raise an exception when invoked out of scope."  # noqa: E501


def test_container_engine_image_parameters():
    CONTEXT.set_master()
    engine = "docker"
    image = "dummy"
    my_bin = Container(engine=engine, image=image)
    f = my_bin(dummy_function)
    _ = f()
    CONTEXT.set_out_of_scope()
    assert (
        "engine" in my_bin.kwargs
    ), "Engine is not defined in kwargs dictionary."
    assert (
        engine == my_bin.kwargs["engine"]
    ), "Engine parameter has not been initialized."
    assert (
        "image" in my_bin.kwargs
    ), "Image is not defined in kwargs dictionary."
    assert (
        image == my_bin.kwargs["image"]
    ), "image parameter has not been initialized."


def test_container_existing_core_element():
    CONTEXT.set_master()
    my_bin = Container(engine="docker", image="dummy")
    f = my_bin(dummy_function)
    # a higher level decorator would place the compss core element as follows:
    _ = f(compss_core_element=CE())
    CONTEXT.set_out_of_scope()
    assert (
        CORE_ELEMENT_KEY not in my_bin.kwargs
    ), "Core Element is not defined in kwargs dictionary."
