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

from pycompss.api.container import Container
from pycompss.runtime.task.core_element import CE
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
import pycompss.util.context as context


def dummy_function(*args, **kwargs):  # noqa
    return 1


def test_container_instantiation():
    context.set_pycompss_context(context.MASTER)
    my_bin = Container(engine="docker", image="dummy")
    assert my_bin.decorator_name == "@container", \
        "The decorator name must be @container."


def test_container_call():
    context.set_pycompss_context(context.MASTER)
    my_bin = Container(engine="docker", image="dummy")
    f = my_bin(dummy_function)
    result = f()
    assert result == 1, \
        "Wrong expected result (should be 1)."


def test_container_call_outside():
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    my_bin = Container(engine="docker", image="dummy")
    f = my_bin(dummy_function)
    thrown = False
    try:
        _ = f()
    except Exception:  # noqa
        thrown = True  # this is OK!
    assert thrown, \
        "The container decorator did not raise an exception when invoked out of scope."  # noqa: E501


def test_container_engine_image_parameters():
    context.set_pycompss_context(context.MASTER)
    engine = "docker"
    image = "dummy"
    my_bin = Container(engine=engine, image=image)
    f = my_bin(dummy_function)
    _ = f()
    assert "engine" in my_bin.kwargs, \
        "Engine is not defined in kwargs dictionary."
    assert engine == my_bin.kwargs["engine"], \
        "Engine parameter has not been initialized."
    assert "image" in my_bin.kwargs, \
        "Image is not defined in kwargs dictionary."
    assert image == my_bin.kwargs["image"], \
        "image parameter has not been initialized."


def test_container_existing_core_element():
    context.set_pycompss_context(context.MASTER)
    my_bin = Container(engine="docker", image="dummy")
    f = my_bin(dummy_function)
    # a higher level decorator would place the compss core element as follows:
    _ = f(compss_core_element=CE())
    assert CORE_ELEMENT_KEY not in my_bin.kwargs, \
        "Core Element is not defined in kwargs dictionary."
