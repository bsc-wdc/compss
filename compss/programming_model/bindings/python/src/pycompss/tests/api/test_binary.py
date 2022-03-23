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
from pycompss.api.binary import Binary
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.runtime.task.core_element import CE


def dummy_function(*args, **kwargs):  # noqa
    return 1


def test_binary_instantiation():
    context.set_pycompss_context(context.MASTER)
    my_bin = Binary(binary="date")
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert my_bin.decorator_name == "@binary", "The decorator name must be @binary."


def test_binary_call():
    context.set_pycompss_context(context.MASTER)
    my_bin = Binary(binary="date")
    f = my_bin(dummy_function)
    result = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert result == 1, "Wrong expected result (should be 1)."


# # Disabled due to support of dummy @binary
# def test_binary_call_outside():
#     context.set_pycompss_context(context.OUT_OF_SCOPE)
#     my_bin = Binary(binary="date")
#     f = my_bin(dummy_function)
#     thrown = False
#     try:
#         _ = f()
#     except Exception:  # noqa
#         thrown = True  # this is OK!
#     context.set_pycompss_context(context.OUT_OF_SCOPE)
#     assert thrown, \
#         "The binary decorator did not raise an exception when invoked out of scope."  # noqa: E501


def test_binary_engine_parameter():
    context.set_pycompss_context(context.MASTER)
    engine = "my_engine"
    my_bin = Binary(binary="date", engine=engine)
    f = my_bin(dummy_function)
    _ = f()
    assert "engine" in my_bin.kwargs, "Engine is not defined in kwargs dictionary."
    assert (
        engine == my_bin.kwargs["engine"]
    ), "Engine parameter has not been initialized."


def test_binary_image_parameter():
    context.set_pycompss_context(context.MASTER)
    image = "my_image"
    my_bin = Binary(binary="date", image=image)
    f = my_bin(dummy_function)
    _ = f()
    assert "image" in my_bin.kwargs, "Image is not defined in kwargs dictionary."
    assert image == my_bin.kwargs["image"], "Image parameter has not been initialized."


def test_binary_existing_core_element():
    context.set_pycompss_context(context.MASTER)
    my_bin = Binary(binary="date")
    f = my_bin(dummy_function)
    # a higher level decorator would place the compss core element as follows:
    _ = f(compss_core_element=CE())
    assert (
        CORE_ELEMENT_KEY not in my_bin.kwargs
    ), "Core Element is not defined in kwargs dictionary."
