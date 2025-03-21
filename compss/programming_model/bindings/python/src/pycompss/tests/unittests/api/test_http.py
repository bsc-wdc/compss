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
from pycompss.api.http import http
from pycompss.runtime.task.definitions.core_element import CE
from pycompss.util.serialization import serializer


def dummy_function(*args, **kwargs):  # noqa
    return 1


def test_http_instantiation():
    CONTEXT.set_master()
    my_http = http(
        service_name="service", resource="resource", request="request"
    )
    serializer.FORCED_SERIALIZER = -1
    assert (
        my_http.decorator_name == "@http"
    ), "The decorator name must be @http"


def test_http_call():
    CONTEXT.set_master()
    my_http = http(
        service_name="service", resource="resource", request="request"
    )
    f = my_http(dummy_function)
    result = f()
    CONTEXT.set_out_of_scope()
    serializer.FORCED_SERIALIZER = -1
    assert result == 1, "Wrong expected result (should be 1)."


def test_http_call_outside():
    CONTEXT.set_out_of_scope()
    my_http = http(
        service_name="service", resource="resource", request="request"
    )
    f = my_http(dummy_function)
    _ = f()
    CONTEXT.set_out_of_scope()
    serializer.FORCED_SERIALIZER = -1


def test_http_existing_core_element():
    CONTEXT.set_master()
    my_http = http(
        service_name="service", resource="resource", request="request"
    )
    f = my_http(dummy_function)
    # a higher level decorator would place the compss core element as follows:
    _ = f(compss_core_element=CE())
    CONTEXT.set_out_of_scope()
    serializer.FORCED_SERIALIZER = -1
    assert (
        CORE_ELEMENT_KEY not in my_http.kwargs
    ), "Core Element is not defined in kwargs dictionary."
