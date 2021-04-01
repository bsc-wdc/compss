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

from pycompss.api.decaf import Decaf
from pycompss.runtime.task.core_element import CE
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
import pycompss.util.context as context


def dummy_function(*args, **kwargs):  # noqa
    return 1


def test_decaf_instantiation():
    context.set_pycompss_context(context.MASTER)
    my_decaf = Decaf(df_script="date")
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        my_decaf.decorator_name == "@decaf"
    ), "The decorator name must be @decaf."


def test_decaf_call():
    context.set_pycompss_context(context.MASTER)
    my_decaf = Decaf(df_script="date")
    f = my_decaf(dummy_function)
    result = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert result == 1, "Wrong expected result (should be 1)."


def test_decaf_call_outside():
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    my_decaf = Decaf(df_script="date")
    f = my_decaf(dummy_function)
    thrown = False
    try:
        _ = f()
    except Exception:  # noqa
        thrown = True  # this is OK!
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        thrown
    ), "The decaf decorator did not raise an exception when invoked out of scope."  # noqa: E501


def test_decaf_runner_parameter():
    context.set_pycompss_context(context.MASTER)
    runner = "my_runner"
    my_decaf = Decaf(df_script="date", runner=runner)
    f = my_decaf(dummy_function)
    _ = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        "runner" in my_decaf.kwargs
    ), "Runner is not defined in kwargs dictionary."
    assert (
        runner == my_decaf.kwargs["runner"]
    ), "Runner parameter has not been initialized."


def test_decaf_dfScript_parameter():  # NOSONAR
    context.set_pycompss_context(context.MASTER)
    df_script = "my_dfScript"  # noqa
    my_decaf = Decaf(df_script="date", dfScript=df_script)
    f = my_decaf(dummy_function)
    _ = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        "dfScript" in my_decaf.kwargs
    ), "dfScript is not defined in kwargs dictionary."
    assert (
        df_script == my_decaf.kwargs["dfScript"]
    ), "dfScript parameter has not been initialized."


def test_decaf_df_executor_parameter():
    context.set_pycompss_context(context.MASTER)
    df_executor = "my_df_executor"
    my_decaf = Decaf(df_script="date", df_executor=df_executor)
    f = my_decaf(dummy_function)
    _ = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        "df_executor" in my_decaf.kwargs
    ), "df_executor is not defined in kwargs dictionary."
    assert (
        df_executor == my_decaf.kwargs["df_executor"]
    ), "df_executor parameter has not been initialized."


def test_decaf_dfExecutor_parameter():  # NOSONAR
    context.set_pycompss_context(context.MASTER)
    df_executor = "my_dfExecutor"  # noqa
    my_decaf = Decaf(df_script="date", dfExecutor=df_executor)
    f = my_decaf(dummy_function)
    _ = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        "dfExecutor" in my_decaf.kwargs
    ), "dfExecutor is not defined in kwargs dictionary."
    assert (
        df_executor == my_decaf.kwargs["dfExecutor"]
    ), "dfExecutor parameter has not been initialized."


def test_decaf_df_lib_parameter():
    context.set_pycompss_context(context.MASTER)
    df_lib = "my_df_lib"
    my_decaf = Decaf(df_script="date", df_lib=df_lib)
    f = my_decaf(dummy_function)
    _ = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        "df_lib" in my_decaf.kwargs
    ), "df_lib is not defined in kwargs dictionary."
    assert (
        df_lib == my_decaf.kwargs["df_lib"]
    ), "df_lib parameter has not been initialized."


def test_decaf_dfLib_parameter():  # NOSONAR
    context.set_pycompss_context(context.MASTER)
    df_lib = "my_dfLib"  # noqa
    my_decaf = Decaf(df_script="date", dfLib=df_lib)
    f = my_decaf(dummy_function)
    _ = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        "dfLib" in my_decaf.kwargs
    ), "dfLib is not defined in kwargs dictionary."
    assert (
        df_lib == my_decaf.kwargs["dfLib"]
    ), "dfLib parameter has not been initialized."


def test_decaf_existing_core_element():
    context.set_pycompss_context(context.MASTER)
    my_decaf = Decaf(df_script="date")
    f = my_decaf(dummy_function)
    # a higher level decorator would place the compss core element as follows:
    _ = f(compss_core_element=CE())
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        CORE_ELEMENT_KEY not in my_decaf.kwargs
    ), "Core Element is not defined in kwargs dictionary."
