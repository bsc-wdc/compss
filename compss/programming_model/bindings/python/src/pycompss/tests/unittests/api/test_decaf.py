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
from pycompss.api.decaf import Decaf
from pycompss.runtime.task.definitions.core_element import CE


def dummy_function(*args, **kwargs):  # noqa
    return 1


def test_decaf_instantiation():
    CONTEXT.set_master()
    my_decaf = Decaf(df_script="date")
    CONTEXT.set_out_of_scope()
    assert (
        my_decaf.decorator_name == "@decaf"
    ), "The decorator name must be @decaf."


def test_decaf_call():
    CONTEXT.set_master()
    my_decaf = Decaf(df_script="date")
    f = my_decaf(dummy_function)
    result = f()
    CONTEXT.set_out_of_scope()
    assert result == 1, "Wrong expected result (should be 1)."


def test_decaf_call_outside():
    CONTEXT.set_out_of_scope()
    my_decaf = Decaf(df_script="date")
    f = my_decaf(dummy_function)
    thrown = False
    try:
        _ = f()
    except Exception:  # noqa
        thrown = True  # this is OK!
    CONTEXT.set_out_of_scope()
    assert thrown, (
        "The decaf decorator did not raise an exception when "
        "invoked out of scope."
    )


def test_decaf_runner_parameter():
    CONTEXT.set_master()
    runner = "my_runner"
    my_decaf = Decaf(df_script="date", runner=runner)
    f = my_decaf(dummy_function)
    _ = f()
    CONTEXT.set_out_of_scope()
    assert (
        "runner" in my_decaf.kwargs
    ), "Runner is not defined in kwargs dictionary."
    assert (
        runner == my_decaf.kwargs["runner"]
    ), "Runner parameter has not been initialized."


def test_decaf_dfscript_parameter():
    CONTEXT.set_master()
    df_script = "my_dfScript"  # noqa
    my_decaf = Decaf(df_script="date", dfScript=df_script)
    f = my_decaf(dummy_function)
    _ = f()
    CONTEXT.set_out_of_scope()
    assert (
        "dfScript" in my_decaf.kwargs
    ), "dfScript is not defined in kwargs dictionary."
    assert (
        df_script == my_decaf.kwargs["dfScript"]
    ), "dfScript parameter has not been initialized."


def test_decaf_df_executor_parameter():
    CONTEXT.set_master()
    df_executor = "my_df_executor"
    my_decaf = Decaf(df_script="date", df_executor=df_executor)
    f = my_decaf(dummy_function)
    _ = f()
    CONTEXT.set_out_of_scope()
    assert (
        "df_executor" in my_decaf.kwargs
    ), "df_executor is not defined in kwargs dictionary."
    assert (
        df_executor == my_decaf.kwargs["df_executor"]
    ), "df_executor parameter has not been initialized."


def test_decaf_dfexecutor_parameter():  # NOSONAR
    CONTEXT.set_master()
    df_executor = "my_dfExecutor"  # noqa
    my_decaf = Decaf(df_script="date", dfExecutor=df_executor)
    f = my_decaf(dummy_function)
    _ = f()
    CONTEXT.set_out_of_scope()
    assert (
        "dfExecutor" in my_decaf.kwargs
    ), "dfExecutor is not defined in kwargs dictionary."
    assert (
        df_executor == my_decaf.kwargs["dfExecutor"]
    ), "dfExecutor parameter has not been initialized."


def test_decaf_df_lib_parameter():
    CONTEXT.set_master()
    df_lib = "my_df_lib"
    my_decaf = Decaf(df_script="date", df_lib=df_lib)
    f = my_decaf(dummy_function)
    _ = f()
    CONTEXT.set_out_of_scope()
    assert (
        "df_lib" in my_decaf.kwargs
    ), "df_lib is not defined in kwargs dictionary."
    assert (
        df_lib == my_decaf.kwargs["df_lib"]
    ), "df_lib parameter has not been initialized."


def test_decaf_dflib_parameter():  # NOSONAR
    CONTEXT.set_master()
    df_lib = "my_dfLib"  # noqa
    my_decaf = Decaf(df_script="date", dfLib=df_lib)
    f = my_decaf(dummy_function)
    _ = f()
    CONTEXT.set_out_of_scope()
    assert (
        "dfLib" in my_decaf.kwargs
    ), "dfLib is not defined in kwargs dictionary."
    assert (
        df_lib == my_decaf.kwargs["dfLib"]
    ), "dfLib parameter has not been initialized."


def test_decaf_existing_core_element():
    CONTEXT.set_master()
    my_decaf = Decaf(df_script="date")
    f = my_decaf(dummy_function)
    # a higher level decorator would place the compss core element as follows:
    _ = f(compss_core_element=CE())
    CONTEXT.set_out_of_scope()
    assert (
        CORE_ELEMENT_KEY not in my_decaf.kwargs
    ), "Core Element is not defined in kwargs dictionary."
