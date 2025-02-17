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
from pycompss.api.mpi import Mpi
from pycompss.runtime.task.definitions.core_element import CE

MPI_RUNNER = "mpirun"
ERROR_EXPECTED_1 = "Wrong expected result (should be 1)."


def dummy_function(*args, **kwargs):  # noqa
    return 1


def test_mpi_instantiation():
    CONTEXT.set_master()
    my_mpi = Mpi(runner=MPI_RUNNER)
    CONTEXT.set_out_of_scope()
    assert my_mpi.decorator_name == "@mpi", "The decorator name must be @mpi."


def test_mpi_call():
    CONTEXT.set_master()
    my_mpi = Mpi(runner=MPI_RUNNER)
    f = my_mpi(dummy_function)
    result = f()
    CONTEXT.set_out_of_scope()
    assert result == 1, ERROR_EXPECTED_1


def test_mpi_call_outside():
    CONTEXT.set_out_of_scope()
    my_mpi = Mpi(runner=MPI_RUNNER, processes=2, binary="date")
    f = my_mpi(dummy_function)
    thrown = False
    try:
        _ = f()
    except Exception:  # noqa
        thrown = True  # this is OK!
    CONTEXT.set_out_of_scope()
    assert (
        thrown
    ), "The mpi decorator did not raise an exception when invoked out of scope."  # noqa: E501


def test_mpi_call_outside_with_computing_nodes_old_style():
    CONTEXT.set_out_of_scope()
    my_mpi = Mpi(runner=MPI_RUNNER, computingNodes=2, binary="date")
    f = my_mpi(dummy_function)
    thrown = False
    try:
        _ = f()
    except Exception:  # noqa
        thrown = True  # this is OK!
    CONTEXT.set_out_of_scope()
    assert (
        thrown
    ), "The mpi decorator did not raise an exception when invoked out of scope (computingNodes)."  # noqa: E501


def test_mpi_call_outside_with_computing_nodes():
    CONTEXT.set_out_of_scope()
    my_mpi = Mpi(runner=MPI_RUNNER, computing_nodes=2, binary="date")
    f = my_mpi(dummy_function)
    thrown = False
    try:
        _ = f()
    except Exception:  # noqa
        thrown = True  # this is OK!
    CONTEXT.set_out_of_scope()
    assert (
        thrown
    ), "The mpi decorator did not raise an exception when invoked out of scope (computing_nodes)."  # noqa: E501


def test_mpi_layout_empty_parameter():
    CONTEXT.set_master()
    layout = dict()
    my_mpi = Mpi(runner=MPI_RUNNER, _layout={"_layout": layout})
    f = my_mpi(dummy_function)
    _ = f()
    CONTEXT.set_out_of_scope()
    assert (
        "_layout" in my_mpi.kwargs
    ), "_layout is not defined in kwargs dictionary."


def test_mpi_binary():
    CONTEXT.set_master()
    my_mpi = Mpi(runner=MPI_RUNNER, binary="date", flags="flags")
    f = my_mpi(dummy_function)
    result = f()
    CONTEXT.set_out_of_scope()
    assert result == 1, ERROR_EXPECTED_1


def test_mpi_binary_scale_bool_true():
    CONTEXT.set_master()
    my_mpi = Mpi(
        runner=MPI_RUNNER, binary="date", flags="flags", scale_by_cu=True
    )
    f = my_mpi(dummy_function)
    result = f()
    CONTEXT.set_out_of_scope()
    assert result == 1, ERROR_EXPECTED_1


def test_mpi_binary_scale_bool_false():
    CONTEXT.set_master()
    my_mpi = Mpi(
        runner=MPI_RUNNER, binary="date", flags="flags", scale_by_cu=False
    )
    f = my_mpi(dummy_function)
    result = f()
    CONTEXT.set_out_of_scope()
    assert result == 1, ERROR_EXPECTED_1


def test_mpi_binary_scale_str():
    CONTEXT.set_master()
    my_mpi = Mpi(
        runner=MPI_RUNNER, binary="date", flags="flags", scale_by_cu="true"
    )
    f = my_mpi(dummy_function)
    result = f()
    CONTEXT.set_out_of_scope()
    assert result == 1, ERROR_EXPECTED_1


def test_mpi_binary_scale_incorrect():
    CONTEXT.set_master()
    my_mpi = Mpi(
        runner=MPI_RUNNER, binary="date", flags="flags", scale_by_cu=1
    )
    f = my_mpi(dummy_function)
    exception = False
    try:
        _ = f()
    except Exception:  # noqa
        exception = True
    CONTEXT.set_out_of_scope()
    assert exception, "Unsupported scale_by_cu value exception not raised."


def test_mpi_existing_core_element():
    CONTEXT.set_master()
    my_mpi = Mpi(runner=MPI_RUNNER)
    f = my_mpi(dummy_function)
    # a higher level decorator would place the compss core element as follows:
    _ = f(compss_core_element=CE())
    CONTEXT.set_out_of_scope()
    assert (
        CORE_ELEMENT_KEY not in my_mpi.kwargs
    ), "Core Element is not defined in kwargs dictionary."
