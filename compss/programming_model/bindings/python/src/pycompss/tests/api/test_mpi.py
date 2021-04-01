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

from pycompss.api.mpi import MPI
from pycompss.runtime.task.core_element import CE
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
import pycompss.util.context as context

MPI_RUNNER = "mpirun"
ERROR_EXPECTED_1 = "Wrong expected result (should be 1)."


def dummy_function(*args, **kwargs):  # noqa
    return 1


def test_mpi_instantiation():
    context.set_pycompss_context(context.MASTER)
    my_mpi = MPI(runner=MPI_RUNNER)
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert my_mpi.decorator_name == "@mpi", "The decorator name must be @mpi."


def test_mpi_call():
    context.set_pycompss_context(context.MASTER)
    my_mpi = MPI(runner=MPI_RUNNER)
    f = my_mpi(dummy_function)
    result = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert result == 1, ERROR_EXPECTED_1


def test_mpi_call_outside():
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    my_mpi = MPI(runner=MPI_RUNNER, processes=2, binary="date")
    f = my_mpi(dummy_function)
    thrown = False
    try:
        _ = f()
    except Exception:  # noqa
        thrown = True  # this is OK!
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert thrown, \
        "The mpi decorator did not raise an exception when invoked out of scope."  # noqa: E501


def test_mpi_call_outside_with_computing_nodes_old_style():
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    my_mpi = MPI(runner=MPI_RUNNER, computingNodes=2, binary="date")
    f = my_mpi(dummy_function)
    thrown = False
    try:
        _ = f()
    except Exception:  # noqa
        thrown = True  # this is OK!
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert thrown, \
        "The mpi decorator did not raise an exception when invoked out of scope (computingNodes)."  # noqa: E501


def test_mpi_call_outside_with_computing_nodes():
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    my_mpi = MPI(runner=MPI_RUNNER, computing_nodes=2, binary="date")
    f = my_mpi(dummy_function)
    thrown = False
    try:
        _ = f()
    except Exception:  # noqa
        thrown = True  # this is OK!
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert thrown, \
        "The mpi decorator did not raise an exception when invoked out of scope (computing_nodes)."  # noqa: E501


def test_mpi_layout_empty_parameter():
    context.set_pycompss_context(context.MASTER)
    layout = dict()
    my_mpi = MPI(runner=MPI_RUNNER, _layout={"_layout": layout})
    f = my_mpi(dummy_function)
    _ = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        "_layout" in my_mpi.kwargs
    ), "_layout is not defined in kwargs dictionary."


def test_mpi_binary():
    context.set_pycompss_context(context.MASTER)
    my_mpi = MPI(runner=MPI_RUNNER, binary="date", flags="flags")
    f = my_mpi(dummy_function)
    result = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert result == 1, ERROR_EXPECTED_1


def test_mpi_binary_scale_bool_true():
    context.set_pycompss_context(context.MASTER)
    my_mpi = MPI(
        runner=MPI_RUNNER, binary="date", flags="flags", scale_by_cu=True
    )  # noqa: E501
    f = my_mpi(dummy_function)
    result = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert result == 1, ERROR_EXPECTED_1


def test_mpi_binary_scale_bool_false():
    context.set_pycompss_context(context.MASTER)
    my_mpi = MPI(
        runner=MPI_RUNNER, binary="date", flags="flags", scale_by_cu=False
    )  # noqa: E501
    f = my_mpi(dummy_function)
    result = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert result == 1, ERROR_EXPECTED_1


def test_mpi_binary_scale_str():
    context.set_pycompss_context(context.MASTER)
    my_mpi = MPI(
        runner=MPI_RUNNER, binary="date", flags="flags", scale_by_cu="ENV_VAR"
    )  # noqa: E501
    f = my_mpi(dummy_function)
    result = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert result == 1, ERROR_EXPECTED_1


def test_mpi_binary_scale_incorrect():
    context.set_pycompss_context(context.MASTER)
    my_mpi = MPI(runner=MPI_RUNNER, binary="date", flags="flags", scale_by_cu=1)
    f = my_mpi(dummy_function)
    exception = False
    try:
        _ = f()
    except Exception:  # noqa
        exception = True
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert exception, "Unsupported scale_by_cu value exception not raised."


def test_mpi_existing_core_element():
    context.set_pycompss_context(context.MASTER)
    my_mpi = MPI(runner=MPI_RUNNER)
    f = my_mpi(dummy_function)
    # a higher level decorator would place the compss core element as follows:
    _ = f(compss_core_element=CE())
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        CORE_ELEMENT_KEY not in my_mpi.kwargs
    ), "Core Element is not defined in kwargs dictionary."
