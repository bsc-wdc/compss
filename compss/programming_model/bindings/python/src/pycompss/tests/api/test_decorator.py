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

from pycompss.api.commons.decorator import PyCOMPSsDecorator
from pycompss.util.exceptions import MissingImplementedException
import pycompss.util.context as context


def test_decorator_instantiation():
    context.set_pycompss_context(context.MASTER)
    my_deco = PyCOMPSsDecorator(decorator_name="@decorator")
    assert my_deco.decorator_name == "@decorator", \
        "The decorator name must be @decorator."


def test_decorator_core_element_exception():
    context.set_pycompss_context(context.MASTER)
    my_deco = PyCOMPSsDecorator(decorator_name="@decorator")
    raised = False
    try:
        my_deco.__configure_core_element__(dict())
    except MissingImplementedException:  # noqa
        raised = True  # expected exception caught
    assert raised, "Expected MissingImplementedException but not raised."


def test_decorator_resolve_working_dir():
    context.set_pycompss_context(context.MASTER)
    working_dir = "/tmp"
    my_deco = PyCOMPSsDecorator(decorator_name="@decorator",
                                working_dir=working_dir)
    my_deco.__resolve_working_dir__()
    assert my_deco.kwargs['working_dir'] == working_dir, \
        "Wrong working directory."


def test_decorator_resolve_workingDir():  # noqa
    context.set_pycompss_context(context.MASTER)
    working_dir = "/tmp"
    my_deco = PyCOMPSsDecorator(decorator_name="@decorator",
                                workingDir=working_dir)
    my_deco.__resolve_working_dir__()
    assert my_deco.kwargs['working_dir'] == working_dir, \
        "Wrong working directory."


def test_decorator_resolve_fail_by_exit_value_bool_true():
    context.set_pycompss_context(context.MASTER)
    fail_by_exit_value = True
    my_deco = PyCOMPSsDecorator(decorator_name="@decorator",
                                fail_by_exit_value=fail_by_exit_value)
    my_deco.__resolve_fail_by_exit_value__()
    assert my_deco.kwargs['fail_by_exit_value'] == str(fail_by_exit_value), \
        "Wrong fail_by_exit_value."


def test_decorator_resolve_fail_by_exit_value_bool_false():
    context.set_pycompss_context(context.MASTER)
    fail_by_exit_value = False
    my_deco = PyCOMPSsDecorator(decorator_name="@decorator",
                                fail_by_exit_value=fail_by_exit_value)
    my_deco.__resolve_fail_by_exit_value__()
    assert my_deco.kwargs['fail_by_exit_value'] == str(fail_by_exit_value), \
        "Wrong fail_by_exit_value."


def test_decorator_resolve_fail_by_exit_value_str():
    context.set_pycompss_context(context.MASTER)
    fail_by_exit_value = "true"
    my_deco = PyCOMPSsDecorator(decorator_name="@decorator",
                                fail_by_exit_value=fail_by_exit_value)
    my_deco.__resolve_fail_by_exit_value__()
    assert my_deco.kwargs['fail_by_exit_value'] == fail_by_exit_value, \
        "Wrong fail_by_exit_value."


def test_decorator_resolve_fail_by_exit_value_exception():
    context.set_pycompss_context(context.MASTER)
    fail_by_exit_value = [1, 2]  # any object
    my_deco = PyCOMPSsDecorator(decorator_name="@decorator",
                                fail_by_exit_value=fail_by_exit_value)
    raised = False
    try:
        my_deco.__resolve_fail_by_exit_value__()
    except Exception:  # noqa
        raised = True
    assert raised, \
        "Expected exception with wrong fail_by_exit_value not raised."


def test_decorator_process_computingNodes():  # noqa
    context.set_pycompss_context(context.MASTER)
    decorator_name = "@decorator"
    computing_nodes = 1
    my_deco = PyCOMPSsDecorator(decorator_name=decorator_name,
                                computingNodes=computing_nodes)
    my_deco.__process_computing_nodes__(decorator_name=decorator_name)
    assert my_deco.kwargs['computing_nodes'] == computing_nodes, \
        "Wrong computing_nodes value."


def test_decorator_process_computing_nodes():
    context.set_pycompss_context(context.MASTER)
    decorator_name = "@decorator"
    computing_nodes = 1
    my_deco = PyCOMPSsDecorator(decorator_name=decorator_name,
                                computingNodes=computing_nodes)
    my_deco.__process_computing_nodes__(decorator_name=decorator_name)
    assert my_deco.kwargs['computing_nodes'] == computing_nodes, \
        "Wrong computing_nodes value."
