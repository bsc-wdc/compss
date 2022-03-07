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

import shutil
import tempfile

from pycompss.api.commons.decorator import PyCOMPSsDecorator
from pycompss.api.commons.decorator import resolve_working_dir
from pycompss.api.commons.decorator import resolve_fail_by_exit_value
from pycompss.api.commons.decorator import process_computing_nodes
from pycompss.api.commons.decorator import run_command
from pycompss.api.commons.constants import WORKING_DIR
from pycompss.api.commons.constants import LEGACY_WORKING_DIR
from pycompss.util.exceptions import MissingImplementedException
import pycompss.util.context as context

DECORATOR_NAME = "@decorator"


def test_decorator_instantiation():
    context.set_pycompss_context(context.MASTER)
    my_deco = PyCOMPSsDecorator(decorator_name=DECORATOR_NAME)
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        my_deco.decorator_name == DECORATOR_NAME
    ), "The decorator name must be @decorator."


def test_decorator_core_element_exception():
    context.set_pycompss_context(context.MASTER)
    my_deco = PyCOMPSsDecorator(decorator_name=DECORATOR_NAME)
    raised = False
    try:
        my_deco.__configure_core_element__(dict(), None)
    except MissingImplementedException:  # noqa
        raised = True  # expected exception caught
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert raised, "Expected MissingImplementedException but not raised."


def test_decorator_resolve_working_dir():
    context.set_pycompss_context(context.MASTER)
    working_dir = tempfile.mkdtemp()
    kwargs = {"decorator_name": DECORATOR_NAME, "working_dir": working_dir}
    resolve_working_dir(kwargs)
    my_deco = PyCOMPSsDecorator(**kwargs)
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert my_deco.kwargs["working_dir"] == working_dir, "Wrong working directory."
    shutil.rmtree(working_dir)


def test_decorator_resolve_workingDir():  # noqa
    context.set_pycompss_context(context.MASTER)
    working_dir = tempfile.mkdtemp()
    kwargs = {"decorator_name": DECORATOR_NAME, "workingDir": working_dir}
    resolve_working_dir(kwargs)
    my_deco = PyCOMPSsDecorator(**kwargs)
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert my_deco.kwargs["working_dir"] == working_dir, "Wrong working directory."
    shutil.rmtree(working_dir)


def test_decorator_resolve_fail_by_exit_value_bool_true():
    fail_by_exit_value = True
    __evaluate_fail_by_exit_value__(fail_by_exit_value)


def test_decorator_resolve_fail_by_exit_value_bool_false():
    fail_by_exit_value = False
    __evaluate_fail_by_exit_value__(fail_by_exit_value)


def test_decorator_resolve_fail_by_exit_value_str():
    fail_by_exit_value = "true"
    __evaluate_fail_by_exit_value__(fail_by_exit_value)


def test_decorator_resolve_fail_by_exit_value_int():
    fail_by_exit_value = 123
    __evaluate_fail_by_exit_value__(fail_by_exit_value)


def __evaluate_fail_by_exit_value__(fail_by_exit_value):
    context.set_pycompss_context(context.MASTER)
    kwargs = {
        "decorator_name": DECORATOR_NAME,
        "fail_by_exit_value": fail_by_exit_value,
    }
    resolve_fail_by_exit_value(kwargs)
    my_deco = PyCOMPSsDecorator(**kwargs)
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert my_deco.kwargs["fail_by_exit_value"] == str(
        fail_by_exit_value
    ), "Wrong fail_by_exit_value value (%s != %s)" % (
        my_deco.kwargs["fail_by_exit_value"],
        str(fail_by_exit_value),
    )


def test_decorator_resolve_fail_by_exit_value_exception():
    context.set_pycompss_context(context.MASTER)
    fail_by_exit_value = [1, 2]  # any object
    kwargs = {
        "decorator_name": DECORATOR_NAME,
        "fail_by_exit_value": fail_by_exit_value,
    }
    raised = False
    try:
        resolve_fail_by_exit_value(kwargs)
    except Exception:  # noqa
        raised = True
    _ = PyCOMPSsDecorator(**kwargs)
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert raised, "Expected exception with wrong fail_by_exit_value not raised."


def test_decorator_process_computingNodes():  # noqa
    context.set_pycompss_context(context.MASTER)
    decorator_name = DECORATOR_NAME
    computing_nodes = 1
    kwargs = {"decorator_name": decorator_name, "computingNodes": computing_nodes}
    process_computing_nodes(decorator_name, kwargs)
    my_deco = PyCOMPSsDecorator(**kwargs)
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert my_deco.kwargs["computing_nodes"] == str(
        computing_nodes
    ), "Wrong computing_nodes value."


def test_decorator_process_computing_nodes():
    context.set_pycompss_context(context.MASTER)
    decorator_name = DECORATOR_NAME
    computing_nodes = 1
    kwargs = {"decorator_name": decorator_name, "computing_odes": computing_nodes}
    process_computing_nodes(decorator_name, kwargs)
    my_deco = PyCOMPSsDecorator(**kwargs)
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert my_deco.kwargs["computing_nodes"] == str(
        computing_nodes
    ), "Wrong computing_nodes value."


def test_run_command():
    cmd = ["date"]
    args = ("+%s",)
    kwargs = {
        WORKING_DIR: "/path/to/working_dir",
        LEGACY_WORKING_DIR: "/path/to/legacy_working_dir",
    }
    result = run_command(cmd, args, kwargs)
    assert result == 0, "Wrong exit code on run_command. Expected %d - Received %d" % (
        0,
        result,
    )
