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

from pycompss.api.compss import COMPSs
from pycompss.runtime.task.core_element import CE
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
import pycompss.util.context as context


def dummy_function(*args, **kwargs):  # noqa
    return 1


def test_compss_instantiation():
    context.set_pycompss_context(context.MASTER)
    my_compss = COMPSs(app_name="date")
    assert (
        my_compss.decorator_name == "@compss"
    ), "The decorator name must be @compss."


def test_compss_call():
    context.set_pycompss_context(context.MASTER)
    my_compss = COMPSs(app_name="date")
    f = my_compss(dummy_function)
    result = f()
    assert result == 1, "Wrong expected result (should be 1)."


def test_compss_call_outside():
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    my_compss = COMPSs(app_name="date")
    f = my_compss(dummy_function)
    thrown = False
    try:
        _ = f()
    except Exception:  # noqa
        thrown = True  # this is OK!
    assert (
        thrown
    ), "The compss decorator did not raise an exception when invoked out of scope."  # noqa: E501


def test_compss_appName_parameter():  # noqa
    context.set_pycompss_context(context.MASTER)
    appName = "my_appName"  # noqa
    my_compss = COMPSs(app_name="date", appName=appName)
    f = my_compss(dummy_function)
    _ = f()
    assert (
        "appName" in my_compss.kwargs
    ), "Image is not defined in kwargs dictionary."
    assert (
        appName == my_compss.kwargs["appName"]
    ), "Image parameter has not been initialized."


def test_compss_runcompss_parameter():
    context.set_pycompss_context(context.MASTER)
    runcompss = "my_runcompss"
    my_compss = COMPSs(app_name="date", runcompss=runcompss)
    f = my_compss(dummy_function)
    _ = f()
    assert (
        "runcompss" in my_compss.kwargs
    ), "Runcompss is not defined in kwargs dictionary."
    assert (
        runcompss == my_compss.kwargs["runcompss"]
    ), "Runcompss parameter has not been initialized."


def test_compss_flags_parameter():
    context.set_pycompss_context(context.MASTER)
    flags = "my_flags"
    my_compss = COMPSs(app_name="date", flags=flags)
    f = my_compss(dummy_function)
    _ = f()
    assert (
        "flags" in my_compss.kwargs
    ), "flags is not defined in kwargs dictionary."
    assert (
        flags == my_compss.kwargs["flags"]
    ), "flags parameter has not been initialized."


def test_compss_worker_in_master_parameter():
    context.set_pycompss_context(context.MASTER)
    worker_in_master = "my_worker_in_master"
    my_compss = COMPSs(app_name="date", worker_in_master=worker_in_master)
    f = my_compss(dummy_function)
    _ = f()
    assert (
        "worker_in_master" in my_compss.kwargs
    ), "worker_in_master is not defined in kwargs dictionary."
    assert (
        worker_in_master == my_compss.kwargs["worker_in_master"]
    ), "worker_in_master parameter has not been initialized."


def test_compss_workerInMaster_parameter():  # noqa
    context.set_pycompss_context(context.MASTER)
    workerInMaster = "my_workerInMaster"  # noqa
    my_compss = COMPSs(app_name="date", workerInMaster=workerInMaster)
    f = my_compss(dummy_function)
    _ = f()
    assert (
        "workerInMaster" in my_compss.kwargs
    ), "workerInMaster is not defined in kwargs dictionary."
    assert (
        workerInMaster == my_compss.kwargs["workerInMaster"]
    ), "workerInMaster parameter has not been initialized."


def test_compss_existing_core_element():
    context.set_pycompss_context(context.MASTER)
    my_compss = COMPSs(app_name="date")
    f = my_compss(dummy_function)
    # a higher level decorator would place the compss core element as follows:
    _ = f(compss_core_element=CE())
    assert (
        CORE_ELEMENT_KEY not in my_compss.kwargs
    ), "Core Element is not defined in kwargs dictionary."
