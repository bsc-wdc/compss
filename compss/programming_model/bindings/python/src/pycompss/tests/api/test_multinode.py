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

import os
from pycompss.api.multinode import MultiNode
from pycompss.runtime.task.core_element import CE
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
import pycompss.util.context as context


ERROR_EXPECTED_1 = "Wrong expected result (should be 1)."


def dummy_function(*args, **kwargs):  # noqa
    return 1


def test_multinode_instantiation():
    context.set_pycompss_context(context.MASTER)
    my_multinode = MultiNode()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        my_multinode.decorator_name == "@multinode"
    ), "The decorator name must be @multinode."


def test_multinode_call_outside():
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    my_multinode = MultiNode()
    f = my_multinode(dummy_function)
    thrown = False
    try:
        _ = f()
    except Exception:  # noqa
        thrown = True  # this is OK!
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        thrown
    ), "The multinode decorator did not raise an exception when invoked out of scope."  # noqa: E501


def test_multinode_call_master():
    context.set_pycompss_context(context.MASTER)
    my_multinode = MultiNode()
    f = my_multinode(dummy_function)
    result = f()
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert result == 1, ERROR_EXPECTED_1


def test_multinode_call_worker():
    context.set_pycompss_context(context.WORKER)
    # prepare test setup
    os.environ["COMPSS_NUM_NODES"] = "2"
    os.environ["COMPSS_NUM_THREADS"] = "2"
    os.environ["COMPSS_HOSTNAMES"] = "hostnames"
    # call
    my_multinode = MultiNode()
    f = my_multinode(dummy_function)
    result = f()
    # clean test setup
    del os.environ["COMPSS_NUM_NODES"]
    del os.environ["COMPSS_NUM_THREADS"]
    del os.environ["COMPSS_HOSTNAMES"]
    # Check result
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert result == 1, ERROR_EXPECTED_1


def test_multinode_call_worker_with_slurm():
    context.set_pycompss_context(context.WORKER)
    # prepare test setup
    os.environ["COMPSS_NUM_NODES"] = "2"
    os.environ["COMPSS_NUM_THREADS"] = "2"
    os.environ["COMPSS_HOSTNAMES"] = "hostname1,hostname2"
    os.environ["SLURM_NTASKS"] = "2"
    os.environ["SLURM_NNODES"] = "2"
    os.environ["SLURM_NODELIST"] = "hostname1,hostname2"
    os.environ["SLURM_TASKS_PER_NODE"] = "2"
    os.environ["SLURM_MEM_PER_NODE"] = "2"
    os.environ["SLURM_MEM_PER_CPU"] = "2"
    # call
    my_multinode = MultiNode()
    f = my_multinode(dummy_function)
    result = f()
    # clean test setup
    del os.environ["COMPSS_NUM_NODES"]
    del os.environ["COMPSS_NUM_THREADS"]
    del os.environ["COMPSS_HOSTNAMES"]
    del os.environ["SLURM_NTASKS"]
    del os.environ["SLURM_NNODES"]
    del os.environ["SLURM_NODELIST"]
    del os.environ["SLURM_TASKS_PER_NODE"]
    del os.environ["SLURM_MEM_PER_NODE"]
    del os.environ["SLURM_MEM_PER_CPU"]
    # Check result
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert result == 1, ERROR_EXPECTED_1


def test_multinode_existing_core_element():
    context.set_pycompss_context(context.MASTER)
    my_multinode = MultiNode()
    f = my_multinode(dummy_function)
    # a higher level decorator would place the compss core element as follows:
    _ = f(compss_core_element=CE())
    context.set_pycompss_context(context.OUT_OF_SCOPE)
    assert (
        CORE_ELEMENT_KEY not in my_multinode.kwargs
    ), "Core Element is not defined in kwargs dictionary."
