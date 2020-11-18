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

import os
import tempfile
from pycompss.util.exceptions import PyCOMPSsException


def test_exaqute_example():
    from exaqute.ExaquteExamples import main
    main()


def test_exaqute_constraints():
    from exaqute.ExaquteExecutionConstraints import ExaquteExecutionConstraints
    constraint = ExaquteExecutionConstraints()
    assert constraint.nodes == 1, "ERROR: wrong number of default nodes"
    assert constraint.cpus_per_task == 1, "ERROR: wrong number of default cpus_per_task"  # noqa: E501
    assert constraint.amount_executions == 1, "ERROR: wrong number of amount_executions"  # noqa: E501


def test_exaqute_parameter():
    from exaqute.ExaquteParameter import IN     # noqa
    from exaqute.ExaquteParameter import INOUT  # noqa

    from exaqute.ExaquteParameter import FILE_IN     # noqa
    from exaqute.ExaquteParameter import FILE_OUT    # noqa
    from exaqute.ExaquteParameter import FILE_INOUT  # noqa

    from exaqute.ExaquteParameter import COLLECTION_IN     # noqa
    from exaqute.ExaquteParameter import COLLECTION_INOUT  # noqa
    from exaqute.ExaquteParameter import COLLECTION_OUT    # noqa

    from exaqute.ExaquteParameter import Type         # noqa
    from exaqute.ExaquteParameter import Direction    # noqa
    from exaqute.ExaquteParameter import StdIOStream  # noqa
    from exaqute.ExaquteParameter import Prefix       # noqa
    from exaqute.ExaquteParameter import Depth        # noqa


def test_exaqute_task():
    from exaqute.ExaquteTask import ExaquteTask
    try:
        _ = ExaquteTask()
    except Exception:  # NOSONAR
        # This is OK
        pass
    else:
        raise PyCOMPSsException("ERROR: Expected not implemented exception.")

    from exaqute.ExaquteTask import get_value_from_remote
    try:
        get_value_from_remote(None)
    except Exception:  # NOSONAR
        # This is OK
        pass
    else:
        raise PyCOMPSsException("ERROR: Expected get value not implemented exception.")  # noqa: E501

    from exaqute.ExaquteTask import barrier
    try:
        barrier()
    except Exception:  # NOSONAR
        # This is OK
        pass
    else:
        raise PyCOMPSsException("ERROR: Expected barrier not implemented exception.")  # noqa: E501

    from exaqute.ExaquteTask import delete_object
    try:
        delete_object(None)
    except Exception:  # NOSONAR
        # This is OK
        pass
    else:
        raise PyCOMPSsException("ERROR: Expected delete object not implemented exception.")  # noqa: E501

    from exaqute.ExaquteTask import compute
    try:
        compute(None)
    except Exception:  # NOSONAR
        # This is OK
        pass
    else:
        raise PyCOMPSsException("ERROR: Expected compute not implemented exception.")  # noqa: E501


def test_exaqute_task_local():
    from exaqute.ExaquteTaskLocal import ExaquteTask

    @ExaquteTask()
    def increment(a):
        return a + 1
    result = increment(2)
    print(result)
    assert result == 3, "ERROR: The local decorator is not transparent."

    from exaqute.ExaquteTaskLocal import get_value_from_remote
    obj = [1, 2, 3]
    result = get_value_from_remote(obj)
    assert obj == result, "ERROR: get_value_from_remote does not retrieve the same object."  # noqa: E501

    from exaqute.ExaquteTaskLocal import barrier
    barrier()

    from exaqute.ExaquteTaskLocal import delete_object
    delete_object(obj)

    from exaqute.ExaquteTaskLocal import delete_file
    temp_file = tempfile.NamedTemporaryFile(delete=False).name
    delete_file(temp_file)
    if os.path.exists(temp_file):
        raise PyCOMPSsException("ERROR: delete_file did not remove the file.")

    from exaqute.ExaquteTaskLocal import compute
    obj = [1, 2, 3]
    result = compute(obj)
    assert obj == result, "ERROR: compute does not retrieve the same object."

    from exaqute.ExaquteTaskLocal import Implement

    @Implement()
    def sub(v):
        return v - 1
    result = sub(2)
    assert result == 1, "ERROR: The implement decorator is not transparent."

    from exaqute.ExaquteTaskLocal import Constraint

    @Constraint()
    def pow(v):
        return v * v

    result = pow(2)
    assert result == 4, "ERROR: The constraint decorator is not transparent."


def test_exaqute_execution_characteristics():
    from exaqute.ExecutionCharacteristics import ExecutionCharacteristics
    ec = ExecutionCharacteristics(1, 2, 3, 4, 5)
    assert ec.generation_nodes == 1, "ERROR: wrong number of generation_nodes"
    assert ec.generation_cpus_per_node == 2, "ERROR: wrong number of generation_cpus_per_node"
    assert ec.run_nodes == 3, "ERROR: wrong number of run_nodes"
    assert ec.run_cpus_per_node == 4, "ERROR: wrong number of run_cpus_per_node"
    assert ec.amount_executions == 5, "ERROR: wrong number of amount_executions"


def test_exaqute_execution_constraints():
    from exaqute.ExecutionConstraints import ExecutionConstraints
    constraint = ExecutionConstraints()
    assert constraint.nodes == 1, "ERROR: wrong number of default nodes"
    assert constraint.cpus_per_task == 1, "ERROR: wrong number of default cpus_per_task"  # noqa: E501
    assert constraint.amount_executions == 1, "ERROR: wrong number of amount_executions"  # noqa: E501
