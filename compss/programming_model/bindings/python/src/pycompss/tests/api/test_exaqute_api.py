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
import tempfile
from pycompss.util.exceptions import PyCOMPSsException


ERROR_AMOUNTS = "ERROR: wrong number of amount_executions"


def test_exaqute_parameter():
    from exaqute.local.consts import IN  # noqa
    from exaqute.local.consts import INOUT  # noqa
    from exaqute.local.consts import FILE_IN  # noqa
    from exaqute.local.consts import FILE_OUT  # noqa
    from exaqute.local.consts import FILE_INOUT  # noqa
    from exaqute.local.consts import COLLECTION_IN  # noqa
    from exaqute.local.consts import COLLECTION_INOUT  # noqa
    from exaqute.local.consts import COLLECTION_OUT  # noqa
    from exaqute.local.consts import Type  # noqa
    from exaqute.local.consts import Depth  # noqa
    from exaqute.local.consts import block_count  # noqa
    from exaqute.local.consts import block_length  # noqa
    from exaqute.local.consts import stride  # noqa

    from exaqute.common.consts import INOUT  # noqa
    from exaqute.common.consts import FILE_IN     # noqa
    from exaqute.common.consts import FILE_OUT    # noqa
    from exaqute.common.consts import FILE_INOUT  # noqa
    from exaqute.common.consts import COLLECTION_IN     # noqa
    from exaqute.common.consts import COLLECTION_INOUT  # noqa
    from exaqute.common.consts import COLLECTION_OUT    # noqa
    from exaqute.common.consts import Type         # noqa
    from exaqute.common.consts import Depth        # noqa
    from exaqute.common.consts import block_count  # noqa
    from exaqute.common.consts import block_length  # noqa
    from exaqute.common.consts import stride  # noqa


def test_exaqute_task():
    from exaqute.pycompss.decorators import Task as ExaquteTask
    _ = ExaquteTask()

    from exaqute.local.functions import get_value_from_remote
    result = False
    try:
        get_value_from_remote(None)
    except Exception:  # NOSONAR
        # This is OK
        result = True
    assert result, "ERROR: Expected get value not implemented exception."

    from exaqute.local.functions import barrier
    result = False
    try:
        barrier()
    except Exception:  # NOSONAR
        # This is OK
        result = True
    assert result, "ERROR: Expected barrier not implemented exception."

    from exaqute.local.functions import delete_object
    result = False
    try:
        delete_object(None)
    except Exception:  # NOSONAR
        # This is OK
        result = True
    assert result, "ERROR: Expected delete object not implemented exception."


def test_exaqute_task_local():
    from exaqute.local.functions import init
    init()

    from exaqute.local.decorators import Task as ExaquteTask

    @ExaquteTask()
    def increment(a):
        return a + 1
    result = increment(2)
    print(result.value)
    assert result.value == 3, "ERROR: The local decorator is not transparent."

    from exaqute.local.functions import get_value_from_remote
    obj = [1, 2, 3]
    result = get_value_from_remote(obj)
    assert obj == result, "ERROR: get_value_from_remote does not retrieve the same object."  # noqa: E501

    from exaqute.local.functions import barrier
    barrier()

    from exaqute.local.functions import delete_object
    delete_object(obj)
