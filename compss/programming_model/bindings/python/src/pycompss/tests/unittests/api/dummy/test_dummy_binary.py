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

CONTEXT.set_out_of_scope()

from pycompss.api.api import compss_wait_on  # noqa
from pycompss.api.binary import Binary  # noqa
from pycompss.api.task import Task  # noqa


@Binary(binary="date", working_dir="/tmp")  # NOSONAR
@Task(returns=1)
def check_date():
    # Intentionally empty since it is a binary task.
    pass


def test_dummy_task():
    result = check_date()
    result = compss_wait_on(result)
    assert result == 0, "ERROR: Unexpected exit code with dummy @binary."
