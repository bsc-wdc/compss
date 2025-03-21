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

from pycompss.api.dummy.reduction import Reduction
from pycompss.api.dummy.task import Task


@Reduction()
@Task()
def increment(value):
    return value + 1


def test_dummy_task():
    result = increment(1)
    assert result == 2, (
        "Unexpected result provided by the dummy task decorator. Expected: 2 Received: %s"  # noqa: E501
        % str(result)
    )
