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


def test_get_compss_direction():
    from pycompss.api.parameter import DIRECTION
    from pycompss.runtime.management.direction import get_compss_direction

    write = get_compss_direction("w")
    assert (
        write == DIRECTION.OUT
    ), "ERROR: Wrong w direction. Expected: OUT"  # noqa: E501
    read_write = get_compss_direction("r+")
    assert (
        read_write == DIRECTION.INOUT
    ), "ERROR: Wrong r+ direction. Expected: INOUT"  # noqa: E501
    append = get_compss_direction("a")
    assert (
        append == DIRECTION.INOUT
    ), "ERROR: Wrong a direction. Expected: INOUT"  # noqa: E501
    concurrent = get_compss_direction("c")
    assert (
        concurrent == DIRECTION.CONCURRENT
    ), "ERROR: Wrong c direction. Expected: CONCURRENT"  # noqa: E501
    commutative = get_compss_direction("cv")
    assert (
        commutative == DIRECTION.COMMUTATIVE
    ), "ERROR: Wrong cv direction. Expected: COMMUTATIVE"  # noqa: E501
    read = get_compss_direction("OTHER")
    assert (
        read == DIRECTION.IN
    ), "ERROR: Wrong other direction. Expected: IN"  # noqa: E501
