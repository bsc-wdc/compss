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

from pycompss.runtime.management.link.separate import c_extension_link
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.process.manager import new_queue


def test_c_extension_link_wrong_message():
    in_queue = new_queue()
    out_queue = new_queue()
    in_queue.put("UNSUPPORTED")
    is_ok = False
    try:
        c_extension_link(in_queue, out_queue, False, "None", "None")
    except PyCOMPSsException:
        is_ok = True
    assert (
        is_ok
    ), "ERROR: Exception not raised when undefined message received in link."
