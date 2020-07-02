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

"""
PyCOMPSs Binding - Management - Runtime
=======================================
    This file contains the COMPSs runtime connection.
    Loads the external C module.
"""

from pycompss.runtime.link import establish_interactive_link
from pycompss.runtime.link import establish_link

# C module extension for the communication with the runtime
# See ext/compssmodule.cc
# Keep the COMPSs runtime link in this module so that any module can access
# it after starting the runtime.
COMPSs = None


def load_runtime(external_process=False):
    """
    Starts the runtime and loads the external C extension module.

    :param external_process: Loads the runtime in an external process if true.
                             Within this python process if false.
    :return: The COMPSs library to be used
    """
    global COMPSs

    if external_process:
        # For interactive python environments
        COMPSs = establish_interactive_link()
    else:
        # Normal python environments
        COMPSs = establish_link()

    return COMPSs
