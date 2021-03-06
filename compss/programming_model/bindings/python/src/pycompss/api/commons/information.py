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

"""
PyCOMPSs API - Information
==========================
   Information about available resources and functions in PyCOMPSs
"""

available_decorators = (
    'binary',
    'container',
    'compss',
    'constraint',
    'decaf',
    'implement',
    'local',
    'mpi',
    'multinode',
    'ompss',
    'opencl',
    'parallel',
    'task'
)

non_worker_decorators = (
    'binary',
    'container',
    'compss',
    'decaf',
    'mpi',
    'ompss',
    'opencl'
)
