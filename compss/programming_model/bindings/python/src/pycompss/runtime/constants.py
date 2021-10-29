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
PyCOMPSs Runtime Constants
==========================
    This file contains a set of constants used from the runtime.

    SEE ALSO: pycompss/runtime/commons/constants.py
"""

# ###################### #
#  Tracing events Codes  #
# ###################### #
# Should be equal to Tracer.java and TraceEvent.java definitions
BINDING_MASTER_TYPE = 9000300
START_RUNTIME_EVENT = 1
STOP_RUNTIME_EVENT = 2
APPLICATION_RUNNING_EVENT = 3       # application running
# 4 is empty
INIT_STORAGE_EVENT = 5              # init_storage (same as worker)
STOP_STORAGE_EVENT = 6              # stop_storage (same as worker)
# API EVENTS
ACCESSED_FILE_EVENT = 7
OPEN_FILE_EVENT = 8
DELETE_FILE_EVENT = 9
GET_FILE_EVENT = 10
GET_DIRECTORY_EVENT = 11
DELETE_OBJECT_EVENT = 12
BARRIER_EVENT = 13
BARRIER_GROUP_EVENT = 14
OPEN_TASK_GROUP_EVENT = 15
CLOSE_TASK_GROUP_EVENT = 16
GET_LOG_PATH_EVENT = 17
GET_NUMBER_RESOURCES_EVENT = 18
REQUEST_RESOURCES_EVENT = 19
# 20 is empty
FREE_RESOURCES_EVENT = 21
REGISTER_CORE_ELEMENT_EVENT = 21
WAIT_ON_EVENT = 22
PROCESS_TASK_EVENT = 23
# CALL EVENTS
TASK_INSTANTIATION = 100
EXTRACT_CORE_ELEMENT = 101
INSPECT_FUNCTION_ARGUMENTS = 102
PROCESS_PARAMETERS = 103
GET_FUNCTION_INFORMATION = 104
PREPARE_CORE_ELEMENT = 105
GET_FUNCTION_SIGNATURE = 106
UPDATE_CORE_ELEMENT = 107
PROCESS_RETURN = 108
# GET_COMPUTING_NODES = 109  # noqa Inside process other arguments with reduction and hints
PROCESS_OTHER_ARGUMENTS = 110
BUILD_RETURN_OBJECTS = 111
SERIALIZE_OBJECT = 112
BUILD_COMPSS_TYPES_DIRECTIONS = 113
ATTRIBUTES_CLEANUP = 114

# SERIALIZATION/DESERIALIZATION EVENTS:
BINDING_SERIALIZATION_SIZE_TYPE = 9000600
BINDING_DESERIALIZATION_SIZE_TYPE = 9000601
BINDING_SERIALIZATION_OBJECT_NUM_TYPE = 9000700
BINDING_DESERIALIZATION_OBJECT_NUM_TYPE = 9000701
