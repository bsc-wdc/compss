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
"""

# ###################### #
#  Tracing events Codes  #
# ###################### #
# Should be equal to Tracer.java and TraceEvent.java definitions
MASTER_EVENTS = 60000300
START_RUNTIME_EVENT = 1001
STOP_RUNTIME_EVENT = 1002
APPLICATION_RUNNING_EVENT = 1003       # application running
# 1004 is empty
INIT_STORAGE_EVENT = 1005              # init_storage (same as worker)
STOP_STORAGE_EVENT = 1006              # stop_storage (same as worker)
# API EVENTS
ACCESSED_FILE_EVENT = 1007
OPEN_FILE_EVENT = 1008
DELETE_FILE_EVENT = 1009
GET_FILE_EVENT = 1010
GET_DIRECTORY_EVENT = 1011
DELETE_OBJECT_EVENT = 1012
BARRIER_EVENT = 1013
BARRIER_GROUP_EVENT = 1014
OPEN_TASK_GROUP_EVENT = 1015
CLOSE_TASK_GROUP_EVENT = 1016
GET_LOG_PATH_EVENT = 1017
GET_NUMBER_RESOURCES_EVENT = 1018
REQUEST_RESOURCES_EVENT = 1019
FREE_RESOURCES_EVENT = 1021
REGISTER_CORE_ELEMENT_EVENT = 1021
WAIT_ON_EVENT = 1022
PROCESS_TASK_EVENT = 1023
# CALL EVENTS
TASK_INSTANTIATION = 2000
EXTRACT_CORE_ELEMENT = 2001
INSPECT_FUNCTION_ARGUMENTS = 2002
PROCESS_PARAMETERS = 2003
GET_FUNCTION_INFORMATION = 2004
PREPARE_CORE_ELEMENT = 2005
GET_FUNCTION_SIGNATURE = 2006
UPDATE_CORE_ELEMENT = 2007
PROCESS_RETURN = 2008
# GET_COMPUTING_NODES = 2009  # noqa Inside process other arguments with reduction and hints
PROCESS_OTHER_ARGUMENTS = 2010
BUILD_RETURN_OBJECTS = 2011
SERIALIZE_OBJECT = 2012
BUILD_COMPSS_TYPES_DIRECTIONS = 2013
ATTRIBUTES_CLEANUP = 2014
