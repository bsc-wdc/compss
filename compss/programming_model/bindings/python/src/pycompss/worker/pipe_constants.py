#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Persistent Worker
===========================
    This file contains a set of constants used when communication through pipes.
"""

#####################
#  Tag variables
#####################
EXECUTE_TASK_TAG = "EXECUTE_TASK"           # -- "task" taskId jobOut jobErr task_params
END_TASK_TAG = "END_TASK"                   # -- "endTask" taskId endStatus
ERROR_TASK_TAG = "ERROR_TASK"
ERROR_TAG = "ERROR"                         # -- "error" [MESSAGE EXPECTED]
PING_TAG = "PING"                           # -- "ping"
PONG_TAG = "PONG"                           # -- "pong"
ADD_EXECUTOR_TAG = "ADD_EXECUTOR"           # -- "addExecutor" in_pipe out_pipe
ADDED_EXECUTOR_TAG = "ADDED_EXECUTOR"       # -- "addedExecutor"
QUERY_EXECUTOR_ID_TAG = "QUERY_EXECUTOR_ID" # -- "query" in_pipe out_pipe
REPLY_EXECUTOR_ID_TAG = "REPLY_EXECUTOR_ID" # -- "reply" executor_id
REMOVE_EXECUTOR_TAG = "REMOVE_EXECUTOR"     # -- "removeExecutor" in_pipe out_pipe
REMOVED_EXECUTOR_TAG = "REMOVED_EXECUTOR"   # -- "removedExecutor"
QUIT_TAG = "QUIT"                           # -- "quit"
REMOVE_TAG = "REMOVE"
SERIALIZE_TAG = "SERIALIZE"

#####################
#  Tracing events Codes
#####################
# Should be equal to Tracer.java definitions (but only worker running all other are trace through
# with function-list
SYNC_EVENTS = 8000666
TASK_EVENTS = 60000100
WORKER_RUNNING_EVENT = 102
