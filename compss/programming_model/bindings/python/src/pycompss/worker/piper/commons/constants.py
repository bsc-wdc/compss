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
PyCOMPSs Persistent Worker Pipe Constants
=========================================
    This file contains a set of constants used when communication through
    pipes.
"""

# ############### #
#  Tag variables  #
# ############### #
"""
---------------------
TAGS EXPECTED FORMAT:
---------------------
- EXECUTE_TASK_TAG........... "task" taskId jobOut jobErr task_params
- END_TASK_TAG............... "endTask" taskId endStatus
- CANCEL_TASK_TAG............ "cancelTask" in_pipe out_pipe
- COMPSS_EXCEPTION_TAG....... "compssException" taskId exception_message
- ERROR_TASK_TAG............. TBD
- ERROR_TAG.................. "error" [MESSAGE EXPECTED]
- PING_TAG................... "ping"
- PONG_TAG................... "pong"
- ADD_EXECUTOR_TAG........... "addExecutor" in_pipe out_pipe
- ADD_EXECUTOR_FAILED_TAG.... "addExecutorFailed"
- ADDED_EXECUTOR_TAG......... "addedExecutor"
- QUERY_EXECUTOR_ID_TAG...... "query" in_pipe out_pipe
- REPLY_EXECUTOR_ID_TAG...... "reply" executor_id
- REMOVE_EXECUTOR_TAG........ "removeExecutor" in_pipe out_pipe
- REMOVED_EXECUTOR_TAG....... "removedExecutor"
- QUIT_TAG................... "quit"
- REMOVE_TAG................. TBD
- SERIALIZE_TAG.............. TBD
"""
EXECUTE_TASK_TAG = "EXECUTE_TASK"
END_TASK_TAG = "END_TASK"
CANCEL_TASK_TAG = "CANCEL_TASK"
COMPSS_EXCEPTION_TAG = "COMPSS_EXCEPTION"
ERROR_TASK_TAG = "ERROR_TASK"
ERROR_TAG = "ERROR"
PING_TAG = "PING"
PONG_TAG = "PONG"
ADD_EXECUTOR_TAG = "ADD_EXECUTOR"
ADD_EXECUTOR_FAILED_TAG = "ADD_EXECUTOR_FAILED"
ADDED_EXECUTOR_TAG = "ADDED_EXECUTOR"
QUERY_EXECUTOR_ID_TAG = "QUERY_EXECUTOR_ID"
REPLY_EXECUTOR_ID_TAG = "REPLY_EXECUTOR_ID"
REMOVE_EXECUTOR_TAG = "REMOVE_EXECUTOR"
REMOVED_EXECUTOR_TAG = "REMOVED_EXECUTOR"
QUIT_TAG = "QUIT"
REMOVE_TAG = "REMOVE"
SERIALIZE_TAG = "SERIALIZE"

# ###################### #
#  Tracing events Codes  #
# ###################### #
# Should be equal to Tracer.java definitions (but only worker running all
# other are trace through with function-list)
# Still uses the events defined in commons.worker_constants

# ################# #
#  Other variables  #
# ################# #
HEADER = "[PYTHON WORKER] "
