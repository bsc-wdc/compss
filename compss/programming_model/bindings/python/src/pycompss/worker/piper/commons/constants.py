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

"""
PyCOMPSs Worker - Piper - Commons - Constants.

This file contains a set of constants used when communication through pipes.

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


class _Tags:
    """Currently supported tags in piper workers."""

    __slots__ = (
        "execute_task",
        "end_task",
        "cancel_task",
        "compss_exception",
        "error_task",
        "error",
        "ping",
        "pong",
        "add_executor",
        "add_executor_failed",
        "added_executor",
        "query_executor_id",
        "reply_executor_id",
        "remove_executor",
        "removed_executor",
        "quit",
        "remove",
        "serialize",
    )

    def __init__(self) -> None:  # pylint: disable=too-many-statements
        """Define supported Tags."""
        self.execute_task = "EXECUTE_TASK"
        self.end_task = "END_TASK"
        self.cancel_task = "CANCEL_TASK"
        self.compss_exception = "COMPSS_EXCEPTION"
        self.error_task = "ERROR_TASK"
        self.error = "ERROR"
        self.ping = "PING"
        self.pong = "PONG"
        self.add_executor = "ADD_EXECUTOR"
        self.add_executor_failed = "ADD_EXECUTOR_FAILED"
        self.added_executor = "ADDED_EXECUTOR"
        self.query_executor_id = "QUERY_EXECUTOR_ID"
        self.reply_executor_id = "REPLY_EXECUTOR_ID"
        self.remove_executor = "REMOVE_EXECUTOR"
        self.removed_executor = "REMOVED_EXECUTOR"
        self.quit = "QUIT"
        self.remove = "REMOVE"
        self.serialize = "SERIALIZE"


TAGS = _Tags()

# ################# #
#  Other variables  #
# ################# #
HEADER = "[PYTHON WORKER] "
