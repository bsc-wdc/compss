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
PyCOMPSs Binding - Management - Link messages.

This file contains the messages needed to link with the compss extension.
"""


class LinkMessages:
    """Link messages definitions (used through the queues)."""

    __slots__ = [
        "start",
        "set_debug",
        "stop",
        "cancel_tasks",
        "accessed_file",
        "open_file",
        "close_file",
        "delete_file",
        "get_file",
        "get_directory",
        "barrier",
        "barrier_group",
        "open_task_group",
        "close_task_group",
        "cancel_task_group",
        "snapshot",
        "get_logging_path",
        "get_master_working_path",
        "get_number_of_resources",
        "request_resources",
        "free_resources",
        "register_core_element",
        "process_http_task",
        "process_task",
        "set_pipes",
        "read_pipes",
        "set_wall_clock",
        "command_done",
    ]

    def __init__(self) -> None:
        """Instantiate a new link messages object."""
        self.start = "START"
        self.set_debug = "SET_DEBUG"
        self.stop = "STOP"
        self.cancel_tasks = "CANCEL_TASKS"
        self.accessed_file = "ACCESSED_FILE"
        self.open_file = "OPEN_FILE"
        self.close_file = "CLOSE_FILE"
        self.delete_file = "DELETE_FILE"
        self.get_file = "GET_FILE"
        self.get_directory = "GET_DIRECTORY"
        self.barrier = "BARRIER"
        self.barrier_group = "BARRIER_GROUP"
        self.open_task_group = "OPEN_TASK_GROUP"
        self.close_task_group = "CLOSE_TASK_GROUP"
        self.cancel_task_group = "CANCEL_TASK_GROUP"
        self.snapshot = "SNAPSHOT"
        self.get_logging_path = "GET_LOGGING_PATH"
        self.get_master_working_path = "GET_MASTER_WORKING_PATH"
        self.get_number_of_resources = "GET_NUMBER_OF_RESOURCES"
        self.request_resources = "REQUEST_RESOURCES"
        self.free_resources = "FREE_RESOURCES"
        self.register_core_element = "REGISTER_CORE_ELEMENT"
        self.process_http_task = "PROCESS_HTTP_TASK"
        self.process_task = "PROCESS_TASK"
        self.set_pipes = "SET_PIPES"
        self.read_pipes = "READ_PIPES"
        self.set_wall_clock = "SET_WALL_CLOCK"
        self.command_done = "COMMAND_DONE"  # Default response message


LINK_MESSAGES = LinkMessages()
