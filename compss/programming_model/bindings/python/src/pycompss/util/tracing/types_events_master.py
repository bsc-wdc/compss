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
PyCOMPSs Util - Tracing - Types and events in master.

This file contains a set of constants that identify the types and events
emitted from when the binding acts as master.

IMPORTANT!!!
Must be equal to Tracer.java and TraceEvent.java definitions
"""


class TypesEventsMaster:
    """Constant events that can be emitted for the master group."""

    __slots__ = (
        "binding_master_type",
        "start_runtime_event",
        "stop_runtime_event",
        "application_running_event",
        "init_storage_event",
        "stop_storage_event",
        "accessed_file_event",
        "open_file_event",
        "delete_file_event",
        "get_file_event",
        "get_directory_event",
        "delete_object_event",
        "barrier_event",
        "barrier_group_event",
        "open_task_group_event",
        "close_task_group_event",
        "cancel_task_group_event",
        "get_log_path_event",
        "get_tmp_path_event",
        "get_number_resources_event",
        "request_resources_event",
        "free_resources_event",
        "register_core_element_event",
        "wait_on_event",
        "process_task_event",
        "wall_clock_limit_event",
        "snapshot_event",
        "task_instantiation",
        "inspect_function_arguments",
        "inspect_constraints",
        "get_function_information",
        "get_function_signature",
        "check_interactive",
        "extract_core_element",
        "prepare_core_element",
        "update_core_element",
        "get_upper_decorators_kwargs",
        "process_other_arguments",
        "process_parameters",
        "process_return",
        "build_return_objects",
        "serialize_object",
        "build_compss_types_directions",
        "process_task_binding",
        "attributes_cleanup",
        "binding_serialization_size_type",
        "binding_deserialization_size_type",
        "binding_serialization_object_num_type",
        "binding_deserialization_object_num_type",
    )

    def __init__(self) -> None:
        """Create a new instance of TypesEventsWorker."""
        # Binding master type
        self.binding_master_type = 9000300

        # Binding master events
        self.start_runtime_event = 1
        self.stop_runtime_event = 2
        self.application_running_event = 3  # application running
        # 4 is empty
        self.init_storage_event = 5  # init_storage (same as worker)
        self.stop_storage_event = 6  # stop_storage (same as worker)

        # API EVENTS
        self.accessed_file_event = 7
        self.open_file_event = 8
        self.delete_file_event = 9
        self.get_file_event = 10
        self.get_directory_event = 11
        self.delete_object_event = 12
        self.barrier_event = 13
        self.barrier_group_event = 14
        self.open_task_group_event = 15
        self.close_task_group_event = 16
        self.get_log_path_event = 17
        self.get_tmp_path_event = 18
        self.get_number_resources_event = 19
        self.request_resources_event = 20
        self.free_resources_event = 21
        self.register_core_element_event = 22
        self.wait_on_event = 23
        self.process_task_event = 24
        self.wall_clock_limit_event = 25
        self.snapshot_event = 26
        self.cancel_task_group_event = 27

        # CALL EVENTS
        self.task_instantiation = 100
        self.inspect_function_arguments = 101
        self.inspect_constraints = 102
        self.get_function_information = 103
        self.get_function_signature = 104
        self.check_interactive = 105
        self.extract_core_element = 106
        self.prepare_core_element = 107
        self.update_core_element = 108
        self.get_upper_decorators_kwargs = 109
        self.process_other_arguments = 110
        self.process_parameters = 111
        self.process_return = 112
        self.build_return_objects = 113
        self.serialize_object = 114
        self.build_compss_types_directions = 115
        self.process_task_binding = 116
        self.attributes_cleanup = 117

        # Serialization/Deserialization types:
        self.binding_serialization_size_type = 9000600
        self.binding_deserialization_size_type = 9000601
        self.binding_serialization_object_num_type = 9000700
        self.binding_deserialization_object_num_type = 9000701


TRACING_MASTER = TypesEventsMaster()
