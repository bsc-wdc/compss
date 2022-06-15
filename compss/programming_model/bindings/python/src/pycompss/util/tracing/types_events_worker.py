#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Util - Tracing - Types and events in worker.

This file contains a set of constants that identify the types and events
emitted from when the binding acts as worker.

IMPORTANT!!!
Must be equal to Tracer.java and TraceEvent.java definitions
"""


class TypesEventsWorker:
    """Constant events that can be emitted for the worker group."""

    __slots__ = (
        "sync_type",
        "binding_tasks_func_type",
        "inside_tasks_type",
        "bind_cpus_event",
        "bind_gpus_event",
        "setup_environment_event",
        "get_task_params_event",
        "import_user_module_event",
        "execute_user_code_event",
        "deserialize_from_bytes_event",
        "deserialize_from_file_event",
        "serialize_to_file_event",
        "serialize_to_file_mpienv_event",
        "build_successful_message_event",
        "build_compss_exception_message_event",
        "build_exception_message_event",
        "clean_environment_event",
        "get_by_id_event",
        "getid_event",
        "make_persistent_event",
        "delete_persistent_event",
        "retrieve_object_from_cache_event",
        "insert_object_into_cache_event",
        "remove_object_from_cache_event",
        "inside_tasks_cpu_affinity_type",
        "inside_tasks_cpu_count_type",
        "inside_tasks_gpu_affinity_type",
        "inside_worker_type",
        "worker_running_event",
        "process_task_event",
        "process_ping_event",
        "process_quit_event",
        "init_storage_event",
        "stop_storage_event",
        "init_storage_at_worker_event",
        "finish_storage_at_worker_event",
        "init_worker_postfork_event",
        "finish_worker_postfork_event",
        "worker_task_instantiation",
        "binding_serialization_cache_size_type",
        "binding_deserialization_cache_size_type",
        "wait_on_event",
    )

    def __init__(self):
        """Create a new instance of TypesEventsWorker."""
        # Binding worker type
        self.sync_type = 8000666

        # Tasks at master (id corresponds to task) type:
        self.binding_tasks_func_type = 9000000

        # Tasks at worker type:
        self.inside_tasks_type = 9000100

        # Binding worker events
        self.bind_cpus_event = 1
        self.bind_gpus_event = 2
        self.setup_environment_event = 3
        self.get_task_params_event = 4
        self.import_user_module_event = 5
        self.execute_user_code_event = 6
        self.deserialize_from_bytes_event = 7
        self.deserialize_from_file_event = 8
        self.serialize_to_file_event = 9
        self.serialize_to_file_mpienv_event = 10
        self.build_successful_message_event = 11
        self.build_compss_exception_message_event = 12
        self.build_exception_message_event = 13
        self.clean_environment_event = 14
        self.get_by_id_event = 15
        self.getid_event = 16
        self.make_persistent_event = 17  # TODO: INCLUDE INTERFACE
        self.delete_persistent_event = 18  # TODO: INCLUDE INTERFACE
        self.retrieve_object_from_cache_event = 19
        self.insert_object_into_cache_event = 20
        self.remove_object_from_cache_event = 21
        self.wait_on_event = 22
        self.worker_task_instantiation = 23  # task worker __init__

        # Task affinity events:
        self.inside_tasks_cpu_affinity_type = 9000150
        self.inside_tasks_cpu_count_type = 9000151
        self.inside_tasks_gpu_affinity_type = 9000160

        # Worker events:
        self.inside_worker_type = 9000200
        self.worker_running_event = 1  # task invocation
        self.process_task_event = 2  # process_task
        self.process_ping_event = 3  # process_ping
        self.process_quit_event = 4  # process_quit
        self.init_storage_event = 5  # init_storage
        self.stop_storage_event = 6  # stop_storage
        self.init_storage_at_worker_event = 7  # initStorageAtWorker
        self.finish_storage_at_worker_event = 8  # finishStorageAtWorker
        self.init_worker_postfork_event = 9  # initWorkerPostFork
        self.finish_worker_postfork_event = 10  # finishWorkerPostFork
        # Other worker events:
        self.binding_serialization_cache_size_type = 9000602
        self.binding_deserialization_cache_size_type = 9000603


TRACING_WORKER = TypesEventsWorker()
