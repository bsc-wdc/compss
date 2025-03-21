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
PyCOMPSs API - commons - constants.

This file contains the common decorator labels.
"""


class _Labels:  # pylint: disable=R0903,R0902
    # disable=too-few-public-methods, too-many-instance-attributes
    """Currently supported labels in all decorators."""

    __slots__ = (
        "returns",
        "priority",
        "on_failure",
        "defaults",
        "time_out",
        "is_replicated",
        "is_distributed",
        "varargs_type",
        "target_direction",
        "numba",
        "numba_flags",
        "numba_signature",
        "numba_declaration",
        "tracing_hook",
        "computing_nodes",
        "computing_units",
        "working_dir",
        "args",
        "parameters",
        "fail_by_exit_value",
        "task",
        "mpi",
        "mpmd_mpi",
        "multinode",
        "binary",
        "compss",
        "http",
        "runner",
        "processes",
        "programs",
        "scale_by_cu",
        "app_name",
        "runcompss",
        "flags",
        "processes_per_node",
        "worker_in_master",
        "engine",
        "image",
        "options",
        "df_script",
        "df_executor",
        "df_lib",
        "julia_executor",
        "julia_args",
        "julia_script",
        "source_class",
        "method",
        "management",
        "management_ignore",
        "management_retry",
        "management_cancel_successor",
        "management_fail",
        "kernel",
        "chunk_size",
        "is_reduce",
        "cache_returns",
        "service_name",
        "resource",
        "request",
        "payload",
        "payload_type",
        "produces",
        "updates",
        "config_file",
        "software_config_file",  # this is internal
        "properties",
        "execution",
        "type",
        "is_workflow",
    )

    def __init__(self) -> None:  # pylint: disable=too-many-statements
        # Expected labels
        # - Task decorator
        self.target_direction = "target_direction"
        self.returns = "returns"
        self.cache_returns = "cache_returns"
        self.priority = "priority"
        self.defaults = "defaults"
        self.time_out = "time_out"
        self.is_replicated = "is_replicated"
        self.is_distributed = "is_distributed"
        self.computing_nodes = "computing_nodes"
        self.computing_units = "computing_units"
        self.is_reduce = "is_reduce"
        self.chunk_size = "chunk_size"
        self.tracing_hook = "tracing_hook"
        self.numba = "numba"
        self.numba_flags = "numba_flags"
        self.numba_signature = "numba_signature"
        self.numba_declaration = "numba_declaration"
        self.varargs_type = "varargs_type"
        # - Others
        self.on_failure = "on_failure"
        self.working_dir = "working_dir"
        self.args = "args"
        self.parameters = "parameters"
        self.fail_by_exit_value = "fail_by_exit_value"
        self.task = "task"
        self.mpi = "mpi"
        self.binary = "binary"
        self.http = "http"
        self.compss = "compss"
        self.mpmd_mpi = "mpmd_mpi"
        self.multinode = "multinode"
        self.runner = "runner"
        self.processes = "processes"
        self.programs = "programs"
        self.scale_by_cu = "scale_by_cu"
        self.app_name = "app_name"
        self.runcompss = "runcompss"
        self.flags = "flags"
        self.processes_per_node = "processes_per_node"
        self.worker_in_master = "worker_in_master"
        self.engine = "engine"
        self.image = "image"
        self.options = "options"
        self.df_script = "df_script"
        self.df_executor = "df_executor"
        self.df_lib = "df_lib"
        self.julia_executor = "executor"
        self.julia_args = "args"
        self.julia_script = "script"
        self.source_class = "source_class"
        self.method = "method"
        self.management = "management"
        self.management_ignore = "IGNORE"
        self.management_retry = "RETRY"
        self.management_cancel_successor = "CANCEL_SUCCESSORS"
        self.management_fail = "FAIL"
        self.kernel = "kernel"

        self.is_workflow = "is_workflow"
        # Http tasks
        self.service_name = "service_name"
        self.resource = "resource"
        self.request = "request"
        self.payload = "payload"
        self.payload_type = "payload_type"
        self.produces = "produces"
        self.updates = "updates"
        self.config_file = "config_file"
        self.software_config_file = "software_config_file"
        self.properties = "properties"
        self.execution = "execution"
        self.type = "type"


class _LegacyLabels:  # pylint: disable=R0903,R0902
    # disable=too-few-public-methods, too-many-instance-attributes
    """Supported labels in all decorators but sensitive to be removed."""

    __slots__ = (
        "is_replicated",
        "is_distributed",
        "varargs_type",
        "target_direction",
        "time_out",
        "computing_units",
        "computing_nodes",
        "working_dir",
        "app_name",
        "worker_in_master",
        "df_executor",
        "df_lib",
        "df_script",
        "source_class",
    )

    def __init__(self) -> None:
        self.is_replicated = "isReplicated"
        self.is_distributed = "isDistributed"
        self.varargs_type = "varargsType"
        self.target_direction = "targetDirection"
        self.time_out = "timeOut"
        self.computing_units = "computingUnits"
        self.computing_nodes = "computingNodes"
        self.working_dir = "workingDir"
        self.app_name = "appName"
        self.worker_in_master = "workerInMaster"
        self.df_executor = "dfExecutor"
        self.df_lib = "dfLib"
        self.df_script = "dfScript"
        self.source_class = "sourceClass"


class _InternalLabels:  # pylint: disable=too-few-public-methods
    """Internal labels."""

    __slots__ = ["unassigned"]

    def __init__(self) -> None:
        self.unassigned = "[unassigned]"


LABELS = _Labels()
LEGACY_LABELS = _LegacyLabels()
INTERNAL_LABELS = _InternalLabels()
