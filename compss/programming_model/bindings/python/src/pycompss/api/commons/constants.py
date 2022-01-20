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
PyCOMPSs API - COMMONS - Constants
==================================
    This file contains the common decorator labels.
"""

# Expected labels
RETURNS = "returns"
PRIORITY = "priority"
ON_FAILURE = "on_failure"
DEFAULTS = "defaults"
TIME_OUT = "time_out"
IS_REPLICATED = "is_replicated"
IS_DISTRIBUTED = "is_distributed"
VARARGS_TYPE = "varargs_type"
TARGET_DIRECTION = "target_direction"
NUMBA = "numba"
NUMBA_FLAGS = "numba_flags"
NUMBA_SIGNATURE = "numba_signature"
NUMBA_DECLARATION = "numba_declaration"
TRACING_HOOK = "tracing_hook"
COMPUTING_NODES = "computing_nodes"
WORKING_DIR = "working_dir"
PARAMS = "params"
FAIL_BY_EXIT_VALUE = "fail_by_exit_value"
MPI = "mpi"
BINARY = "binary"
RUNNER = "runner"
PROCESSES = "processes"
PROGRAMS = "programs"
SCALE_BY_CU = "scale_by_cu"
APP_NAME = "app_name"
RUNCOMPSS = "runcompss"
FLAGS = "flags"
PROCESSES_PER_NODE = "processes_per_node"
WORKER_IN_MASTER = "worker_in_master"
ENGINE = "engine"
IMAGE = "image"
DF_SCRIPT = "df_script"
DF_EXECUTOR = "df_executor"
DF_LIB = "df_lib"
SOURCE_CLASS = "source_class"
METHOD = "method"
MANAGEMENT = "management"
MANAGEMENT_IGNORE = "IGNORE"
MANAGEMENT_RETRY = "RETRY"
MANAGEMENT_CANCEL_SUCCESSOR = "CANCEL_SUCCESSORS"
MANAGEMENT_FAIL = "FAIL"
KERNEL = "kernel"
CHUNK_SIZE = "chunk_size"
IS_REDUCE = "is_reduce"
# Http tasks
SERVICE_NAME = "service_name"
RESOURCE = "resource"
REQUEST = "request"
PAYLOAD = "payload"
PAYLOAD_TYPE = "payload_type"
PRODUCES = "produces"
UPDATES = "updates"
CONFIG_FILE = "config_file"
PROPERTIES = "properties"
TYPE = "type"

# Legacy labels
LEGACY_IS_REPLICATED = "isReplicated"
LEGACY_IS_DISTRIBUTED = "isDistributed"
LEGACY_VARARGS_TYPE = "varargsType"
LEGACY_TARGET_DIRECTION = "targetDirection"
LEGACY_TIME_OUT = "timeOut"
LEGACY_COMPUTING_NODES = "computingNodes"
LEGACY_WORKING_DIR = "workingDir"
LEGACY_APP_NAME = "appName"
LEGACY_WORKER_IN_MASTER = "workerInMaster"
LEGACY_DF_EXECUTOR = "dfExecutor"
LEGACY_DF_LIB = "dfLib"
LEGACY_DF_SCRIPT = "dfScript"
LEGACY_SOURCE_CLASS = "sourceClass"

# Internal
UNASSIGNED = "[unassigned]"
