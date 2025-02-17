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
PyCOMPSs Util - Interactive - Utils.

Provides auxiliary methods for the interactive mode.
"""

from pycompss.util.typing_helper import typing


def parameters_to_dict(
    log_level: str,
    debug: bool,
    o_c: bool,
    graph: bool,
    trace: bool,
    monitor: int,
    project_xml: str,
    resources_xml: str,
    summary: bool,
    task_execution: str,
    storage_impl: str,
    storage_conf: str,
    streaming_backend: str,
    streaming_master_name: str,
    streaming_master_port: str,
    task_count: int,
    app_name: str,
    uuid: str,
    log_dir: str,
    master_working_dir: str,
    extrae_cfg: str,
    extrae_final_directory: str,
    comm: str,
    conn: str,
    master_name: str,
    master_port: str,
    scheduler: str,
    jvm_workers: str,
    cpu_affinity: str,
    gpu_affinity: str,
    fpga_affinity: str,
    fpga_reprogram: str,
    profile_input: str,
    profile_output: str,
    scheduler_config: str,
    external_adaptation: bool,
    propagate_virtual_environment: bool,
    mpi_worker: bool,
    worker_cache: typing.Union[bool, str],
    shutdown_in_node_failure: bool,
    io_executors: int,
    env_script: str,
    reuse_on_block: bool,
    nested_enabled: bool,
    tracing_task_dependencies: bool,
    trace_label: str,
    extrae_cfg_python: str,
    wcl: int,
    cache_profiler: bool,
    ear: bool,
    data_provenance: bool,
    checkpoint_policy: str,
    checkpoint_params: str,
    checkpoint_folder: str,
) -> dict:
    """Convert all given parameters into a dictionary."""
    all_vars = {
        "log_level": log_level,
        "debug": debug,
        "o_c": o_c,
        "graph": graph,
        "trace": trace,
        "monitor": monitor,
        "project_xml": project_xml,
        "resources_xml": resources_xml,
        "summary": summary,
        "task_execution": task_execution,
        "storage_impl": storage_impl,
        "storage_conf": storage_conf,
        "streaming_backend": streaming_backend,
        "streaming_master_name": streaming_master_name,
        "streaming_master_port": streaming_master_port,
        "task_count": task_count,
        "app_name": app_name,
        "uuid": uuid,
        "log_dir": log_dir,
        "master_working_dir": master_working_dir,
        "extrae_cfg": extrae_cfg,
        "extrae_final_directory": extrae_final_directory,
        "comm": comm,
        "conn": conn,
        "master_name": master_name,
        "master_port": master_port,
        "scheduler": scheduler,
        "jvm_workers": jvm_workers,
        "cpu_affinity": cpu_affinity,
        "gpu_affinity": gpu_affinity,
        "fpga_affinity": fpga_affinity,
        "fpga_reprogram": fpga_reprogram,
        "profile_input": profile_input,
        "profile_output": profile_output,
        "scheduler_config": scheduler_config,
        "external_adaptation": external_adaptation,
        "propagate_virtual_environment": propagate_virtual_environment,
        "mpi_worker": mpi_worker,
        "worker_cache": worker_cache,
        "shutdown_in_node_failure": shutdown_in_node_failure,
        "io_executors": io_executors,
        "env_script": env_script,
        "reuse_on_block": reuse_on_block,
        "nested_enabled": nested_enabled,
        "tracing_task_dependencies": tracing_task_dependencies,
        "trace_label": trace_label,
        "extrae_cfg_python": extrae_cfg_python,
        "wcl": wcl,
        "cache_profiler": cache_profiler,
        "ear": ear,
        "data_provenance": data_provenance,
        "checkpoint_policy": checkpoint_policy,
        "checkpoint_params": checkpoint_params,
        "checkpoint_folder": checkpoint_folder,
    }
    return all_vars
