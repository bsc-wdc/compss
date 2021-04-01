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
PyCOMPSs Interactive utils
==============================================
    Provides auxiliary methods for the interactive mode
"""


def parameters_to_dict(log_level,                      # type: str
                       debug,                          # type: bool
                       o_c,                            # type: bool
                       graph,                          # type: bool
                       trace,                          # type: bool
                       monitor,                        # type: int
                       project_xml,                    # type: str
                       resources_xml,                  # type: str
                       summary,                        # type: bool
                       task_execution,                 # type: str
                       storage_impl,                   # type: str
                       storage_conf,                   # type: str
                       streaming_backend,              # type: str
                       streaming_master_name,          # type: str
                       streaming_master_port,          # type: str
                       task_count,                     # type: int
                       app_name,                       # type: str
                       uuid,                           # type: str
                       base_log_dir,                   # type: str
                       specific_log_dir,               # type: str
                       extrae_cfg,                     # type: str
                       comm,                           # type: str
                       conn,                           # type: str
                       master_name,                    # type: str
                       master_port,                    # type: str
                       scheduler,                      # type: str
                       jvm_workers,                    # type: str
                       cpu_affinity,                   # type: str
                       gpu_affinity,                   # type: str
                       fpga_affinity,                  # type: str
                       fpga_reprogram,                 # type: str
                       profile_input,                  # type: str
                       profile_output,                 # type: str
                       scheduler_config,               # type: str
                       external_adaptation,            # type: bool
                       propagate_virtual_environment,  # type: bool
                       mpi_worker,                     # type: bool
                       worker_cache,                   # type: str
                       ):  # NOSONAR
    # type: (...) -> dict
    """ Converts the given parameters into a dictionary """
    all_vars = {"log_level": log_level,
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
                "base_log_dir": base_log_dir,
                "specific_log_dir": specific_log_dir,
                "extrae_cfg": extrae_cfg,
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
                "worker_cache": worker_cache}
    return all_vars
