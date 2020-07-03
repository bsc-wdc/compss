#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Binding - Management - Runtime
=======================================
    This file contains the COMPSs runtime connection.
    Loads the external C module.
"""

# C module extension for the communication with the runtime
# See ext/compssmodule.cc
# Keep the COMPSs runtime link in this module so that any module can access
# it through the module methods.
_COMPSs = None


def load_runtime(external_process=False):
    """
    Starts the runtime and loads the external C extension module.

    :param external_process: Loads the runtime in an external process if true.
                             Within this python process if false.
    :return: None
    """
    global _COMPSs

    if external_process:
        # For interactive python environments
        from pycompss.runtime.management.link import establish_interactive_link
        _COMPSs = establish_interactive_link()
    else:
        # Normal python environments
        from pycompss.runtime.management.link import establish_link
        _COMPSs = establish_link()


def start_runtime():
    _COMPSs.start_runtime()  # noqa


def set_debug(mode):
    _COMPSs.set_debug(mode)  # noqa


def stop_runtime(code):
    _COMPSs.stop_runtime(code)  # noqa


def cancel_application_tasks(value):
    _COMPSs.cancel_application_tasks(value)  # noqa


def accessed_file(file_name):
    _COMPSs.accessed_file(file_name)  # noqa


def open_file(file_name, mode):
    return _COMPSs.open_file(file_name, mode)  # noqa


def close_file(file_name, mode):
    _COMPSs.close_file(file_name, mode)  # noqa


def delete_file(file_name, mode):
    return _COMPSs.delete_file(file_name, mode)  # noqa


def get_file(app_id, file_name):
    _COMPSs.get_file(app_id, file_name)  # noqa


def get_directory(app_id, file_name):
    _COMPSs.get_directory(app_id, file_name)  # noqa


def barrier(app_id, no_more_tasks):
    _COMPSs.barrier(app_id, no_more_tasks)  # noqa


def barrier_group(app_id, group_name):
    _COMPSs.barrier_group(app_id, group_name)  # noqa


def open_task_group(group_name, implicit_barrier, mode):
    _COMPSs.open_task_group(group_name, implicit_barrier, mode)  # noqa


def close_task_group(group_name, mode):
    _COMPSs.close_task_group(group_name, mode)  # noqa


def get_logging_path():
    return _COMPSs.get_logging_path()  # noqa


def get_number_of_resources(app_id):
    _COMPSs.get_number_of_resources(app_id)  # noqa


def request_resources(app_id, num_resources, group_name):
    _COMPSs.request_resources(app_id, num_resources, group_name)  # noqa


def free_resources(app_id, num_resources, group_name):
    _COMPSs.free_resources(app_id, num_resources, group_name)  # noqa


def register_core_element(ce_signature,
                          impl_signature,
                          impl_constraints_str,
                          impl_type,
                          impl_io,
                          impl_type_args):
    _COMPSs.register_core_element(ce_signature,    # noqa
                                  impl_signature,
                                  impl_constraints_str,
                                  impl_type,
                                  impl_io,
                                  impl_type_args)


def process_task(app_id,
                 signature,
                 on_failure,
                 time_out,
                 has_priority,
                 num_nodes,
                 replicated,
                 distributed,
                 has_target,
                 num_returns,
                 values,
                 names,
                 compss_types,
                 compss_directions,
                 compss_streams,
                 compss_prefixes,
                 content_types,
                 weights,
                 keep_renames):
    _COMPSs.process_task(app_id,    # noqa
                         signature,
                         on_failure,
                         time_out,
                         has_priority,
                         num_nodes,
                         replicated,
                         distributed,
                         has_target,
                         num_returns,
                         values,
                         names,
                         compss_types,
                         compss_directions,
                         compss_streams,
                         compss_prefixes,
                         content_types,
                         weights,
                         keep_renames)
