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
PyCOMPSs Util - Environment - Configuration.

This file contains the configurator methods.
Currently, it is used by interactive.py and launch.py.
"""

import base64
import json
import os
import pathlib
import sys
import uuid as _uuid
import re
from tempfile import mkstemp

from pycompss.runtime.task.features import TASK_FEATURES
from pycompss.util.supercomputer.scs import get_log_dir
from pycompss.util.supercomputer.scs import get_log_level
from pycompss.util.supercomputer.scs import get_master_node
from pycompss.util.supercomputer.scs import get_master_port
from pycompss.util.supercomputer.scs import get_master_working_dir
from pycompss.util.supercomputer.scs import get_storage_conf
from pycompss.util.supercomputer.scs import get_tracing
from pycompss.util.supercomputer.scs import get_uuid
from pycompss.util.supercomputer.scs import get_xmls
from pycompss.util.typing_helper import typing

DEFAULT_PROJECT_PATH = "/Runtime/configuration/xml/projects/"
DEFAULT_RESOURCES_PATH = "/Runtime/configuration/xml/resources/"
DEFAULT_LOG_PATH = "/Runtime/configuration/log/"
DEFAULT_TRACING_PATH = "/Runtime/configuration/xml/tracing/"


def preload_user_code() -> bool:
    """Check if the user code has to be preloaded or not.

    Check if the user code has to be preloaded before starting the runtime
    or has been disabled by the user through environment variable.

    :return: True if preload. False otherwise.
    """
    environment_variable_load = "COMPSS_LOAD_SOURCE"
    if environment_variable_load not in os.environ:
        return True
    if (
        environment_variable_load in os.environ
        and os.environ[environment_variable_load] != "false"
    ):
        return True
    return False


def export_current_flags(all_vars: dict) -> None:
    """Export current flags to PYCOMPSS_CURRENT_FLAGS environment variable.

    Save all vars in global environment variable current flags so that
    events.py can restart the notebook with the same flags.
    Removes b" and " to avoid issues with javascript.

    :param all_vars: Dictionary containing all flags.
    :return: None.
    """
    environment_variable_current_flags = "PYCOMPSS_CURRENT_FLAGS"
    all_flags = str(base64.b64encode(json.dumps(all_vars).encode()))[2:-1]
    os.environ[environment_variable_current_flags] = all_flags


def prepare_environment(
    interactive: bool,
    o_c: bool,
    storage_impl: str,
    app: str,
    debug: bool,
    mpi_worker: bool,
) -> dict:
    """Set up the environment variable and retrieve their content.

    :param interactive: True | False If the environment is interactive or not.
    :param o_c: Object conversion to string.
    :param storage_impl: Storage implementation.
    :param app: Application name.
    :param debug: True | False If debug is enabled.
    :param mpi_worker: True | False if mpi worker is enabled.
    :return: Dictionary which contains the compss_home, pythonpath, classpath,
             ld_library_path, cp, extrae_home, extrae_lib and file_name values.
    """
    launch_path = os.path.dirname(os.path.realpath(__file__))

    if "COMPSS_HOME" in os.environ:
        compss_home = os.environ["COMPSS_HOME"]
    else:
        compss_home = ""
        if interactive:
            # compss_home = launch_path without the last 6 folders:
            # (Bindings/python/version/pycompss/util/environment)
            compss_home = os.path.sep.join(launch_path.split(os.path.sep)[:-6])
        os.environ["COMPSS_HOME"] = compss_home

    # Grab the existing PYTHONPATH, CLASSPATH and LD_LIBRARY_PATH environment
    # variables values
    pythonpath = os.getcwd() + ":."
    if "PYTHONPATH" in os.environ:
        pythonpath += ":" + os.environ["PYTHONPATH"]
    classpath = ""
    if "CLASSPATH" in os.environ:
        classpath = os.environ["CLASSPATH"]
    ld_library_path = ""
    if "LD_LIBRARY_PATH" in os.environ:
        ld_library_path = os.environ["LD_LIBRARY_PATH"]

    # Enable/Disable object to string conversion
    # set cross-module variable
    TASK_FEATURES.set_object_conversion(o_c)

    # Get the filename and its path.
    file_name = os.path.splitext(os.path.basename(app))[0]
    cp = os.path.dirname(app)
    if interactive:
        # Rename file_name and cp
        file_name = "Interactive"
        cp = os.getcwd() + "/"

    # Set storage classpath
    if storage_impl != "":
        if storage_impl == "redis":
            cp = (
                cp
                + ":"
                + compss_home
                + "/Tools/storage/redis/compss-redisPSCO.jar"
            )
        else:
            cp = cp + ":" + storage_impl

    # Set extrae dependencies
    if "EXTRAE_HOME" not in os.environ:
        # It can be defined by the user or by launch_compss when running
        # in Supercomputer
        extrae_home = compss_home + "/Dependencies/extrae"
        os.environ["EXTRAE_HOME"] = extrae_home
    else:
        extrae_home = os.environ["EXTRAE_HOME"]

    extrae_lib = extrae_home + "/lib"
    os.environ["LD_LIBRARY_PATH"] = extrae_lib + ":" + ld_library_path
    os.environ["EXTRAE_USE_POSIX_CLOCK"] = "0"

    control_binding_commons_debug(debug)

    env_vars = {
        "compss_home": compss_home,
        "pythonpath": pythonpath,
        "classpath": classpath,
        "ld_library_path": ld_library_path,
        "cp": cp,
        "extrae_home": extrae_home,
        "extrae_lib": extrae_lib,
        "file_name": file_name,
        "mpi_worker": mpi_worker,
    }
    return env_vars


def control_binding_commons_debug(debug: bool) -> None:
    """Enable the binding-commons debug mode.

    :returns: None.
    """
    if debug:
        # Add environment variable to get binding-commons debug information
        os.environ["COMPSS_BINDINGS_DEBUG"] = "1"


def prepare_loglevel_graph_for_monitoring(
    monitor: typing.Union[None, int], graph: bool, debug: bool, log_level: str
) -> dict:
    """Check if monitor is enabled and updates graph and log level.

    If monitor is True, then the log_level and graph are set to debug.

    :param monitor: Monitor refresh frequency. None if disabled.
    :param graph: True | False If graph is enabled or disabled.
    :param debug: True | False If debug is enabled or disabled.
    :param log_level: Defined log level.
    :return: Dictionary containing the updated monitor, graph and log_level
             values.
    """
    if monitor != -1:
        # Enable the graph if the monitoring is enabled
        graph = True
        # Set log level info
        log_level = "info"

    if debug:
        # If debug is enabled, the output is more verbose
        log_level = "debug"

    monitoring_vars = {
        "monitor": monitor,
        "graph": graph,
        "log_level": log_level,
    }
    return monitoring_vars


def updated_variables_in_sc() -> dict:
    """Retrieve the updated variable values within SCs.

    :return: Dictionary containing the updated variables (project_xml,
             resources_xml, master_name, master_port, uuid, log_dir,
             master_working_dir, storage_conf, log_level, debug and trace).
    """
    # Since the deployment in supercomputers is done through the use of
    # enqueue_compss and consequently launch_compss - the project and resources
    # xmls are already created
    project_xml, resources_xml = get_xmls()
    # It also exported some environment variables that we need here
    master_name = get_master_node()
    master_port = get_master_port()
    uuid = get_uuid()
    log_dir = get_log_dir()
    master_working_dir = get_master_working_dir()
    storage_conf = get_storage_conf()
    # Override debug considering the parameter defined in
    # pycompss_interactive_sc script and exported by launch_compss
    log_level = get_log_level()
    debug = log_level == "debug"
    # Override tracing considering the parameter defined in
    # pycompss_interactive_sc script and exported by launch_compss
    trace = get_tracing()
    updated_vars = {
        "project_xml": project_xml,
        "resources_xml": resources_xml,
        "master_name": master_name,
        "master_port": master_port,
        "uuid": uuid,
        "log_dir": log_dir,
        "master_working_dir": master_working_dir,
        "storage_conf": storage_conf,
        "log_level": log_level,
        "debug": debug,
        "trace": trace,
    }
    return updated_vars


def prepare_tracing_environment(
    trace: bool, extrae_lib: str, ld_library_path: str
) -> str:
    """Prepare the environment for tracing.

    Also retrieves the appropriate trace value for the initial configuration
    file (which is an integer).

    :param trace: [ True | False ] Tracing mode.
    :param extrae_lib: Extrae lib path.
    :param ld_library_path: LD_LIBRARY_PATH environment content
    :return: Trace mode (as integer)
    """
    if trace is True:
        ld_library_path = ld_library_path + ":" + extrae_lib
    return ld_library_path


def check_infrastructure_variables(
    project_xml: str,
    resources_xml: str,
    compss_home: str,
    app_name: str,
    file_name: str,
    external_adaptation: bool,
) -> dict:
    """Check the infrastructure variables and updates them if None.

    :param project_xml: Project xml file path (None if not defined).
    :param resources_xml: Resources xml file path (None if not defined).
    :param compss_home: Compss home path.
    :param app_name: Application name (if None, it changes it with filename).
    :param file_name: Application file name.
    :param external_adaptation: External adaptation.
    :return: Updated variables (project_xml, resources_xml, app_name,
                                external_adaptation, major_version,
                                python_interpreter, python_version and
                                python_virtual_environment).
    """
    if project_xml == "":
        project_xml = (
            compss_home + DEFAULT_PROJECT_PATH + "default_project.xml"
        )
    if resources_xml == "":
        resources_xml = (
            compss_home + DEFAULT_RESOURCES_PATH + "default_resources.xml"
        )
    app_name = file_name if app_name == "" else app_name
    external_adaptation_str = "true" if external_adaptation else "false"
    major_version = str(sys.version_info[0])
    python_interpreter = "python" + major_version
    python_version = major_version
    # Check if running within a virtual environment
    if "VIRTUAL_ENV" in os.environ:
        python_virtual_environment = os.environ["VIRTUAL_ENV"]
    elif "CONDA_DEFAULT_ENV" in os.environ:
        python_virtual_environment = os.environ["CONDA_DEFAULT_ENV"]
    else:
        python_virtual_environment = "null"
    inf_vars = {
        "project_xml": project_xml,
        "resources_xml": resources_xml,
        "app_name": app_name,
        "external_adaptation": external_adaptation_str,
        "major_version": major_version,
        "python_interpreter": python_interpreter,
        "python_version": python_version,
        "python_virtual_environment": python_virtual_environment,
    }
    return inf_vars


def create_init_config_file(
    compss_home: str,
    debug: bool,
    log_level: str,
    project_xml: str,
    resources_xml: str,
    summary: bool,
    task_execution: str,
    storage_conf: str,
    streaming_backend: str,
    streaming_master_name: str,
    streaming_master_port: str,
    task_count: int,
    app_name: str,
    uuid: str,
    log_dir: str,
    master_working_dir: str,
    graph: bool,
    monitor: int,
    trace: int,
    extrae_cfg: str,
    extrae_final_directory: str,
    comm: str,
    conn: str,
    master_name: str,
    master_port: str,
    scheduler: str,
    cp: str,
    classpath: str,
    ld_library_path: str,
    pythonpath: str,
    jvm_workers: str,
    cpu_affinity: str,
    gpu_affinity: str,
    fpga_affinity: str,
    fpga_reprogram: str,
    profile_input: str,
    profile_output: str,
    scheduler_config: str,
    external_adaptation: str,
    python_interpreter: str,
    python_version: str,
    python_virtual_environment: str,
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
    **kwargs: typing.Any,
) -> None:
    """Create the initialization file for the runtime start (java opts file).

    :param compss_home: <String> COMPSs installation path.
    :param debug:  <Boolean> Enable/Disable debugging
                   (True|False) (overrides log_level).
    :param log_level: <String> Define the log level
                      ("off" (default) | "info" | "debug").
    :param project_xml: <String> Specific project.xml path.
    :param resources_xml: <String> Specific resources.xml path.
    :param summary: <Boolean> Enable/Disable summary (True|False).
    :param task_execution: <String> Who performs the task execution
                           (normally "compss").
    :param storage_conf: None|<String> Storage configuration file path.
    :param streaming_backend: Streaming backend (default: None => "null").
    :param streaming_master_name: Streaming master name
                                  (default: None => "null").
    :param streaming_master_port: Streaming master port
                                  (default: None => "null").
    :param task_count: <Integer> Number of tasks
                       (for structure initialization purposes).
    :param app_name: <String> Application name.
    :param uuid: None|<String> Application UUID.
    :param log_dir: None|<String> Log path.
    :param master_working_dir: None|<String> Master working path.
    :param graph: <Boolean> Enable/Disable graph generation.
    :param monitor: None|<Integer> Disable/Frequency of the monitor.
    :param trace: <Boolean> Enable/Disable trace generation.
    :param extrae_cfg: None|<String> Default extrae configuration/user
                       specific extrae configuration.
    :param extrae_final_directory: None|<String>
                                   Default extrae final directory.
    :param comm: <String> GAT/NIO.
    :param conn: <String> Connector
                 (normally: es.bsc.compss.connectors.DefaultSSHConnector).
    :param master_name: <String> Master node name.
    :param master_port: <String> Master node port.
    :param scheduler: <String> Scheduler (normally:
                  es.bsc.compss.scheduler.lookahead.locality.LocalityTS).
    :param cp: <String>  Application path.
    :param classpath: <String> CLASSPATH environment variable contents.
    :param ld_library_path: <String> LD_LIBRARY_PATH environment
                            variable contents.
    :param pythonpath: <String> PYTHONPATH environment variable contents.
    :param jvm_workers: <String> Worker"s jvm configuration
                        (example: "-Xms1024m,-Xmx1024m,-Xmn400m").
    :param cpu_affinity: <String> CPU affinity (default: automatic).
    :param gpu_affinity: <String> GPU affinity (default: automatic).
    :param fpga_affinity: <String> FPGA affinity (default: automatic).
    :param fpga_reprogram: <String> FPGA reprogram command (default: "").
    :param profile_input: <String> profiling input.
    :param profile_output: <String> profiling output.
    :param scheduler_config: <String> Path to the file which contains the
                             scheduler configuration..
    :param external_adaptation: <String> Enable external adaptation.
                                This option disables the Resource Optimizer.
    :param python_interpreter: <String> Python interpreter.
    :param python_version: <String> Python interpreter version.
    :param python_virtual_environment: <String> Python virtual env path.
    :param propagate_virtual_environment: <Boolean> Propagate python virtual
                                          environment to workers.
    :param mpi_worker: Use the MPI worker [ True | False ] (default: False).
    :param worker_cache: Use the worker cache [ True | int(size) | False ]
                         (default: False).
    :param shutdown_in_node_failure: Shutdown in node failure [ True | False]
                                     (default: False).
    :param io_executors: <Integer> Number of IO executors.
    :param env_script: <String> Environment script to be sourced in workers.
    :param reuse_on_block: Reuse on block [ True | False] (default: True).
    :param nested_enabled: Nested enabled [ True | False] (default: False).
    :param tracing_task_dependencies: Include task dependencies in trace
                                      [ True | False] (default: False).
    :param trace_label: <String> Add trace label.
    :param extrae_cfg_python: <String> Extrae configuration file for the
                              workers.
    :param wcl: <Integer> Wall clock limit. Stops the runtime if reached.
                0 means forever.
    :param cache_profiler: Use the cache profiler [ True | False ]
                           (default: False).
    :param ear: Use the EAR [ True | False ] (default: False).
    :param kwargs: Any other parameter.
    :return: None.
    """
    temp_fd, temp_path = mkstemp()
    with open(temp_path, "w") as jvm_options_file:
        # JVM GENERAL OPTIONS
        jvm_options_file.write("-XX:+PerfDisableSharedMem\n")
        jvm_options_file.write("-XX:-UsePerfData\n")
        jvm_options_file.write("-XX:+UseG1GC\n")
        jvm_options_file.write("-XX:+UseThreadPriorities\n")
        jvm_options_file.write("-XX:ThreadPriorityPolicy=0\n")
        jvm_options_file.write(
            "-javaagent:" + compss_home + "/Runtime/compss-engine.jar\n"
        )
        jvm_options_file.write("-Dcompss.to.file=false\n")
        jvm_options_file.write("-Dcompss.appName=" + app_name + "\n")

        if data_provenance:
            jvm_options_file.write("-Dcompss.data_provenance=true\n")
        else:
            jvm_options_file.write("-Dcompss.data_provenance=false\n")

        if uuid == "":
            my_uuid = str(_uuid.uuid4())
        else:
            my_uuid = uuid
        jvm_options_file.write("-Dcompss.uuid=" + my_uuid + "\n")

        if shutdown_in_node_failure:
            jvm_options_file.write("-Dcompss.shutdown_in_node_failure=true\n")
        else:
            jvm_options_file.write("-Dcompss.shutdown_in_node_failure=false\n")

        if log_dir == "":
            log_dir = __create_log_dir()
        if master_working_dir == "":
            master_working_dir = __create_master_working_dir(log_dir)
        jvm_options_file.write(
            "-Dcompss.master.workingDir=" + master_working_dir + "\n"
        )
        jvm_options_file.write("-Dcompss.log.dir=" + log_dir + "\n")

        conf_file_key = "-Dlog4j.configurationFile="
        log_off = True
        if debug or log_level == "debug":
            jvm_options_file.write(
                conf_file_key
                + compss_home
                + DEFAULT_LOG_PATH
                + "COMPSsMaster-log4j.debug\n"
            )  # DEBUG
            log_off = False
        elif monitor is not None or log_level == "info":
            jvm_options_file.write(
                conf_file_key
                + compss_home
                + DEFAULT_LOG_PATH
                + "COMPSsMaster-log4j.info\n"
            )  # INFO
            log_off = False
        if log_off:
            jvm_options_file.write(
                conf_file_key
                + compss_home
                + DEFAULT_LOG_PATH
                + "COMPSsMaster-log4j\n"
            )  # NO DEBUG

        if graph:
            jvm_options_file.write("-Dcompss.graph=true\n")
        else:
            jvm_options_file.write("-Dcompss.graph=false\n")

        if monitor == -1:
            jvm_options_file.write("-Dcompss.monitor=0\n")
        else:
            jvm_options_file.write("-Dcompss.monitor=" + str(monitor) + "\n")

        if summary:
            jvm_options_file.write("-Dcompss.summary=true\n")
        else:
            jvm_options_file.write("-Dcompss.summary=false\n")

        jvm_options_file.write(
            "-Dcompss.worker.cp="
            + cp
            + ":"
            + compss_home
            + "/Runtime/compss-engine.jar:"
            + classpath
            + "\n"
        )
        jvm_options_file.write("-Dcompss.worker.appdir=" + cp + "\n")
        jvm_options_file.write(
            "-Dcompss.worker.jvm_opts=" + jvm_workers + "\n"
        )
        jvm_options_file.write(
            "-Dcompss.worker.cpu_affinity=" + cpu_affinity + "\n"
        )
        jvm_options_file.write(
            "-Dcompss.worker.gpu_affinity=" + gpu_affinity + "\n"
        )
        jvm_options_file.write(
            "-Dcompss.worker.fpga_affinity=" + fpga_affinity + "\n"
        )
        jvm_options_file.write(
            "-Dcompss.worker.fpga_reprogram=" + fpga_reprogram + "\n"
        )
        jvm_options_file.write(
            "-Dcompss.worker.io_executors=" + str(io_executors) + "\n"
        )
        jvm_options_file.write(
            "-Dcompss.worker.env_script=" + env_script + "\n"
        )

        if comm == "GAT":
            gat = "-Dcompss.comm=es.bsc.compss.gat.master.GATAdaptor"
            jvm_options_file.write(gat + "\n")
        elif comm == "NIO":
            nio = "-Dcompss.comm=es.bsc.compss.nio.master.NIOAdaptor"
            jvm_options_file.write(nio + "\n")
        elif comm == "GOS":
            gos = "-Dcompss.comm=es.bsc.compss.gos.master.GOSAdaptor"
            jvm_options_file.write(gos + "\n")
        else:
            jvm_options_file.write("-Dcompss.comm=" + comm + "\n")

        jvm_options_file.write("-Dcompss.masterName=" + master_name + "\n")
        jvm_options_file.write("-Dcompss.masterPort=" + master_port + "\n")

        jvm_options_file.write(
            "-Dgat.adaptor.path="
            + compss_home
            + "/Dependencies/JAVA_GAT/lib/adaptors\n"
        )
        if debug:
            jvm_options_file.write("-Dgat.debug=true\n")
        else:
            jvm_options_file.write("-Dgat.debug=false\n")
        jvm_options_file.write("-Dgat.broker.adaptor=sshtrilead\n")
        jvm_options_file.write("-Dgat.file.adaptor=sshtrilead\n")

        if reuse_on_block:
            jvm_options_file.write("-Dcompss.execution.reuseOnBlock=true\n")
        else:
            jvm_options_file.write("-Dcompss.execution.reuseOnBlock=true\n")

        if nested_enabled:
            jvm_options_file.write("-Dcompss.execution.nested.enabled=true\n")
        else:
            jvm_options_file.write("-Dcompss.execution.nested.enabled=false\n")

        jvm_options_file.write("-Dcompss.scheduler=" + scheduler + "\n")
        jvm_options_file.write(
            "-Dcompss.scheduler.config=" + scheduler_config + "\n"
        )
        jvm_options_file.write(
            "-Dcompss.profile.input=" + profile_input + "\n"
        )
        jvm_options_file.write(
            "-Dcompss.profile.output=" + profile_output + "\n"
        )

        jvm_options_file.write("-Dcompss.project.file=" + project_xml + "\n")
        jvm_options_file.write(
            "-Dcompss.resources.file=" + resources_xml + "\n"
        )
        jvm_options_file.write(
            "-Dcompss.project.schema="
            + compss_home
            + DEFAULT_PROJECT_PATH
            + "project_schema.xsd\n"
        )
        jvm_options_file.write(
            "-Dcompss.resources.schema="
            + compss_home
            + DEFAULT_RESOURCES_PATH
            + "resources_schema.xsd\n"
        )

        jvm_options_file.write("-Dcompss.conn=" + conn + "\n")
        jvm_options_file.write(
            "-Dcompss.external.adaptation=" + external_adaptation + "\n"
        )

        jvm_options_file.write(
            "-Dcompss.checkpoint.policy=" + str(checkpoint_policy) + "\n"
        )
        jvm_options_file.write(
            "-Dcompss.checkpoint.params=" + str(checkpoint_params) + "\n"
        )
        jvm_options_file.write(
            "-Dcompss.checkpoint.folder=" + str(checkpoint_folder) + "\n"
        )

        jvm_options_file.write("-Dcompss.lang=python\n")

        jvm_options_file.write("-Dcompss.core.count=" + str(task_count) + "\n")

        jvm_options_file.write(
            "-Djava.class.path="
            + cp
            + ":"
            + compss_home
            + "/Runtime/compss-engine.jar:"
            + classpath
            + "\n"
        )
        jvm_options_file.write("-Djava.library.path=" + ld_library_path + "\n")

        # SPECIFIC JVM OPTIONS FOR PYTHON
        jvm_options_file.write(
            "-Dcompss.worker.pythonpath=" + cp + ":" + pythonpath + "\n"
        )
        jvm_options_file.write(
            "-Dcompss.python.interpreter=" + python_interpreter + "\n"
        )
        jvm_options_file.write(
            "-Dcompss.python.version=" + python_version + "\n"
        )
        jvm_options_file.write(
            "-Dcompss.python.virtualenvironment="
            + python_virtual_environment
            + "\n"
        )
        virtualenv_prefix = "-Dcompss.python.propagate_virtualenvironment="
        if propagate_virtual_environment:
            jvm_options_file.write(virtualenv_prefix + "true\n")
        else:
            jvm_options_file.write(virtualenv_prefix + "false\n")
        if mpi_worker:
            jvm_options_file.write("-Dcompss.python.mpi_worker=true\n")
        else:
            jvm_options_file.write("-Dcompss.python.mpi_worker=false\n")
        if worker_cache:
            jvm_options_file.write("-Dcompss.python.worker_cache=true\n")
        else:
            jvm_options_file.write("-Dcompss.python.worker_cache=false\n")

        if cache_profiler:
            jvm_options_file.write(
                "-Dcompss.python.cache_profiler="
                + str(worker_cache).lower()
                + "\n"
            )
        else:
            jvm_options_file.write("-Dcompss.python.cache_profiler=false\n")

        # SPECIFIC FOR STREAMING
        if streaming_backend == "":
            jvm_options_file.write("-Dcompss.streaming=NONE\n")
        else:
            jvm_options_file.write(
                "-Dcompss.streaming=" + str(streaming_backend) + "\n"
            )
        if streaming_master_name == "":
            jvm_options_file.write("-Dcompss.streaming.masterName=null\n")
        else:
            jvm_options_file.write(
                "-Dcompss.streaming.masterName="
                + str(streaming_master_name)
                + "\n"
            )
        if streaming_master_port == "":
            jvm_options_file.write("-Dcompss.streaming.masterPort=null\n")
        else:
            jvm_options_file.write(
                "-Dcompss.streaming.masterPort="
                + str(streaming_master_port)
                + "\n"
            )

        # STORAGE SPECIFIC
        jvm_options_file.write(
            "-Dcompss.task.execution=" + task_execution + "\n"
        )
        if storage_conf == "":
            jvm_options_file.write("-Dcompss.storage.conf=null\n")
        else:
            jvm_options_file.write(
                "-Dcompss.storage.conf=" + storage_conf + "\n"
            )

        # TOOLS SPECIFIC
        if extrae_cfg == "":
            extrae_xml_name = "extrae_basic.xml"
            extrae_xml_path = (
                compss_home + DEFAULT_TRACING_PATH + extrae_xml_name
            )
            extrae_cfg = "null"
        else:
            extrae_xml_name = os.path.basename(extrae_cfg)
            extrae_xml_path = extrae_cfg
        if trace:
            jvm_options_file.write("-Dcompss.tracing=true\n")
            # Process extrae_xml_path
            extrae_xml_final_path_dir = os.path.join(log_dir, "cfgfiles")
            pathlib.Path(extrae_xml_final_path_dir).mkdir(
                parents=False, exist_ok=True
            )
            extrae_trace_path = os.path.join(log_dir, "trace")
            extrae_xml_final_path = os.path.join(
                extrae_xml_final_path_dir, extrae_xml_name
            )
            got_extrae_final_directory = __process_extrae_file(
                extrae_xml_path,
                extrae_xml_final_path,
                extrae_trace_path,
                compss_home,
            )
            if extrae_cfg == "null":
                os.environ["EXTRAE_CONFIG_FILE"] = extrae_xml_final_path
        else:
            # Any other case: deactivated
            jvm_options_file.write("-Dcompss.tracing=false" + "\n")
            got_extrae_final_directory = "null"
        if tracing_task_dependencies:
            jvm_options_file.write("-Dcompss.tracing.task.dependencies=true\n")
        else:
            jvm_options_file.write(
                "-Dcompss.tracing.task.dependencies=false\n"
            )
        if extrae_final_directory == "":
            jvm_options_file.write(
                "-Dcompss.extrae.working_dir="
                + got_extrae_final_directory
                + "\n"
            )
        else:
            jvm_options_file.write(
                "-Dcompss.extrae.working_dir=" + extrae_final_directory + "\n"
            )
        jvm_options_file.write("-Dcompss.extrae.file=" + extrae_cfg + "\n")
        if extrae_cfg_python == "":
            jvm_options_file.write("-Dcompss.extrae.file.python=null\n")
        else:
            jvm_options_file.write(
                "-Dcompss.extrae.file.python=" + str(extrae_cfg_python) + "\n"
            )
        if trace_label == "":
            jvm_options_file.write("-Dcompss.trace.label=None\n")
        else:
            jvm_options_file.write(
                "-Dcompss.trace.label=" + str(trace_label) + "\n"
            )

        # WALLCLOCK LIMIT
        jvm_options_file.write("-Dcompss.wcl=" + str(wcl) + "\n")

        # EAR
        if ear:
            jvm_options_file.write("-Dcompss.ear=true\n")
        else:
            jvm_options_file.write("-Dcompss.ear=false\n")

        # Uncomment for debugging purposes
        # jvm_options_file.write("-Xcheck:jni\n")
        # jvm_options_file.write("-verbose:jni\n")

    # Close the temporary file
    os.close(temp_fd)
    os.environ["JVM_OPTIONS_FILE"] = temp_path

    # Uncomment if you want to check the configuration file path:
    # print("JVM_OPTIONS_FILE: %s" % temp_path)


def __process_extrae_file(
    extrae_xml_path: str,
    extrae_xml_final_path: str,
    extrae_trace_path: str,
    compss_home: str,
) -> str:
    """Sed and grep extrae file.

    :param extrae_xml_path: Source extrae xml file path.
    :param extrae_xml_final_path: Destination extrae xml file path.
    :param extrae_trace_path: Extrae trace working directory
    :param compss_home: COMPSs home directory
    :return: final_directory from extrae_xml_path
    """
    got_extrae_final_directory = "null"
    with open(extrae_xml_path, "r") as extrae_xml_fd:
        extrae_xml_data = extrae_xml_fd.readlines()
    # Look for final-directory
    for i, line in enumerate(extrae_xml_data):
        if "{{EXTRAE_HOME}}" in line:
            # Sed with real path
            extrae_home = str(
                os.path.join(compss_home, "Dependencies", "extrae")
            )
            extrae_xml_data[i] = line.replace("{{EXTRAE_HOME}}", extrae_home)
        if "{{TRACE_OUTPUT_DIR}}" in line:
            # Sed with real path
            extrae_xml_data[i] = line.replace(
                "{{TRACE_OUTPUT_DIR}}", extrae_trace_path
            )
        if "final-directory" in line:
            # Extract final-directory
            key = '<final-directory enabled="(.*)">(.*)</final-directory>'
            found = re.search(key, extrae_xml_data[i]).group(2)  # type: ignore
            got_extrae_final_directory = found
    with open(extrae_xml_final_path, "w") as extrae_xml_fd:
        extrae_xml_fd.writelines(extrae_xml_data)
    return got_extrae_final_directory


def __create_log_dir() -> str:
    """Create log directory.

    :return: Updated log directory.
    """
    home_folder = str(pathlib.Path.home())
    compss_log_folder = os.path.join(home_folder, ".COMPSs")
    pathlib.Path(compss_log_folder).mkdir(parents=False, exist_ok=True)
    log_folder_prefix = "Interactive_"
    sub_folders = list(os.walk(compss_log_folder))[0][1]
    existing_folders = []
    for folder in sub_folders:
        if folder.startswith(log_folder_prefix):
            existing_folders.append(folder)
    next_int = len(existing_folders) + 1
    next_subfolder = log_folder_prefix + f"{next_int:02d}"
    log_dir = os.path.join(compss_log_folder, next_subfolder)
    pathlib.Path(log_dir).mkdir(parents=False, exist_ok=True)
    return log_dir


def __create_master_working_dir(log_dir: str) -> str:
    """Create master working directory (log_dir/tmpFiles).

    :return: Updated master working directory.
    """
    master_working_dir = os.path.join(log_dir, "tmpFiles")
    pathlib.Path(master_working_dir).mkdir(parents=False, exist_ok=True)
    return master_working_dir
