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
PyCOMPSs Binding - Interactive API.

Provides the functions for the use of PyCOMPSs interactively.
"""

import logging
import os
import sys
import time

import pycompss.util.interactive.helpers as interactive_helpers
from pycompss.util.context import CONTEXT
from pycompss.runtime.binding import get_log_path
from pycompss.runtime.commons import CONSTANTS
from pycompss.runtime.commons import GLOBALS
from pycompss.runtime.start.initialization import LAUNCH_STATUS
from pycompss.runtime.start.interactive_initialization import EXTRA_LAUNCH_STATUS
from pycompss.runtime.management.classes import Future
from pycompss.runtime.management.object_tracker import OT

# Streaming imports
from pycompss.streams.environment import init_streaming
from pycompss.streams.environment import stop_streaming
from pycompss.util.environment.configuration import (
    export_current_flags,
    prepare_environment,
    prepare_loglevel_graph_for_monitoring,
    updated_variables_in_sc,
    prepare_tracing_environment,
    check_infrastructure_variables,
    create_init_config_file,
)
from pycompss.util.interactive.events import release_event_manager
from pycompss.util.interactive.events import setup_event_manager
from pycompss.util.interactive.flags import check_flags
from pycompss.util.interactive.flags import print_flag_issues
from pycompss.util.interactive.graphs import show_graph
from pycompss.util.interactive.outwatcher import STDW
from pycompss.util.interactive.state import check_monitoring_file
from pycompss.util.interactive.state import show_resources_status
from pycompss.util.interactive.state import show_statistics
from pycompss.util.interactive.state import show_tasks_info
from pycompss.util.interactive.state import show_tasks_status
from pycompss.util.interactive.utils import parameters_to_dict
from pycompss.util.logger.helpers import get_logging_cfg_file
from pycompss.util.logger.helpers import init_logging
from pycompss.util.process.manager import initialize_multiprocessing

# Storage imports
from pycompss.util.storages.persistent import master_init_storage
from pycompss.util.storages.persistent import master_stop_storage

# Tracing imports
from pycompss.util.tracing.helpers import emit_manual_event
from pycompss.util.tracing.types_events_master import TRACING_MASTER
from pycompss.util.typing_helper import typing


# Initialize multiprocessing
initialize_multiprocessing()


def start(  # pylint: disable=too-many-arguments, too-many-locals
    log_level: str = "off",
    debug: bool = False,
    o_c: bool = False,
    graph: bool = False,
    trace: bool = False,
    monitor: int = -1,
    project_xml: str = "",
    resources_xml: str = "",
    summary: bool = False,
    task_execution: str = "compss",
    storage_impl: str = "",
    storage_conf: str = "",
    streaming_backend: str = "",
    streaming_master_name: str = "",
    streaming_master_port: str = "",
    task_count: int = 50,
    app_name: str = CONSTANTS.interactive_file_name,
    uuid: str = "",
    base_log_dir: str = "",
    specific_log_dir: str = "",
    extrae_cfg: str = "",
    extrae_final_directory: str = "",
    comm: str = "NIO",
    conn: str = CONSTANTS.default_conn,
    master_name: str = "",
    master_port: str = "",
    scheduler: str = CONSTANTS.default_sched,
    jvm_workers: str = CONSTANTS.default_jvm_workers,
    cpu_affinity: str = "automatic",
    gpu_affinity: str = "automatic",
    fpga_affinity: str = "automatic",
    fpga_reprogram: str = "",
    profile_input: str = "",
    profile_output: str = "",
    scheduler_config: str = "",
    external_adaptation: bool = False,
    propagate_virtual_environment: bool = True,
    mpi_worker: bool = False,
    worker_cache: typing.Union[bool, str] = False,
    shutdown_in_node_failure=False,
    io_executors: int = 0,
    env_script: str = "",
    reuse_on_block: bool = True,
    nested_enabled: bool = False,
    tracing_task_dependencies: bool = False,
    trace_label: str = "",
    extrae_cfg_python: str = "",
    wcl: int = 0,
    cache_profiler: bool = False,
    verbose: bool = False,
    disable_external: bool = False,
) -> None:
    """Start the runtime in interactive mode.

    :param log_level: Logging level [ "trace"|"debug"|"info"|"api"|"off" ]
                      (default: "off")
    :param debug: Debug mode [ True | False ]
                  (default: False) (overrides log-level)
    :param o_c: Objects to string conversion [ True|False ]
                (default: False)
    :param graph: Generate graph [ True|False ]
                  (default: False)
    :param trace: Generate trace [ True | False ]
                  (default: False)
    :param monitor: Monitor refresh rate
                    (default: None)
    :param project_xml: Project xml file path
                        (default: None)
    :param resources_xml: Resources xml file path
                          (default: None)
    :param summary: Execution summary [ True | False ]
                    (default: False)
    :param task_execution: Task execution
                           (default: "compss")
    :param storage_impl: Storage implementation path
                         (default: None)
    :param storage_conf: Storage configuration file path
                         (default: None)
    :param streaming_backend: Streaming backend
                              (default: None)
    :param streaming_master_name: Streaming master name
                                  (default: None)
    :param streaming_master_port: Streaming master port
                                  (default: None)
    :param task_count: Task count
                       (default: 50)
    :param app_name: Application name
                     (default: CONSTANTS.interactive_file_name)
    :param uuid: UUId
                 (default: None)
    :param base_log_dir: Base logging directory
                         (default: None)
    :param specific_log_dir: Specific logging directory
                             (default: None)
    :param extrae_cfg: Extrae configuration file path
                       (default: None)
    :param extrae_final_directory: Extrae final directory (default: "")
    :param comm: Communication library
                 (default: NIO)
    :param conn: Connector
                 (default: DefaultSSHConnector)
    :param master_name: Master Name
                        (default: "")
    :param master_port: Master port
                        (default: "")
    :param scheduler: Scheduler (see runcompss)
                      (default: es.bsc.compss.scheduler.lookahead.locality.LocalityTS)  # noqa: E501
    :param jvm_workers: Java VM parameters
                        (default: "-Xms1024m,-Xmx1024m,-Xmn400m")
    :param cpu_affinity: CPU Core affinity
                         (default: "automatic")
    :param gpu_affinity: GPU affinity
                         (default: "automatic")
    :param fpga_affinity: FPGA affinity
                          (default: "automatic")
    :param fpga_reprogram: FPGA repogram command
                           (default: "")
    :param profile_input: Input profile
                          (default: "")
    :param profile_output: Output profile
                           (default: "")
    :param scheduler_config: Scheduler configuration
                             (default: "")
    :param external_adaptation: External adaptation [ True|False ]
                                (default: False)
    :param propagate_virtual_environment: Propagate virtual environment [ True|False ]  # noqa: E501
                                          (default: False)
    :param mpi_worker: Use the MPI worker [ True|False ]
                       (default: False)
    :param worker_cache: Use the worker cache [ True | int(size) | False]
                         (default: False)
    :param shutdown_in_node_failure: Shutdown in node failure [ True | False]
                                     (default: False)
    :param io_executors: <Integer> Number of IO executors
    :param env_script: <String> Environment script to be sourced in workers
    :param reuse_on_block: Reuse on block [ True | False]
                           (default: True)
    :param nested_enabled: Nested enabled [ True | False]
                           (default: True)
    :param tracing_task_dependencies: Include task dependencies in trace
                                      [ True | False] (default: False)
    :param trace_label: <String> Add trace label
    :param extrae_cfg_python: <String> Extrae configuration file for the
                              workers
    :param wcl: <Integer> Wall clock limit. Stops the runtime if reached.
                0 means forever.
    :param cache_profiler: Use the cache profiler [ True | False]
                         (default: False)
    :param verbose: Verbose mode [ True|False ]
                    (default: False)
    :param disable_external: To avoid to load compss in external process [ True | False ]
                             Necessary in scenarios like pytest which fails with
                             multiprocessing. It also disables the outwatcher
                             since pytest also captures stdout and stderr.
                             (default: False)
    :return: None
    """
    if CONTEXT.in_pycompss():
        print("The runtime is already running")
        return None

    EXTRA_LAUNCH_STATUS.set_graphing(graph)
    EXTRA_LAUNCH_STATUS.set_disable_external(disable_external)

    temp_app_filename = "".join(
        (
            os.path.join(os.getcwd(), CONSTANTS.interactive_file_name),
            "_",
            str(time.strftime("%d%m%y_%H%M%S")),
            ".py",
        )
    )
    LAUNCH_STATUS.set_app_path(temp_app_filename)

    interactive_helpers.DEBUG = debug
    if debug:
        log_level = "debug"

    __show_flower__()

    # Let the Python binding know we are at master
    CONTEXT.set_master()
    # Then we can import the appropriate start and stop functions from the API
    from pycompss.api.api import compss_start  # pylint: disable=import-outside-toplevel

    ##############################################################
    # INITIALIZATION
    ##############################################################

    # Initial dictionary with the user defined parameters
    all_vars = parameters_to_dict(
        log_level,
        debug,
        o_c,
        graph,
        trace,
        monitor,
        project_xml,
        resources_xml,
        summary,
        task_execution,
        storage_impl,
        storage_conf,
        streaming_backend,
        streaming_master_name,
        streaming_master_port,
        task_count,
        app_name,
        uuid,
        base_log_dir,
        specific_log_dir,
        extrae_cfg,
        extrae_final_directory,
        comm,
        conn,
        master_name,
        master_port,
        scheduler,
        jvm_workers,
        cpu_affinity,
        gpu_affinity,
        fpga_affinity,
        fpga_reprogram,
        profile_input,
        profile_output,
        scheduler_config,
        external_adaptation,
        propagate_virtual_environment,
        mpi_worker,
        worker_cache,
        shutdown_in_node_failure,
        io_executors,
        env_script,
        reuse_on_block,
        nested_enabled,
        tracing_task_dependencies,
        trace_label,
        extrae_cfg_python,
        wcl,
        cache_profiler,
    )
    # Save all vars in global current flags so that events.py can restart
    # the notebook with the same flags
    export_current_flags(all_vars)

    # Check the provided flags
    flags, issues = check_flags(all_vars)
    if not flags:
        print_flag_issues(issues)
        return None

    # Prepare the environment
    env_vars = prepare_environment(
        True, o_c, storage_impl, "undefined", debug, mpi_worker
    )
    all_vars.update(env_vars)

    # Update the log level and graph values if monitoring is enabled
    monitoring_vars = prepare_loglevel_graph_for_monitoring(
        monitor, graph, debug, log_level
    )
    all_vars.update(monitoring_vars)

    # Check if running in supercomputer and update the variables accordingly
    # with the defined in the launcher and exported in environment variables.
    if CONSTANTS.running_in_supercomputer:
        updated_vars = updated_variables_in_sc()
        if verbose:
            print(f"- Overridden project xml with: {updated_vars['project_xml']}")
            print(f"- Overridden resources xml with: {updated_vars['resources_xml']}")
            print(f"- Overridden master name with: {updated_vars['master_name']}")
            print(f"- Overridden master port with: {updated_vars['master_port']}")
            print(f"- Overridden uuid with: {updated_vars['uuid']}")
            print(f"- Overridden base log dir with: {updated_vars['base_log_dir']}")
            print(
                f"- Overridden specific log dir with: {updated_vars['specific_log_dir']}"
            )
            print(f"- Overridden storage conf with: {updated_vars['storage_conf']}")
            print(f"- Overridden log level with: {str(updated_vars['log_level'])}")
            print(f"- Overridden debug with: {str(updated_vars['debug'])}")
            print(f"- Overridden trace with: {str(updated_vars['trace'])}")
        all_vars.update(updated_vars)

    # Update the tracing environment if set and set the appropriate trace
    # integer value
    all_vars["ld_library_path"] = prepare_tracing_environment(
        all_vars["trace"], all_vars["extrae_lib"], all_vars["ld_library_path"]
    )

    # Update the infrastructure variables if necessary
    inf_vars = check_infrastructure_variables(
        all_vars["project_xml"],
        all_vars["resources_xml"],
        all_vars["compss_home"],
        all_vars["app_name"],
        all_vars["file_name"],
        all_vars["external_adaptation"],
    )
    all_vars.update(inf_vars)

    # With all this information, create the configuration file for the
    # runtime start
    create_init_config_file(**all_vars)

    # Start the event manager (ipython hooks)
    ipython = globals()["__builtins__"]["get_ipython"]()
    setup_event_manager(ipython)

    ##############################################################
    # RUNTIME START
    ##############################################################

    print("* - Starting COMPSs runtime...                         *")
    sys.stdout.flush()  # Force flush
    compss_start(log_level, all_vars["trace"], True, disable_external)

    log_path = get_log_path()
    GLOBALS.set_temporary_directory(log_path)
    print("* - Log path : " + log_path)

    # Setup logging
    binding_log_path = get_log_path()
    log_path = os.path.join(
        all_vars["compss_home"],
        "Bindings",
        "python",
        str(all_vars["major_version"]),
        "log",
    )
    GLOBALS.set_temporary_directory(binding_log_path)
    logging_cfg_file = get_logging_cfg_file(log_level)
    init_logging(os.path.join(log_path, logging_cfg_file), binding_log_path)
    logger = logging.getLogger("pycompss.runtime.launch")

    __print_setup__(verbose, all_vars)

    logger.debug("--- START ---")
    logger.debug("PyCOMPSs Log path: %s", log_path)

    logger.debug("Starting storage")
    persistent_storage = master_init_storage(all_vars["storage_conf"], logger)
    LAUNCH_STATUS.set_persistent_storage(persistent_storage)

    logger.debug("Starting streaming")
    streaming = init_streaming(
        all_vars["streaming_backend"],
        all_vars["streaming_master_name"],
        all_vars["streaming_master_port"],
    )
    LAUNCH_STATUS.set_streaming(streaming)

    # Start monitoring the stdout and stderr
    if not disable_external:
        STDW.start_watching()

    # MAIN EXECUTION
    # let the user write an interactive application
    print("* - PyCOMPSs Runtime started... Have fun!              *")
    print(EXTRA_LAUNCH_STATUS.get_line_separator())

    # Emit the application start event (the 0 is in the stop function)
    emit_manual_event(TRACING_MASTER.application_running_event)
    return None


def __show_flower__() -> None:
    """Show the flower and version through stdout.

    :return: None
    """
    line_separator = EXTRA_LAUNCH_STATUS.get_line_separator()
    print(line_separator)  # NOSONAR # noqa
    print(r"**************** PyCOMPSs Interactive ******************")  # NOSONAR # noqa
    print(line_separator)  # NOSONAR # noqa
    print(r"*          .-~~-.--.           ______        ______    *")  # NOSONAR # noqa
    print(r"*         :         )         |____  \      /  __  \   *")  # NOSONAR # noqa
    print(r"*   .~ ~ -.\       /.- ~~ .      __) |      | |  | |   *")  # NOSONAR # noqa
    print(r"*   >       `.   .'       <     |__  |      | |  | |   *")  # NOSONAR # noqa
    print(r"*  (         .- -.         )   ____) |   _  | |__| |   *")  # NOSONAR # noqa
    print(r"*   `- -.-~  `- -'  ~-.- -'   |______/  |_| \______/   *")  # NOSONAR # noqa
    print(r"*     (        :        )           _ _ .-:            *")  # NOSONAR # noqa
    print(r"*      ~--.    :    .--~        .-~  .-~  }            *")  # NOSONAR # noqa
    print(r"*          ~-.-^-.-~ \_      .~  .-~   .~              *")  # NOSONAR # noqa
    print(r"*                   \ \ '     \ '_ _ -~                *")  # NOSONAR # noqa
    print(r"*                    \`.\`.    //                      *")  # NOSONAR # noqa
    print(r"*           . - ~ ~-.__\`.\`-.//                       *")  # NOSONAR # noqa
    print(r"*       .-~   . - ~  }~ ~ ~-.~-.                       *")  # NOSONAR # noqa
    print(r"*     .' .-~      .-~       :/~-.~-./:                 *")  # NOSONAR # noqa
    print(r"*    /_~_ _ . - ~                 ~-.~-._              *")  # NOSONAR # noqa
    print(r"*                                     ~-.<             *")  # NOSONAR # noqa
    print(line_separator)  # NOSONAR # noqa


def __print_setup__(verbose: bool, all_vars: typing.Dict[str, typing.Any]) -> None:
    """Print the setup variables through stdout (only if verbose is True).

    :param verbose: Verbose mode [True | False]
    :param all_vars: Dictionary containing all variables.
    :return: None
    """
    line_separator = EXTRA_LAUNCH_STATUS.get_line_separator()
    logger = logging.getLogger(__name__)
    output = ""
    output += line_separator + "\n"
    output += " CONFIGURATION: \n"
    for key, value in sorted(all_vars.items()):
        output += f"  - {key} : {value} \n"
    output += line_separator
    if verbose:
        print(output)
    logger.debug(output)


def stop(sync: bool = False, _hard_stop: bool = False) -> None:
    """Runtime stop.

    :param sync: Scope variables synchronization [ True | False ]
                 (default: False)
    :param _hard_stop: Stop COMPSs when runtime has died [ True | False ].
                       (default: False)
    :return: None
    """
    logger = logging.getLogger(__name__)
    ipython = globals()["__builtins__"]["get_ipython"]()

    if not CONTEXT.in_pycompss():
        return __hard_stop__(interactive_helpers.DEBUG, sync, logger, ipython)

    from pycompss.api.api import compss_stop  # pylint: disable=import-outside-toplevel

    line_separator = EXTRA_LAUNCH_STATUS.get_line_separator()
    print(line_separator)
    print("*************** STOPPING PyCOMPSs ******************")
    print(line_separator)
    disable_external = EXTRA_LAUNCH_STATUS.get_disable_external()
    if not disable_external:
        # Wait 5 seconds to give some time to process the remaining messages
        # of the STDW and check if there is some error that could have stopped
        # the runtime before continuing.
        print("Checking if any issue happened.")
        time.sleep(5)
        messages = STDW.get_messages()
        if messages:
            for message in messages:
                sys.stderr.write("".join((message, "\n")))

    # Uncomment the following lines to see the ipython dictionary
    # in a structured way:
    #   import pprint
    #   pprint.pprint(ipython.__dict__, width=1)
    if sync and not _hard_stop:
        sync_msg = "Synchronizing all future objects left on the user scope."
        print(sync_msg)
        logger.debug(sync_msg)
        from pycompss.api.api import (  # pylint: disable=import-outside-toplevel
            compss_wait_on,
        )

        reserved_names = (
            "quit",
            "exit",
            "get_ipython",
            "ipycompss",
            "In",
            "Out",
        )
        raw_code = ipython.__dict__["user_ns"]
        for k in raw_code:
            obj_k = raw_code[k]
            if not k.startswith("_"):  # not internal objects
                if isinstance(obj_k, Future):
                    print(f"Found a future object: {str(k)}")
                    logger.debug("Found a future object: %s", str(k))
                    new_obj_k = compss_wait_on(obj_k)
                    if new_obj_k == obj_k:
                        print(f"\t - Could not retrieve object: {str(k)}")
                        logger.debug("\t - Could not retrieve object: %s", str(k))
                    else:
                        ipython.__dict__["user_ns"][k] = new_obj_k
                elif k not in reserved_names:
                    try:
                        if OT.is_pending_to_synchronize(obj_k):
                            print(f"Found an object to synchronize: {str(k)}")
                            logger.debug("Found an object to synchronize: %s", str(k))
                            ipython.__dict__["user_ns"][k] = compss_wait_on(obj_k)
                    except TypeError:
                        # Unhashable type: List - could be a collection
                        if isinstance(obj_k, list):
                            print(f"Found a list to synchronize: {str(k)}")
                            logger.debug("Found a list to synchronize: %s", str(k))
                            ipython.__dict__["user_ns"][k] = compss_wait_on(obj_k)
    else:
        print("Warning: some of the variables used with PyCOMPSs may")
        print("         have not been brought to the master.")

    # Stop streaming
    if LAUNCH_STATUS.get_streaming():
        stop_streaming()

    # Stop persistent storage
    if LAUNCH_STATUS.get_persistent_storage():
        master_stop_storage(logger)

    # Emit the 0 for the TRACING_MASTER.*_event emitted on start function.
    emit_manual_event(0)

    # Stop runtime
    compss_stop(_hard_stop=_hard_stop)

    # Cleanup events and files
    release_event_manager(ipython)
    __clean_temp_files__()

    # Stop watching stdout and stderr
    if not disable_external:
        STDW.stop_watching(clean=True)
        # Retrieve the remaining messages that could have been captured.
        last_messages = STDW.get_messages()
        if last_messages:
            for message in last_messages:
                print(message)

    # Let the Python binding know we are not at master anymore
    CONTEXT.set_out_of_scope()

    print(line_separator)
    logger.debug("--- END ---")
    # --- Execution finished ---
    return None


def __hard_stop__(
    debug: bool, sync: bool, logger: logging.Logger, ipython: typing.Any
) -> None:
    """Stop the binding securely when the runtime crashes.

    If the runtime has been stopped by any error, this method stops the
    remaining things in the binding.

    :param debug: If debugging.
    :param sync: Scope variables synchronization [ True | False ].
    :param logger: Logger where to put the logging messages.
    :param ipython: Ipython instance.
    :return: None
    """
    print("The runtime is not running.")
    # Check that everything is stopped as well:

    # Stop streaming
    if LAUNCH_STATUS.get_streaming():
        stop_streaming()

    # Stop persistent storage
    if LAUNCH_STATUS.get_persistent_storage():
        master_stop_storage(logger)

    # Clean any left object in the object tracker
    OT.clean_object_tracker()

    # Cleanup events and files
    release_event_manager(ipython)
    __clean_temp_files__()

    # Stop watching stdout and stderr
    if not EXTRA_LAUNCH_STATUS.get_disable_external():
        STDW.stop_watching(clean=not debug)
        # Retrieve the remaining messages that could have been captured.
        last_messages = STDW.get_messages()
        if last_messages:
            for message in last_messages:
                print(message)

    if sync:
        print("* Can not synchronize any future object.")


def current_task_graph(
    fit: bool = False, refresh_rate: int = 1, timeout: int = 0
) -> typing.Any:
    """Show current graph.

    :param fit: Fit to width [ True | False ] (default: False)
    :param refresh_rate: Update the current task graph every "refresh_rate"
                         seconds. Default 1 second if timeout != 0.
    :param timeout: Time during the current task graph is going to be updated.
    :return: None
    """
    if not EXTRA_LAUNCH_STATUS.get_graphing():
        print("Oops! Graph is not enabled in this execution.")
        print(
            "      Please, enable it by setting the graph flag when"
            + " starting PyCOMPSs."
        )
        return None
    return show_graph(
        log_path=GLOBALS.get_temporary_directory(),
        name="current_graph",
        fit=fit,
        refresh_rate=refresh_rate,
        timeout=timeout,
    )


def complete_task_graph(
    fit: bool = False, refresh_rate: int = 1, timeout: int = 0
) -> typing.Any:
    """Show complete graph.

    :param fit: Fit to width [ True | False ] (default: False)
    :param refresh_rate: Update the current task graph every "refresh_rate"
                         seconds. Default 1 second if timeout != 0
    :param timeout: Time during the current task graph is going to be updated.
    :return: None
    """
    if not EXTRA_LAUNCH_STATUS.get_graphing():
        print("Oops! Graph is not enabled in this execution.")
        print(
            "      Please, enable it by setting the graph flag when"
            + " starting PyCOMPSs."
        )
        return None
    return show_graph(
        log_path=GLOBALS.get_temporary_directory(),
        name="complete_graph",
        fit=fit,
        refresh_rate=refresh_rate,
        timeout=timeout,
    )


def tasks_info() -> None:
    """Show tasks info.

    :return: None
    """
    log_path = GLOBALS.get_temporary_directory()
    if check_monitoring_file(log_path):
        show_tasks_info(log_path)
    else:
        print("Oops! Monitoring is not enabled in this execution.")
        print(
            "      Please, enable it by setting the monitor flag when"
            + " starting PyCOMPSs."
        )


def tasks_status() -> None:
    """Show tasks status.

    :return: None
    """
    log_path = GLOBALS.get_temporary_directory()
    if check_monitoring_file(log_path):
        show_tasks_status(log_path)
    else:
        print("Oops! Monitoring is not enabled in this execution.")
        print(
            "      Please, enable it by setting the monitor flag when"
            + " starting PyCOMPSs."
        )


def statistics() -> None:
    """Show statistics info.

    :return: None
    """
    log_path = GLOBALS.get_temporary_directory()
    if check_monitoring_file(log_path):
        show_statistics(log_path)
    else:
        print("Oops! Monitoring is not enabled in this execution.")
        print(
            "      Please, enable it by setting the monitor flag when"
            + " starting PyCOMPSs."
        )


def resources_status() -> None:
    """Show resources status info.

    :return: None
    """
    log_path = GLOBALS.get_temporary_directory()
    if check_monitoring_file(log_path):
        show_resources_status(log_path)
    else:
        print("Oops! Monitoring is not enabled in this execution.")
        print(
            "      Please, enable it by setting the monitor flag when"
            + " starting PyCOMPSs."
        )


# ########################################################################### #
# ########################################################################### #
# ########################################################################### #


def __clean_temp_files__() -> None:
    """Remove any temporary files that may exist.

    Currently: APP_PATH, which contains the file path where all interactive
               code required by the worker is.

    :return: None
    """
    app_path = LAUNCH_STATUS.get_app_path()
    try:
        if os.path.exists(app_path):
            os.remove(app_path)
        if os.path.exists(app_path + "c"):
            os.remove(app_path + "c")
    except OSError:
        print("[ERROR] An error has occurred when cleaning temporary files.")
