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
PyCOMPSs Binding - Launch.

This file contains the __main__ method.
It is called from the runcompss/enqueue_compss/cli script with the user and
environment parameters.
"""

import argparse
import gc
import logging

# Imports
import os
import sys
import traceback

# Project imports
from pycompss.util.context import CONTEXT
from pycompss.util.context import loading_context
from pycompss.api.exceptions import COMPSsException
from pycompss.runtime.binding import get_log_path
from pycompss.runtime.binding import get_tmp_path
from pycompss.runtime.commons import CONSTANTS
from pycompss.runtime.commons import GLOBALS
from pycompss.runtime.start.initialization import LAUNCH_STATUS
from pycompss.runtime.task.features import TASK_FEATURES

# Streaming imports
from pycompss.streams.environment import init_streaming
from pycompss.streams.environment import stop_streaming
from pycompss.util.environment.configuration import (
    preload_user_code,
    export_current_flags,
    prepare_environment,
    prepare_loglevel_graph_for_monitoring,
    updated_variables_in_sc,
    prepare_tracing_environment,
    check_infrastructure_variables,
    create_init_config_file,
)
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.exceptions import SerializerException
from pycompss.util.interactive.flags import check_flags
from pycompss.util.interactive.flags import print_flag_issues
from pycompss.util.interactive.utils import parameters_to_dict
from pycompss.util.logger.helpers import clean_log_configs
from pycompss.util.logger.helpers import init_logging
from pycompss.util.logger.remittent import LOG_REMITTENT
from pycompss.util.process.manager import initialize_multiprocessing
from pycompss.util.process.preloader import preimports
from pycompss.util.process.preloader import preload_imports
from pycompss.util.storages.persistent import master_init_storage
from pycompss.util.storages.persistent import master_stop_storage

# Storage imports
from pycompss.util.storages.persistent import use_storage

# Tracing imports
from pycompss.util.tracing.helpers import EventMaster
from pycompss.util.tracing.types_events_master import TRACING_MASTER
from pycompss.util.typing_helper import typing
from pycompss.util.warnings.modules import show_optional_module_warnings

# Spend less time in gc; do this before significant computation
gc.set_threshold(150000)


def stop_all(exit_code: int) -> None:
    """Stop everything smoothly.

    :param exit_code: Exit code.
    :return: None.
    """
    from pycompss.api.api import (  # pylint: disable=import-outside-toplevel
        compss_stop,
    )

    # Stop streaming
    if LAUNCH_STATUS.get_streaming():
        stop_streaming()
    # Stop persistent storage
    if LAUNCH_STATUS.get_persistent_storage():
        master_stop_storage(LAUNCH_STATUS.get_logger())
    compss_stop(exit_code)
    sys.stdout.flush()
    sys.stderr.flush()
    sys.exit(exit_code)


def parse_arguments() -> argparse.Namespace:
    """Parse PyCOMPSs arguments.

    :return: Argument's parser.
    """
    parser = argparse.ArgumentParser(
        description="PyCOMPSs application launcher"
    )
    parser.add_argument(
        "wall_clock",
        help="Application Wall Clock limit "
        "[wall_clock<=0 deactivated|wall_clock>0 max duration in seconds]",
    )
    parser.add_argument(
        "log_level", help="Logging level [trace|debug|api|info|off]"
    )
    parser.add_argument("tracing", help="Tracing [True | False]")
    parser.add_argument(
        "object_conversion", help="Object_conversion [true|false]"
    )
    parser.add_argument(
        "storage_configuration", help="Storage configuration [null|*]"
    )
    parser.add_argument("streaming_backend", help="Streaming Backend [null|*]")
    parser.add_argument(
        "streaming_master_name", help="Streaming Master Name [*]"
    )
    parser.add_argument(
        "streaming_master_port", help="Streaming Master Port [*]"
    )
    parser.add_argument("app_path", help="Application path")
    return parser.parse_args()


def __load_user_module(app_path: str, log_level: str) -> None:
    """Load the user module (resolve all user imports).

    This has shown to be necessary before doing "start_compss" in order
    to avoid segmentation fault in some libraries.

    :param app_path: Path to the file to be imported.
    :param log_level: Logging level.
    :return: None.
    """
    app_name = os.path.basename(app_path).split(".")[0]
    try:
        from importlib.machinery import SourceFileLoader  # noqa

        _ = SourceFileLoader(app_name, app_path).load_module()
    except Exception:  # pylint: disable=broad-except
        # Ignore any exception to try to run.
        # This exception can be produce for example with applications
        # that have code replacer and have imports to code that does not
        # exist (e.g. using autoparallel)
        if log_level != "off":
            print(
                "WARNING: Could not load the application "
                "(this may be the cause of a running exception."
            )


def __register_implementation_core_elements() -> None:
    """Register all implementations accumulated during initialization.

    Register the @implements core elements accumulated during the
    initialization of the @implements decorators. They have not been
    registered because the runtime was not started. And the load is
    necessary to resolve all user imports before starting the runtime (it has
    been found that starting the runtime and loading the user code may lead
    to import errors with some libraries - reason: unknown).

    :return: None
    """
    task_list = CONTEXT.get_to_register()
    for task, impl_signature in task_list:
        task.register_task()
        task.decorated_function.registered = True
        task.decorated_function.signature = impl_signature


def compss_main() -> None:
    """Execute the given application and flags with PyCOMPSs.

    General call:
    python $PYCOMPSS_HOME/pycompss/runtime/launch.py $wall_clock $log_level
           $PyObject_serialize $storage_conf $streaming_backend
           $streaming_master_name $streaming_master_port
           $fullAppPath $application_args

    :return: None.
    """
    # Let the Python binding know we are at master
    CONTEXT.set_master()
    # Then we can import the appropriate start and stop functions from the API
    from pycompss.api.api import (  # pylint: disable=import-outside-toplevel
        compss_start,
    )
    from pycompss.api.api import (  # pylint: disable=import-outside-toplevel
        compss_set_wall_clock,
    )

    # See parse_arguments, defined above
    # In order to avoid parsing user arguments, we are going to remove user
    # args from sys.argv
    user_sys_argv = sys.argv[10:]
    sys.argv = sys.argv[:10]
    args = parse_arguments()
    # We are done, now sys.argv must contain user args only
    sys.argv = [args.app_path] + user_sys_argv

    # Get log_level
    log_level = args.log_level

    # Setup tracing
    tracing = args.tracing == "true"

    # Get storage configuration at master
    storage_conf = args.storage_configuration

    # Load user imports before starting the runtime (can be avoided if
    # ENVIRONMENT_VARIABLE_LOAD -- defined in configuration.py --
    # is set to false).
    # Reason: some cases like autoparallel can require to avoid loading.
    # It is disabled if using storage (with dataClay this can not be done)
    if preload_user_code() and not use_storage(storage_conf):
        with loading_context():
            __load_user_module(args.app_path, log_level)

    # Start the runtime
    compss_start(log_level, tracing, False)

    # Register @implements core elements (they can not be registered in
    # __load_user__module__).
    __register_implementation_core_elements()

    # Get application wall clock limit
    wall_clock = int(args.wall_clock)
    if wall_clock > 0:
        compss_set_wall_clock(wall_clock)

    # Get object_conversion boolean
    TASK_FEATURES.set_object_conversion(args.object_conversion == "true")

    # Get application execution path
    LAUNCH_STATUS.set_app_path(args.app_path)

    # Setup logging
    binding_log_path = get_log_path()
    GLOBALS.set_log_directory(binding_log_path)
    init_logging(LOG_REMITTENT.MASTER, log_level, binding_log_path)
    logger = logging.getLogger("pycompss.runtime.launch")
    LAUNCH_STATUS.set_logger(logger)

    # Setup tmp path
    binding_tmp_path = get_tmp_path()  # master.workingDir
    GLOBALS.set_temporary_directory(binding_tmp_path)

    # Pre-load imports
    if preimports():
        if __debug__:
            logger.debug("Preloading imports")
        preload_imports(logger, "", "")

    # Get JVM options
    # jvm_opts = os.environ["JVM_OPTIONS_FILE"]
    # from pycompss.util.jvm.parser import convert_to_dict
    # opts = convert_to_dict(jvm_opts)
    # storage_conf = opts.get("-Dcompss.storage.conf")

    exit_code = 0
    try:
        if __debug__:
            logger.debug("--- START ---")
            logger.debug("PyCOMPSs Log path: %s", binding_log_path)

        # Start persistent storage
        persistent_storage = master_init_storage(storage_conf, logger)
        LAUNCH_STATUS.set_persistent_storage(persistent_storage)

        # Start streaming
        streaming = init_streaming(
            args.streaming_backend,
            args.streaming_master_name,
            args.streaming_master_port,
        )
        LAUNCH_STATUS.set_streaming(streaming)

        # Show module warnings
        if __debug__:
            show_optional_module_warnings()

        # MAIN EXECUTION
        app_path = args.app_path
        with EventMaster(TRACING_MASTER.application_running_event):
            # MAIN EXECUTION
            with open(app_path) as user_file:
                exec(  # nosec B102  # need to run the user application
                    compile(user_file.read(), app_path, "exec"), globals()
                )

        # End
        if __debug__:
            logger.debug("--- END ---")
    except SystemExit as system_exit:
        # Re-raising would not allow to stop the runtime gracefully.
        if system_exit.code != 0:
            print(
                "[ ERROR ]: User program ended with exitcode %s.",
                system_exit.code,
            )
            print("\t\tShutting down runtime...")
            if system_exit.code is None:
                exit_code = -1
            elif isinstance(system_exit.code, str):
                exit_code = -1
            else:
                exit_code = system_exit.code
    except SerializerException:
        exit_code = 1
        # If an object that can not be serialized has been used as a parameter.
        print("[ ERROR ]: Serialization exception")
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        for line in lines:
            if app_path in line:
                print("[ ERROR ]: In: %s", line)
        exit_code = 1
    except COMPSsException as compss_exception:
        # Any other exception occurred
        print(
            "[ ERROR ]: A COMPSs exception occurred: %s", str(compss_exception)
        )
        traceback.print_exc()
        exit_code = 0  # COMPSs exception is not considered an error
    except Exception as general_exception:  # pylint: disable=broad-except
        # Any other exception occurred
        print("[ ERROR ]: An exception occurred: %s" + str(general_exception))
        traceback.print_exc()
        exit_code = 1
    finally:
        # Stop runtime
        stop_all(exit_code)
        clean_log_configs()
    # --- Execution finished ---


# ###################################################### #
#        ------  FOR EXTERNAL EXECUTION ------           #
# Starts a new COMPSs runtime and calls the application. #
# ###################################################### #


def launch_pycompss_application(
    app: str,
    func: typing.Optional[str],
    log_level: str = "off",
    o_c: bool = False,
    debug: bool = False,
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
    app_name: str = "",
    uuid: str = "",
    log_dir: str = "",
    master_working_dir: str = "",
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
    shutdown_in_node_failure: bool = False,
    io_executors: int = 0,
    env_script: str = "",
    reuse_on_block: bool = True,
    nested_enabled: bool = False,
    tracing_task_dependencies: bool = False,
    trace_label: str = "",
    extrae_cfg_python: str = "",
    wcl: int = 0,
    cache_profiler: bool = False,
    ear: bool = False,
    data_provenance: bool = False,
    checkpoint_policy: str = "es.bsc.compss.checkpoint.policies.NoCheckpoint",
    checkpoint_params: str = "",
    checkpoint_folder: str = "",
    *args: typing.Any,
    **kwargs: typing.Any
) -> typing.Any:
    """Launch PyCOMPSs application from function.

    :param app: Application path
    :param func: Function
    :param log_level: Logging level [ "trace"|"debug"|"info"|"api"|"off" ]
                      (default: "off")
    :param o_c: Objects to string conversion [ True | False ] (default: False)
    :param debug: Debug mode [ True | False ] (default: False)
                  (overrides log_level)
    :param graph: Generate graph [ True | False ] (default: False)
    :param trace: Generate trace [ True | False ] (default: False)
    :param monitor: Monitor refresh rate (default: None)
    :param project_xml: Project xml file path
    :param resources_xml: Resources xml file path
    :param summary: Execution summary [ True | False ] (default: False)
    :param task_execution: Task execution (default: "compss")
    :param storage_impl: Storage implementation path
    :param storage_conf: Storage configuration file path
    :param streaming_backend: Streaming backend (default: None)
    :param streaming_master_name: Streaming master name (default: None)
    :param streaming_master_port: Streaming master port (default: None)
    :param task_count: Task count (default: 50)
    :param app_name: Application name (default: Interactive_date)
    :param uuid: UUId
    :param log_dir: Logging directory
    :param master_working_dir: Master working directory
    :param extrae_cfg: Extrae configuration file path
    :param extrae_final_directory: Extrae final directory (default: "")
    :param comm: Communication library (default: NIO)
    :param conn: Connector (default: DefaultSSHConnector)
    :param master_name: Master Name (default: "")
    :param master_port: Master port (default: "")
    :param scheduler: Scheduler (default:
                  es.bsc.compss.scheduler.lookahead.locality.LocalityTS)
    :param jvm_workers: Java VM parameters
                        (default: "-Xms1024m,-Xmx1024m,-Xmn400m")
    :param cpu_affinity: CPU Core affinity (default: "automatic")
    :param gpu_affinity: GPU Core affinity (default: "automatic")
    :param fpga_affinity: FPA Core affinity (default: "automatic")
    :param fpga_reprogram: FPGA reprogram command (default: "")
    :param profile_input: Input profile  (default: "")
    :param profile_output: Output profile  (default: "")
    :param scheduler_config: Scheduler configuration  (default: "")
    :param external_adaptation: External adaptation [ True | False ]
                                (default: False)
    :param propagate_virtual_environment: Propagate virtual environment
                                          [ True | False ] (default: False)
    :param mpi_worker: Use the MPI worker [ True | False ] (default: False)
    :param worker_cache: Use the worker cache [ True | int(size) | False]
                         (default: False)
    :param shutdown_in_node_failure: Shutdown in node failure [ True | False]
                                     (default: False)
    :param io_executors: <Integer> Number of IO executors
    :param env_script: <String> Environment script to be sourced in workers
    :param reuse_on_block: Reuse on block [ True | False] (default: True)
    :param nested_enabled: Nested enabled [ True | False] (default: False)
    :param tracing_task_dependencies: Include task dependencies in trace
                                      [ True | False] (default: False)
    :param trace_label: <String> Add trace label
    :param extrae_cfg_python: <String> Extrae configuration file for the
                              workers
    :param wcl: <Integer> Wallclock limit. Stops the runtime if reached.
                0 means forever.
    :param cache_profiler: Use the cache profiler [ True | False ]
                           (default: False)
    :param ear: Use EAR [ True | False ] (default: False)
    :param data_provenance: Enable data provenance [ True | False ]
                            (default: False)
    :param checkpoint_policy: Checkpointing policy.
                              (default: "es.bsc.compss.checkpoint.
                                         policies.NoCheckpoint")
    :param checkpoint_params: Checkpointing parameters.
                              (default: "")
    :param checkpoint_folder: Checkpointing folder.
                              (default: "")
    :param args: Positional arguments
    :param kwargs: Named arguments
    :return: Execution result
    """
    # Check that COMPSs is available
    if "COMPSS_HOME" not in os.environ:
        # Do not allow to continue if COMPSS_HOME is not defined
        raise PyCOMPSsException(
            "ERROR: COMPSS_HOME is not defined in the environment"
        )

    # Let the Python binding know we are at master
    CONTEXT.set_master()
    # Then we can import the appropriate start and stop functions from the API
    from pycompss.api.api import (  # pylint: disable=import-outside-toplevel
        compss_start,
    )
    from pycompss.api.api import (  # pylint: disable=import-outside-toplevel
        compss_stop,
    )

    ##############################################################
    # INITIALIZATION
    ##############################################################

    if debug:
        log_level = "debug"

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
        log_dir,
        master_working_dir,
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
        ear,
        data_provenance,
        checkpoint_policy,
        checkpoint_params,
        checkpoint_folder,
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
        False, o_c, storage_impl, app, debug, mpi_worker
    )
    all_vars.update(env_vars)

    monitoring_vars = prepare_loglevel_graph_for_monitoring(
        monitor, graph, debug, log_level
    )
    all_vars.update(monitoring_vars)

    if CONSTANTS.running_in_supercomputer:
        updated_vars = updated_variables_in_sc()
        all_vars.update(updated_vars)

    all_vars["ld_library_path"] = prepare_tracing_environment(
        all_vars["trace"], all_vars["extrae_lib"], all_vars["ld_library_path"]
    )

    inf_vars = check_infrastructure_variables(
        all_vars["project_xml"],
        all_vars["resources_xml"],
        all_vars["compss_home"],
        all_vars["app_name"],
        all_vars["file_name"],
        all_vars["external_adaptation"],
    )
    all_vars.update(inf_vars)

    create_init_config_file(**all_vars)

    ##############################################################
    # RUNTIME START
    ##############################################################

    # Runtime start
    compss_start(log_level, all_vars["trace"], True)

    # Setup logging
    binding_log_path = get_log_path()
    GLOBALS.set_log_directory(binding_log_path)
    init_logging(LOG_REMITTENT.MASTER, log_level, binding_log_path)
    logger = logging.getLogger("pycompss.runtime.launch")

    # Setup tmp path
    binding_tmp_path = get_tmp_path()  # master.workingDir
    GLOBALS.set_temporary_directory(binding_tmp_path)

    logger.debug("--- START ---")
    logger.debug("PyCOMPSs Log path: %s", binding_log_path)

    if storage_impl and storage_conf:
        logger.debug("Starting storage")
        persistent_storage = master_init_storage(
            all_vars["storage_conf"], logger
        )
    else:
        persistent_storage = False

    logger.debug("Starting streaming")
    streaming = init_streaming(
        all_vars["streaming_backend"],
        all_vars["streaming_master_name"],
        all_vars["streaming_master_port"],
    )

    saved_argv = sys.argv
    sys.argv = list(args)
    # Execution:
    with EventMaster(TRACING_MASTER.application_running_event):
        if func is None or func == "__main__":
            with open(app, "r") as app_fd:
                exec(  # nosec B102  # need to run the user application
                    app_fd.read()
                )
            result = None
        else:
            from importlib.machinery import SourceFileLoader  # noqa

            imported_module = SourceFileLoader(
                all_vars["file_name"], app
            ).load_module()
            method_to_call = getattr(imported_module, func)
            try:
                result = method_to_call(*args, **kwargs)
            except TypeError:
                result = method_to_call()
    # Recover the system arguments
    sys.argv = saved_argv

    # Stop streaming
    if streaming:
        stop_streaming()

    # Stop persistent storage
    if persistent_storage:
        master_stop_storage(logger)

    logger.debug("--- END ---")

    ##############################################################
    # RUNTIME STOP
    ##############################################################

    # Stop runtime
    compss_stop()
    clean_log_configs()

    return result


if __name__ == "__main__":
    # This is the PyCOMPSs entry point.

    # Initialize multiprocessing
    initialize_multiprocessing()

    compss_main()
