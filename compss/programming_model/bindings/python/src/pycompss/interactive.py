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
PyCOMPSs Binding - Interactive API
==================================
    Provides the current start and stop for the use of PyCOMPSs interactively.
"""

import os
import sys
import logging
import time

import pycompss.util.context as context
import pycompss.util.interactive.helpers as interactive_helpers
from pycompss.runtime.binding import get_log_path
from pycompss.runtime.management.object_tracker import OT_is_pending_to_synchronize
from pycompss.runtime.management.classes import Future
from pycompss.runtime.commons import RUNNING_IN_SUPERCOMPUTER
from pycompss.runtime.commons import INTERACTIVE_FILE_NAME
from pycompss.runtime.commons import set_temporary_directory
from pycompss.util.environment.configuration import prepare_environment
from pycompss.util.environment.configuration import prepare_loglevel_graph_for_monitoring  # noqa: E501
from pycompss.util.environment.configuration import updated_variables_in_sc
from pycompss.util.environment.configuration import prepare_tracing_environment
from pycompss.util.environment.configuration import check_infrastructure_variables  # noqa: E501
from pycompss.util.environment.configuration import create_init_config_file
from pycompss.util.logger.helpers import get_logging_cfg_file
from pycompss.util.logger.helpers import init_logging
from pycompss.util.interactive.flags import check_flags
from pycompss.util.interactive.flags import print_flag_issues

# Tracing imports
from pycompss.util.tracing.helpers import emit_manual_event
from pycompss.runtime.constants import APPLICATION_RUNNING_EVENT

# Storage imports
from pycompss.util.storages.persistent import master_init_storage
from pycompss.util.storages.persistent import master_stop_storage

# Streaming imports
from pycompss.streams.environment import init_streaming
from pycompss.streams.environment import stop_streaming


# GLOBAL VARIABLES
APP_PATH = INTERACTIVE_FILE_NAME
PERSISTENT_STORAGE = False
STREAMING = False
LOG_PATH = '/tmp/'
GRAPHING = False


def start(log_level='off',                     # type: str
          debug=False,                         # type: bool
          o_c=False,                           # type: bool
          graph=False,                         # type: bool
          trace=False,                         # type: bool
          monitor=None,                        # type: int
          project_xml=None,                    # type: str
          resources_xml=None,                  # type: str
          summary=False,                       # type: bool
          task_execution='compss',             # type: str
          storage_impl=None,                   # type: str
          storage_conf=None,                   # type: str
          streaming_backend=None,              # type: str
          streaming_master_name=None,          # type: str
          streaming_master_port=None,          # type: str
          task_count=50,                       # type: int
          app_name=INTERACTIVE_FILE_NAME,      # type: str
          uuid=None,                           # type: str
          base_log_dir=None,                   # type: str
          specific_log_dir=None,               # type: str
          extrae_cfg=None,                     # type: str
          comm='NIO',                          # type: str
          conn='es.bsc.compss.connectors.DefaultSSHConnector',  # type: str
          master_name='',                      # type: str
          master_port='',                      # type: str
          scheduler='es.bsc.compss.scheduler.loadbalancing.LoadBalancingScheduler',  # type: str  # noqa: E501
          jvm_workers='-Xms1024m,-Xmx1024m,-Xmn400m',  # type: str
          cpu_affinity='automatic',            # type: str
          gpu_affinity='automatic',            # type: str
          fpga_affinity='automatic',           # type: str
          fpga_reprogram='',                   # type: str
          profile_input='',                    # type: str
          profile_output='',                   # type: str
          scheduler_config='',                 # type: str
          external_adaptation=False,           # type: bool
          propagate_virtual_environment=True,  # type: bool
          mpi_worker=False,                    # type: bool
          verbose=False                        # type: bool
          ):
    # type: (...) -> None
    """ Start the runtime in interactive mode.

    :param log_level: Logging level [ 'trace'|'debug'|'info'|'api'|'off' ]
                      (default: 'off')
    :param debug: Debug mode [ True | False ]
                  (default: False) (overrides log-level)
    :param o_c: Objects to string conversion [ True|False ]
                (default: False)
    :param graph: Generate graph [ True|False ]
                  (default: False)
    :param trace: Generate trace [ True|False|'scorep'|'arm-map'|'arm-ddt' ]
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
                           (default: 'compss')
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
                     default: INTERACTIVE_FILE_NAME)
    :param uuid: UUId
                 (default: None)
    :param base_log_dir: Base logging directory
                         (default: None)
    :param specific_log_dir: Specific logging directory
                             (default: None)
    :param extrae_cfg: Extrae configuration file path
                       (default: None)
    :param comm: Communication library
                 (default: NIO)
    :param conn: Connector
                 (default: DefaultSSHConnector)
    :param master_name: Master Name
                        (default: '')
    :param master_port: Master port
                        (default: '')
    :param scheduler: Scheduler (see runcompss)
                      (default: es.bsc.compss.scheduler.loadbalancing.LoadBalancingScheduler)  # noqa
    :param jvm_workers: Java VM parameters
                        (default: '-Xms1024m,-Xmx1024m,-Xmn400m')
    :param cpu_affinity: CPU Core affinity
                         (default: 'automatic')
    :param gpu_affinity: GPU affinity
                         (default: 'automatic')
    :param fpga_affinity: FPGA affinity
                          (default: 'automatic')
    :param fpga_reprogram: FPGA repogram command
                           (default: '')
    :param profile_input: Input profile
                          (default: '')
    :param profile_output: Output profile
                           (default: '')
    :param scheduler_config: Scheduler configuration
                             (default: '')
    :param external_adaptation: External adaptation [ True|False ]
                                (default: False)
    :param propagate_virtual_environment: Propagate virtual environment [ True|False ]  # noqa
                                          (default: False)
    :param mpi_worker: Use the MPI worker [ True|False ]
                       (default: False)
    :param verbose: Verbose mode [ True|False ]
                    (default: False)
    :return: None
    """
    # Export global variables
    global GRAPHING
    GRAPHING = graph
    __export_globals__()

    interactive_helpers.DEBUG = debug

    __show_flower__()

    # Let the Python binding know we are at master
    context.set_pycompss_context(context.MASTER)
    # Then we can import the appropriate start and stop functions from the API
    from pycompss.api.api import compss_start

    ##############################################################
    # INITIALIZATION
    ##############################################################

    # Initial dictionary with the user defined parameters
    all_vars = {'log_level': log_level,
                'debug': debug,
                'o_c': o_c,
                'graph': graph,
                'trace': trace,
                'monitor': monitor,
                'project_xml': project_xml,
                'resources_xml': resources_xml,
                'summary': summary,
                'task_execution': task_execution,
                'storage_impl': storage_impl,
                'storage_conf': storage_conf,
                'streaming_backend': streaming_backend,
                'streaming_master_name': streaming_master_name,
                'streaming_master_port': streaming_master_port,
                'task_count': task_count,
                'app_name': app_name,
                'uuid': uuid,
                'base_log_dir': base_log_dir,
                'specific_log_dir': specific_log_dir,
                'extrae_cfg': extrae_cfg,
                'comm': comm,
                'conn': conn,
                'master_name': master_name,
                'master_port': master_port,
                'scheduler': scheduler,
                'jvm_workers': jvm_workers,
                'cpu_affinity': cpu_affinity,
                'gpu_affinity': gpu_affinity,
                'fpga_affinity': fpga_affinity,
                'fpga_reprogram': fpga_reprogram,
                'profile_input': profile_input,
                'profile_output': profile_output,
                'scheduler_config': scheduler_config,
                'external_adaptation': external_adaptation,
                'propagate_virtual_environment': propagate_virtual_environment,
                'mpi_worker': mpi_worker}

    # Check the provided flags
    flags, issues = check_flags(all_vars)
    if not flags:
        print_flag_issues(issues)
        return None

    # Prepare the environment
    env_vars = prepare_environment(True, o_c, storage_impl,
                                   'undefined', debug, trace, mpi_worker)
    all_vars.update(env_vars)

    # Update the log level and graph values if monitoring is enabled
    monitoring_vars = prepare_loglevel_graph_for_monitoring(monitor,
                                                            graph,
                                                            debug,
                                                            log_level)
    all_vars.update(monitoring_vars)

    # Check if running in supercomputer and update the variables accordingly
    # with the defined in the launcher and exported in environment variables.
    if RUNNING_IN_SUPERCOMPUTER:
        updated_vars = updated_variables_in_sc()
        if verbose:
            print("- Overridden project xml with: %s" %
                  updated_vars['project_xml'])
            print("- Overridden resources xml with: %s" %
                  updated_vars['resources_xml'])
            print("- Overridden master name with: %s" %
                  updated_vars['master_name'])
            print("- Overridden master port with: %s" %
                  updated_vars['master_port'])
            print("- Overridden uuid with: %s" %
                  updated_vars['uuid'])
            print("- Overridden base log dir with: %s" %
                  updated_vars['base_log_dir'])
            print("- Overridden specific log dir with: %s" %
                  updated_vars['specific_log_dir'])
            print("- Overridden storage conf with: %s" %
                  updated_vars['storage_conf'])
            print("- Overridden log level with: %s" %
                  str(updated_vars['log_level']))
            print("- Overridden debug with: %s" %
                  str(updated_vars['debug']))
            print("- Overridden trace with: %s" %
                  str(updated_vars['trace']))
        all_vars.update(updated_vars)

    # Update the tracing environment if set and set the appropriate trace
    # integer value
    tracing_vars = prepare_tracing_environment(all_vars['trace'],
                                               all_vars['extrae_lib'],
                                               all_vars['ld_library_path'])
    all_vars['trace'], all_vars['ld_library_path'] = tracing_vars

    # Update the infrastructure variables if necessary
    inf_vars = check_infrastructure_variables(all_vars['project_xml'],
                                              all_vars['resources_xml'],
                                              all_vars['compss_home'],
                                              all_vars['app_name'],
                                              all_vars['file_name'],
                                              all_vars['external_adaptation'])
    all_vars.update(inf_vars)

    # With all this information, create the configuration file for the
    # runtime start
    create_init_config_file(**all_vars)

    ##############################################################
    # RUNTIME START
    ##############################################################

    print("* - Starting COMPSs runtime...                       *")
    sys.stdout.flush()  # Force flush
    compss_start(log_level, all_vars['trace'], True)

    global LOG_PATH
    LOG_PATH = get_log_path()
    set_temporary_directory(LOG_PATH)
    print("* - Log path : " + LOG_PATH)

    # Setup logging
    binding_log_path = get_log_path()
    log_path = os.path.join(all_vars['compss_home'],
                            'Bindings',
                            'python',
                            str(all_vars['major_version']),
                            'log')
    set_temporary_directory(binding_log_path)
    logging_cfg_file = get_logging_cfg_file(log_level)
    init_logging(os.path.join(log_path, logging_cfg_file), binding_log_path)
    logger = logging.getLogger("pycompss.runtime.launch")

    __print_setup__(verbose, all_vars)

    logger.debug("--- START ---")
    logger.debug("PyCOMPSs Log path: %s" % LOG_PATH)

    logger.debug("Starting storage")
    global PERSISTENT_STORAGE
    PERSISTENT_STORAGE = master_init_storage(all_vars['storage_conf'], logger)

    logger.debug("Starting streaming")
    global STREAMING
    STREAMING = init_streaming(all_vars['streaming_backend'],
                               all_vars['streaming_master_name'],
                               all_vars['streaming_master_port'])

    # MAIN EXECUTION
    # let the user write an interactive application
    print("* - PyCOMPSs Runtime started... Have fun!            *")
    print("******************************************************")

    # Emit the application start event (the 0 is in the stop function)
    emit_manual_event(APPLICATION_RUNNING_EVENT)


def __show_flower__():
    # type: () -> None
    """ Shows the flower and version through stdout.

    :return: None
    """
    print("******************************************************")  # noqa
    print("*************** PyCOMPSs Interactive *****************")  # noqa
    print("******************************************************")  # noqa
    print("*          .-~~-.--.           _____       ________  *")  # noqa
    print("*         :         )         |____ \     |____   /  *")  # noqa
    print("*   .~ ~ -.\       /.- ~~ .     ___) |        /  /   *")  # noqa
    print("*   >       `.   .'       <    / ___/        /  /    *")  # noqa
    print("*  (         .- -.         )  | |___   _    /  /     *")  # noqa
    print("*   `- -.-~  `- -'  ~-.- -'   |_____| |_|  /__/      *")  # noqa
    print("*     (        :        )           _ _ .-:          *")  # noqa
    print("*      ~--.    :    .--~        .-~  .-~  }          *")  # noqa
    print("*          ~-.-^-.-~ \_      .~  .-~   .~            *")  # noqa
    print("*                   \ \ '     \ '_ _ -~              *")  # noqa
    print("*                    \`.\`.    //                    *")  # noqa
    print("*           . - ~ ~-.__\`.\`-.//                     *")  # noqa
    print("*       .-~   . - ~  }~ ~ ~-.~-.                     *")  # noqa
    print("*     .' .-~      .-~       :/~-.~-./:               *")  # noqa
    print("*    /_~_ _ . - ~                 ~-.~-._            *")  # noqa
    print("*                                     ~-.<           *")  # noqa
    print("******************************************************")  # noqa


def __print_setup__(verbose, all_vars):
    # type: (bool, dict) -> None
    """ Print the setup variables through stdout (only if verbose is True).

    :param verbose: Verbose mode [True | False]
    :param all_vars: Dictionary containing all variables.
    :return: None
    """
    logger = logging.getLogger(__name__)
    output = ""
    output += "******************************************************\n"
    output += " CONFIGURATION: \n"
    for k, v in sorted(all_vars.items()):
        output += '  - {0:20} : {1} \n'.format(k, v)
    output += "******************************************************"
    if verbose:
        print(output)
    logger.debug(output)


def stop(sync=False):
    # type: (bool) -> None
    """ Runtime stop.

    :param sync: Scope variables synchronization [ True | False ]
                 (default: False)
    :return: None
    """
    from pycompss.api.api import compss_stop

    print("****************************************************")
    print("*************** STOPPING PyCOMPSs ******************")
    print("****************************************************")

    logger = logging.getLogger(__name__)

    if sync:
        sync_msg = "Synchronizing all future objects left on the user scope."
        print(sync_msg)
        logger.debug(sync_msg)
        from pycompss.api.api import compss_wait_on

        ipython = globals()['__builtins__']['get_ipython']()
        # import pprint
        # pprint.pprint(ipython.__dict__, width=1)
        reserved_names = ('quit', 'exit', 'get_ipython',
                          'APP_PATH', 'ipycompss', 'In', 'Out')
        raw_code = ipython.__dict__['user_ns']
        for k in raw_code:
            obj_k = raw_code[k]
            if not k.startswith('_'):   # not internal objects
                if type(obj_k) == Future:
                    print("Found a future object: %s" % str(k))
                    logger.debug("Found a future object: %s" % (k,))
                    ipython.__dict__['user_ns'][k] = compss_wait_on(obj_k)
                elif k not in reserved_names:
                    try:
                        if OT_is_pending_to_synchronize(obj_k):
                            print("Found an object to synchronize: %s" % str(k))
                            logger.debug("Found an object to synchronize: %s" % (k,))
                            ipython.__dict__['user_ns'][k] = compss_wait_on(obj_k)
                    except TypeError:
                        # Unhashable type: List - could be a collection
                        if isinstance(obj_k, list):
                            print("Found a list to synchronize: %s" % str(k))
                            logger.debug("Found a list to synchronize: %s" % (k,))
                            ipython.__dict__['user_ns'][k] = compss_wait_on(obj_k)
                else:
                    pass
    else:
        print("Warning: some of the variables used with PyCOMPSs may")
        print("         have not been brought to the master.")

    # Stop streaming
    if STREAMING:
        stop_streaming()

    # Stop persistent storage
    if PERSISTENT_STORAGE:
        master_stop_storage(logger)

    # Emit the 0 for the APPLICATION_RUNNING_EVENT emitted on start function.
    emit_manual_event(0)

    # Stop runtime
    compss_stop()

    __clean_temp_files__()

    # Let the Python binding know we are not at master anymore
    context.set_pycompss_context(context.OUT_OF_SCOPE)

    print("****************************************************")
    logger.debug("--- END ---")

    # --- Execution finished ---


def __show_current_graph__(fit=False):
    # type: (bool) -> ...
    """ Show current graph.

    :param fit: Fit to width [ True | False ] (default: False)
    :return: None
    """
    if GRAPHING:
        return __show_graph__(name='current_graph', fit=fit)
    else:
        print('Oops! Graph is not enabled in this execution.')
        print('      Please, enable it by setting the graph flag when' +
              ' starting PyCOMPSs.')


def __show_complete_graph__(fit=False):
    # type: (bool) -> ...
    """ Show complete graph.

    :param fit: Fit to width [ True | False ] (default: False)
    :return: None
    """
    if GRAPHING:
        return __show_graph__(name='complete_graph', fit=fit)
    else:
        print('Oops! Graph is not enabled in this execution.')
        print('      Please, enable it by setting the graph flag when' +
              ' starting PyCOMPSs.')
        return None


def __show_graph__(name='complete_graph', fit=False):
    # type: (str, bool) -> ...
    """ Show graph.

    :param name: Graph to show (default: 'complete_graph')
    :param fit: Fit to width [ True | False ] (default: False)
    :return: None
    """
    try:
        from graphviz import Source  # noqa
    except ImportError:
        print('Oops! graphviz is not available.')
        return None
    monitor_file = open(LOG_PATH + '/monitor/' + name + '.dot', 'r')
    text = monitor_file.read()
    monitor_file.close()
    if fit:
        try:
            # Convert to png and show full picture
            filename = LOG_PATH + '/monitor/' + name
            extension = 'png'
            import os
            if os.path.exists(filename + '.' + extension):
                os.remove(filename + '.' + extension)
            s = Source(text, filename=filename, format=extension)
            s.render()
            from IPython.display import Image  # noqa
            image = Image(filename=filename + '.' + extension)
            return image
        except Exception:
            print('Oops! Failed rendering the graph.')
            raise
    else:
        return Source(text)


# ########################################################################### #
# ########################################################################### #
# ########################################################################### #


def __export_globals__():
    # type: () -> None
    """ Export globals into interactive environment.

    :return: None
    """
    global APP_PATH
    # Super ugly, but I see no other way to define the APP_PATH across the
    # interactive execution without making the user to define it explicitly.
    # It is necessary to define only one APP_PATH because of the two decorators
    # need to access the same information.
    # if the file is created per task, the constraint will not be able to work.
    # Get ipython globals
    ipython = globals()['__builtins__']['get_ipython']()
    # import pprint
    # pprint.pprint(ipython.__dict__, width=1)
    # Extract user globals from ipython
    user_globals = ipython.__dict__['ns_table']['user_global']
    # Inject APP_PATH variable to user globals so that task and constraint
    # decorators can get it.
    temp_app_filename = "".join((os.path.join(os.getcwd(),
                                              INTERACTIVE_FILE_NAME),
                                 '_',
                                 str(time.strftime('%d%m%y_%H%M%S')),
                                 '.py'))
    user_globals['APP_PATH'] = temp_app_filename
    APP_PATH = temp_app_filename


def __clean_temp_files__():
    # type: () -> None
    """ Remove any temporary files that may exist.

    Currently: APP_PATH, which contains the file path where all interactive
               code required by the worker is.

    :return: None
    """
    try:
        if os.path.exists(APP_PATH):
            os.remove(APP_PATH)
        if os.path.exists(APP_PATH + 'c'):
            os.remove(APP_PATH + 'c')
    except OSError:
        print("[ERROR] An error has occurred when cleaning temporary files.")
