#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Binding - Launch
=========================
This file contains the __main__ method.
It is called from pycompssext script with the user and environment parameters.
"""

import os
import sys
import logging
import traceback
from tempfile import mkdtemp

import pycompss.util.context as context
import pycompss.runtime.binding as binding
from pycompss.runtime.binding import get_log_path
from pycompss.runtime.commons import IS_PYTHON3
from pycompss.runtime.commons import RUNNING_IN_SUPERCOMPUTER
from pycompss.util.launcher import prepare_environment
from pycompss.util.launcher import prepare_loglevel_graph_for_monitoring
from pycompss.util.launcher import updated_variables_in_sc
from pycompss.util.launcher import prepare_tracing_environment
from pycompss.util.launcher import check_infrastructure_variables
from pycompss.util.launcher import create_init_config_file
from pycompss.util.launcher import setup_logger
from pycompss.util.persistent_storage import init_storage
from pycompss.util.persistent_storage import stop_storage
from pycompss.util.logs import init_logging
from pycompss.util.serializer import SerializerException
from pycompss.util.optional_modules import show_optional_module_warnings


# Global variable also used within decorators
app_path = None

if IS_PYTHON3:
    _py_version = 3
else:
    _py_version = 2


def get_logging_cfg_file(log_level):
    """
    Retrieves the logging configuration file.

    :param log_level: Log level [ 'debug' | 'info' | 'off' ]
    :return: Logging configuration file
    """

    logging_cfg_file = 'logging.json'
    cfg_files = {
        'debug': 'logging_debug.json',
        'info': 'logging_off.json',
        'off': 'logging_off.json'
    }
    if log_level in cfg_files:
        logging_cfg_file = cfg_files[log_level]
    return logging_cfg_file


def parse_arguments():
    """
    Parse PyCOMPSs arguments.

    :return: Parser arguments.
    """

    import argparse
    parser = argparse.ArgumentParser(description='PyCOMPSs application launcher')
    parser.add_argument('log_level', help='Logging level [debug|info|off]')
    parser.add_argument('object_conversion', help='Object_conversion [true|false]')
    parser.add_argument('storage_configuration', help='Storage configuration [null|*]')
    parser.add_argument('app_path', help='Application path')
    return parser.parse_args()


def compss_main():
    """
    General call:
    python $PYCOMPSS_HOME/pycompss/runtime/launch.py $log_level $PyObject_serialize $storage_conf $fullAppPath $application_args

    :return: None
    """
    global app_path

    # Let the Python binding know we are at master
    context.set_pycompss_context(context.MASTER)
    # Then we can import the appropriate start and stop functions from the API
    from pycompss.api.api import compss_start, compss_stop

    # Start the runtime, see bindings commons
    compss_start()
    # See parse_arguments, defined above
    # In order to avoid parsing user arguments, we are going to remove user
    # args from sys.argv
    user_sys_argv = sys.argv[5:]
    sys.argv = sys.argv[:5]
    args = parse_arguments()
    # We are done, now sys.argv must contain user args only
    sys.argv = [args.app_path] + user_sys_argv

    # Get log_level
    log_level = args.log_level

    # Get object_conversion boolean
    binding.object_conversion = args.object_conversion == 'true'

    # Get storage configuration at master
    storage_conf = args.storage_configuration

    # Get application execution path
    app_path = args.app_path

    binding_log_path = get_log_path()
    log_path = os.path.join(os.getenv('COMPSS_HOME'), 'Bindings', 'python', str(_py_version), 'log')
    binding.temp_dir = mkdtemp(prefix='pycompss', dir=os.path.join(binding_log_path, 'tmpFiles/'))

    logging_cfg_file = get_logging_cfg_file(log_level)

    init_logging(os.path.join(log_path, logging_cfg_file), binding_log_path)
    if __debug__:
        logger = logging.getLogger("pycompss.runtime.launch")

    # Get JVM options
    # jvm_opts = os.environ['JVM_OPTIONS_FILE']
    # from pycompss.util.jvm_parser import convert_to_dict
    # opts = convert_to_dict(jvm_opts)
    # storage_conf = opts.get('-Dcompss.storage.conf')

    try:
        if __debug__:
            logger.debug('--- START ---')
            logger.debug('PyCOMPSs Log path: %s' % binding_log_path)
        persistent_storage = init_storage(storage_conf, logger)
        if __debug__:
            show_optional_module_warnings()
        # MAIN EXECUTION
        if IS_PYTHON3:
            exec (compile(open(app_path).read(), app_path, 'exec'), globals())
        else:
            execfile(app_path, globals())  # MAIN EXECUTION
        if persistent_storage:
            stop_storage()
        if __debug__:
            logger.debug('--- END ---')
    except SystemExit as e:
        if e.code != 0:  # Seems this is not happening
            print('[ ERROR ]: User program ended with exitcode %s.' % e.code)
            print('\t\tShutting down runtime...')
    except SerializerException:
        # If an object that can not be serialized has been used as a parameter.
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        for line in lines:
            if app_path in line:
                print('[ ERROR ]: In: %s', line)
    finally:
        compss_stop()
        sys.stdout.flush()
        sys.stderr.flush()
    # --- Execution finished ---


# ###################################################### #
#        ------  FOR EXTERNAL EXECUTION ------           #
# Starts a new COMPSs runtime and calls the application. #
# ###################################################### #

def launch_pycompss_application(app, func,
                                log_level='off',
                                o_c=False,
                                debug=False,
                                graph=False,
                                trace=False,
                                monitor=None,
                                project_xml=None,
                                resources_xml=None,
                                summary=False,
                                task_execution='compss',
                                storage_impl=None,
                                storage_conf=None,
                                task_count=50,
                                app_name=None,
                                uuid=None,
                                base_log_dir=None,
                                specific_log_dir=None,
                                extrae_cfg=None,
                                comm='NIO',
                                conn='es.bsc.compss.connectors.DefaultSSHConnector',
                                master_name='',
                                master_port='',
                                scheduler='es.bsc.compss.scheduler.loadBalancingScheduler.LoadBalancingScheduler',
                                jvm_workers='-Xms1024m,-Xmx1024m,-Xmn400m',
                                cpu_affinity='automatic',
                                gpu_affinity='automatic',
                                fpga_affinity='automatic',
                                fpga_reprogram='',
                                profile_input='',
                                profile_output='',
                                scheduler_config='',
                                external_adaptation=False,
                                propagate_virtual_environment=True,
                                *args, **kwargs
                                ):
    """
    Launch PyCOMPSs application from function.

    :param app: Application path
    :param func: Function
    :param args: Arguments
    :param kwargs: Keyword arguments
    :param log_level: Logging level [ 'off' | 'info'  | 'debug' ] (default: 'off')
    :param o_c: Objects to string conversion [ True | False ] (default: False)
    :param debug: Debug mode [ True | False ] (default: False) (overrides log_level)
    :param graph: Generate graph [ True | False ] (default: False)
    :param trace: Generate trace [ True | False ] (default: False)
    :param monitor: Monitor refresh rate (default: None)
    :param project_xml: Project xml file path
    :param resources_xml: Resources xml file path
    :param summary: Execution summary [ True | False ] (default: False)
    :param task_execution: Task execution (default: 'compss')
    :param storage_impl: Storage implementation path
    :param storage_conf: Storage configuration file path
    :param task_count: Task count (default: 50)
    :param app_name: Application name (default: Interactive_date)
    :param uuid: UUId
    :param base_log_dir: Base logging directory
    :param specific_log_dir: Specific logging directory
    :param extrae_cfg: Extrae configuration file path
    :param comm: Communication library (default: NIO)
    :param conn: Connector (default: DefaultSSHConnector)
    :param master_name: Master Name (default: '')
    :param master_port: Master port (default: '')
    :param scheduler: Scheduler (default: LoadBalancingScheduler)
    :param jvm_workers: Java VM parameters (default: '-Xms1024m,-Xmx1024m,-Xmn400m')
    :param cpu_affinity: CPU Core affinity (default: 'automatic')
    :param gpu_affinity: GPU Core affinity (default: 'automatic')
    :param fpga_affinity: FPA Core affinity (default: 'automatic')
    :param fpga_reprogram: FPGA repogram command (default: '')
    :param profile_input: Input profile  (default: '')
    :param profile_output: Output profile  (default: '')
    :param scheduler_config: Scheduler configuration  (default: '')
    :param external_adaptation: External adaptation [ True | False ] (default: False)
    :param propagate_virtual_environment: Propagate virtual environment [ True | False ] (default: False)
    :param args: Positional arguments
    :param kwargs: Named arguments
    :return: Execution result
    """

    # Let the Python binding know we are at master
    context.set_pycompss_context(context.MASTER)
    # Then we can import the appropriate start and stop functions from the API
    from pycompss.api.api import compss_start, compss_stop

    # Prepare the environment
    env_vars = prepare_environment(True, o_c, storage_impl, app, debug)
    compss_home, pythonpath, classpath, ld_library_path, cp, extrae_home, extrae_lib, file_name = env_vars

    ##############################################################
    # INITIALIZATION
    ##############################################################

    monitor, graph, log_level = prepare_loglevel_graph_for_monitoring(monitor, graph, debug, log_level)

    if RUNNING_IN_SUPERCOMPUTER:
        updated_vars = updated_variables_in_sc()
        project_xml, resources_xml, master_name, master_port, uuid, base_log_dir, specific_log_dir, storage_conf, log_level, debug, trace = updated_vars

    trace = prepare_tracing_environment(trace, extrae_lib)

    updated_vars = check_infrastructure_variables(project_xml, resources_xml, compss_home, app_name, file_name, external_adaptation)
    project_xml, resources_xml, app_name, external_adaptation, major_version, python_interpreter, python_version, python_virtual_environment = updated_vars

    create_init_config_file(compss_home,
                            debug,
                            log_level,
                            project_xml,
                            resources_xml,
                            summary,
                            task_execution,
                            storage_conf,
                            task_count,
                            app_name,
                            uuid,
                            base_log_dir,
                            specific_log_dir,
                            graph,
                            monitor,
                            trace,
                            extrae_cfg,
                            comm,
                            conn,
                            master_name,
                            master_port,
                            scheduler,
                            cp,
                            classpath,
                            ld_library_path,
                            pythonpath,
                            jvm_workers,
                            cpu_affinity,
                            gpu_affinity,
                            fpga_affinity,
                            fpga_reprogram,
                            profile_input,
                            profile_output,
                            scheduler_config,
                            external_adaptation,
                            python_interpreter,
                            python_version,
                            python_virtual_environment,
                            propagate_virtual_environment)

    ##############################################################
    # RUNTIME START
    ##############################################################

    # Runtime start
    compss_start()

    # Configure logging
    log_path = get_log_path()
    logger = setup_logger(debug, log_level, major_version, compss_home, log_path)

    logger.debug('--- START ---')
    logger.debug('PyCOMPSs Log path: %s' % log_path)
    persistent_storage = init_storage(storage_conf, logger)

    saved_argv = sys.argv
    sys.argv = args
    # Execution:
    if func is None or func == '__main__':
        execfile(app)
        result = None
    else:
        import imp
        imported_module = imp.load_source(file_name, app)
        method_to_call = getattr(imported_module, func)
        result = method_to_call(*args, **kwargs)
    # Recover the system arguments
    sys.argv = saved_argv

    if persistent_storage:
        stop_storage()

    logger.debug('--- END ---')

    compss_stop()

    return result


if __name__ == '__main__':
    """
    This is the PyCOMPSs entry point
    """
    compss_main()
