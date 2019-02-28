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
from pycompss.util.configurators import create_init_config_file
from pycompss.util.scs import get_master_node
from pycompss.util.scs import get_master_port
from pycompss.util.scs import get_xmls
from pycompss.util.scs import get_uuid
from pycompss.util.scs import get_base_log_dir
from pycompss.util.scs import get_specific_log_dir
from pycompss.util.scs import get_log_level
from pycompss.util.scs import get_tracing
from pycompss.util.scs import get_storage_conf
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
    persistent_storage = False
    if storage_conf != 'null':
        persistent_storage = True
        from storage.api import init as init_storage
        from storage.api import finish as finish_storage

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
        if persistent_storage:
            if __debug__:
                logger.debug('Storage configuration file: %s' % storage_conf)
            init_storage(config_file_path=storage_conf)
        if __debug__:
            show_optional_module_warnings()
        # MAIN EXECUTION
        if IS_PYTHON3:
            exec (compile(open(app_path).read(), app_path, 'exec'), globals())
        else:
            execfile(app_path, globals())  # MAIN EXECUTION
        if persistent_storage:
            finish_storage()
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
    Launch pycompss application.

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
    :return: Execution result
    """

    global app_path

    # Let the Python binding know we are at master
    context.set_pycompss_context(context.MASTER)
    # Then we can import the appropriate start and stop functions from the API
    from pycompss.api.api import compss_start, compss_stop

    launch_path = os.path.dirname(os.path.abspath(__file__))
    # compss_home = launch_path without the last 4 folders:
    # (Bindings/python/pycompss/runtime)
    compss_home = os.path.sep.join(launch_path.split(os.path.sep)[:-4])

    # Grab the existing PYTHONPATH and CLASSPATH values
    pythonpath = os.environ['PYTHONPATH']
    classpath = os.environ['CLASSPATH']
    ld_library_path = os.environ['LD_LIBRARY_PATH']

    # Enable/Disable object to string conversion
    # set cross-module variable
    binding.object_conversion = o_c

    # Get the filename and its path.
    file_name = os.path.splitext(os.path.basename(app))[0]
    cp = os.path.dirname(app)

    # Set storage classpath
    if storage_impl:
        if storage_impl == 'redis':
            cp = cp + ':' + compss_home + '/Tools/storage/redis/compss-redisPSCO.jar'
        else:
            cp = cp + ':' + storage_impl

    # Set extrae dependencies
    if not "EXTRAE_HOME" in os.environ:
        # It can be defined by the user or by launch_compss when running in Supercomputer
        extrae_home = compss_home + '/Dependencies/extrae'
        os.environ['EXTRAE_HOME'] = extrae_home
    else:
        extrae_home = os.environ['EXTRAE_HOME']
    extrae_lib = extrae_home + '/lib'

    if monitor is not None:
        # Enable the graph if the monitoring is enabled
        graph = True
        # Set log level info
        log_level = 'info'

    if debug:
        # If debug is enabled, the output is more verbose
        log_level = 'debug'

    if RUNNING_IN_SUPERCOMPUTER:
        # Since the deployment in supercomputers is done through the use of enqueue_compss
        # and consequently launch_compss - the project and resources xmls are already created
        project_xml, resources_xml = get_xmls()
        # It also exported some environment variables that we need here
        master_name = get_master_node()
        master_port = get_master_port()
        uuid = get_uuid()
        base_log_dir = get_base_log_dir()
        specific_log_dir = get_specific_log_dir()
        storage_conf = get_storage_conf()
        # Override debug considering the parameter defined in pycompss_interactive_sc script
        # and exported by launch_compss
        log_level = get_log_level()
        if log_level == 'debug':
            debug = True
        else:
            debug = False
        # Override tracing considering the parameter defined in pycompss_interactive_sc script
        # and exported by launch_compss
        trace = get_tracing()

    if debug:
        # Add environment variable to get binding-commons debug information
        os.environ['COMPSS_BINDINGS_DEBUG'] = '1'

    if trace is False:
        trace = 0
    elif trace == 'basic' or trace is True:
        trace = 1
        os.environ['LD_PRELOAD'] = extrae_lib + '/libpttrace.so'
    elif trace == 'advanced':
        trace = 2
        os.environ['LD_PRELOAD'] = extrae_lib + '/libpttrace.so'
    else:
        print("ERROR: Wrong tracing parameter ( [ True | basic ] | advanced | False)")
        return -1

    if project_xml is None:
        project_xml = compss_home + os.path.sep + 'Runtime/configuration/xml/projects/default_project.xml'
    if resources_xml is None:
        resources_xml = compss_home + os.path.sep + 'Runtime/configuration/xml/resources/default_resources.xml'
    app_name = file_name if app_name is None else app_name
    external_adaptation = 'true' if external_adaptation else 'false'
    major_version = str(sys.version_info[0])
    python_interpreter = 'python' + major_version
    python_version = major_version
    # Check if running within a virtual environment
    if 'VIRTUAL_ENV' in os.environ:
        python_virtual_environment = os.environ['VIRTUAL_ENV']
    else:
        python_virtual_environment = 'null'

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
    app_path = app
    log_path = get_log_path()
    # Logging setup
    if debug or log_level == "debug":
        json_path = '/Bindings/python/' + major_version + '/log/logging_debug.json'
        init_logging(compss_home + json_path, log_path)
    elif log_level == "info":
        json_path = '/Bindings/python/' + major_version + '/log/logging_off.json'
        init_logging(compss_home + json_path, log_path)
    elif log_level == "off":
        json_path = '/Bindings/python/' + major_version + '/log/logging_off.json'
        init_logging(compss_home + json_path, log_path)
    else:
        # Default
        json_path = '/Bindings/python/' + str(major_version) + '/log/logging.json'
        init_logging(compss_home + json_path, log_path)
    logger = logging.getLogger("pycompss.runtime.launch")

    logger.debug('--- START ---')
    logger.debug('PyCOMPSs Log path: %s' % log_path)
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
    logger.debug('--- END ---')

    compss_stop()

    return result


if __name__ == '__main__':
    """
    This is the PyCOMPSs entry point
    """
    compss_main()
