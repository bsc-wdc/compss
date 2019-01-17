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
        logger = logging.getLogger('pycompss.runtime.launch')

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


#################################################
# For external execution
#################################################

# Version 4.0
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
                                obj_conv=False,
                                cpu_affinity='automatic',
                                gpu_affinity='automatic',
                                fpga_affinity='automatic',
                                fpga_reprogram='',
                                profile_input='',
                                profile_output='',
                                scheduler_config='',
                                external_adaptation=False,
                                python_propagate_virtual_environment=True,
                                *args, **kwargs
                                ):
    """
    Launch pycompss application.

    :param app: Application path
    :param func: Function
    :param args: Arguments
    :param kwargs: Keyword arguments
    :param log_level: Logging level [ 'on' | 'off'] (default: 'off')
    :param o_c: Objects to string conversion [ True | False ] (default: False)
    :param debug: Debug mode [ True | False ] (default: False)
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
    :param obj_conv: Object conversion
    :param cpu_affinity: CPU Core affinity (default: 'automatic')
    :param gpu_affinity: GPU Core affinity (default: 'automatic')
    :param fpga_affinity: FPA Core affinity (default: 'automatic')
    :param fpga_reprogram: FPGA repogram command (default: '')
    :param profile_input: Input profile  (default: '')
    :param profile_output: Output profile  (default: '')
    :param scheduler_config: Scheduler configuration  (default: '')
    :param external_adaptation: External adaptation [ True | False ] (default: False)
    :param python_propagate_virtual_environment: Propagate virtual environment [ True | False ] (default: False)
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
    binding.object_conversion = obj_conv

    # Get the filename and its path.
    file_name = os.path.splitext(os.path.basename(app))[0]
    cp = os.path.dirname(app)

    # Set storage classpath
    if storage_impl:
        if storage_impl == 'redis':
            cp = cp + ':' + compss_home + '/Tools/storage/redis/compss-redisPSCO.jar'
        else:
            cp = cp + ':' + storage_impl

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
        if log_level == 'off':
            debug = False
        else:
            debug = True
        # Override tracing considering the parameter defined in pycompss_interactive_sc script
        # and exported by launch_compss
        if get_tracing():
            trace = 1
        else:
            trace = 0

    # Build a dictionary with all variables needed for initializing the runtime.
    config = dict()
    config['compss_home'] = compss_home
    config['debug'] = debug
    if project_xml is None:
        project_xml = 'Runtime/configuration/xml/projects/default_project.xml'
        config['project_xml'] = compss_home + os.path.sep + project_xml
    else:
        config['project_xml'] = project_xml
    if resources_xml is None:
        resources_xml = 'Runtime/configuration/xml/resources/default_resources.xml'
        config['resources_xml'] = compss_home + os.path.sep + resources_xml
    else:
        config['resources_xml'] = resources_xml
    config['summary'] = summary
    config['task_execution'] = task_execution
    config['storage_conf'] = storage_conf
    config['task_count'] = task_count
    if app_name is None:
        config['app_name'] = file_name
    else:
        config['app_name'] = app_name
    config['uuid'] = uuid
    config['base_log_dir'] = base_log_dir
    config['specific_log_dir'] = specific_log_dir
    config['graph'] = graph
    config['monitor'] = monitor
    config['trace'] = trace
    config['extrae_cfg'] = extrae_cfg
    config['comm'] = comm
    config['conn'] = conn
    config['master_name'] = master_name
    config['master_port'] = master_port
    config['scheduler'] = scheduler
    config['cp'] = cp
    config['classpath'] = classpath
    config['ld_library_path'] = ld_library_path
    config['jvm_workers'] = jvm_workers
    config['pythonpath'] = pythonpath
    config['cpu_affinity'] = cpu_affinity
    config['gpu_affinity'] = gpu_affinity
    config['fpga_affinity'] = fpga_affinity
    config['fpga_reprogram'] = fpga_reprogram
    config['profile_input'] = profile_input
    config['profile_output'] = profile_output
    config['scheduler_config'] = scheduler_config
    if external_adaptation:
        config['external_adaptation'] = 'true'
    else:
        config['external_adaptation'] = 'false'
    major_version = str(sys.version_info[0])
    config['python_interpreter'] = 'python' + major_version
    config['python_version'] = major_version
    if 'VIRTUAL_ENV' in os.environ:
        # Running within a virtual environment
        python_virtual_environment = os.environ['VIRTUAL_ENV']
    else:
        python_virtual_environment = 'null'
    config['python_virtual_environment'] = python_virtual_environment
    config['python_propagate_virtual_environment'] = python_propagate_virtual_environment

    initialize_compss(config)

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


def initialize_compss(config):
    """
    Creates the initialization files for the runtime start (java options file).
    Receives a dictionary (config) with the configuration parameters.

    WARNING!!! if new parameters are included in the runcompss launcher,
    they have to be considered in this configuration. Otherwise, the runtime will not start.

    * Current required parameters:
        - 'compss_home'      = <String>       = COMPSs installation path
        - 'debug'            = <Boolean>      = Enable/Disable debugging (True|False)
        - 'project_xml'      = <String>       = Specific project.xml path
        - 'resources_xml'    = <String>       = Specific resources.xml path
        - 'summary'          = <Boolean>      = Enable/Disable summary (True|False)
        - 'task_execution'   = <String>       = Who performs the task execution (normally "compss")
        - 'storage_conf'     = None|<String>  = Storage configuration file path
        - 'task_count'       = <Integer>      = Number of tasks (for structure initialization purposes)
        - 'app_name'         = <String>       = Application name
        - 'uuid'             = None|<String>  = Application UUID
        - 'base_log_dir'     = None|<String>  = Base log path
        - 'specific_log_dir' = None|<String>  = Specific log path
        - 'graph'            = <Boolean>      = Enable/Disable graph generation
        - 'monitor'          = None|<Integer> = Disable/Frequency of the monitor
        - 'trace'            = <Boolean>      = Enable/Disable trace generation
        - 'extrae_cfg'       = None|<String>  = Default extrae configuration/User specific extrae configuration
        - 'comm'             = <String>       = GAT/NIO
        - 'conn'             = <String>       = Connector (normally: es.bsc.compss.connectors.DefaultSSHConnector)
        - 'master_name'      = <String>       = Master node name
        - 'master_port'      = <String>       = Master node port
        - 'scheduler'        = <String>       = Scheduler (normally: es.bsc.compss.scheduler.resourceEmptyScheduler.ResourceEmptyScheduler)
        - 'cp'               = <String>       = Application path
        - 'classpath'        = <String>       = CLASSPATH environment variable contents
        - 'ld_library_path'  = <String>       = LD_LIBRARY_PATH environment variable contents
        - 'pythonpath'       = <String>       = PYTHONPATH environment variable contents
        - 'jvm_workers'      = <String>       = Worker's jvm configuration (example: "-Xms1024m,-Xmx1024m,-Xmn400m")
        - 'cpu_affinity'     = <String>       = CPU affinity (default: automatic)
        - 'gpu_affinity'     = <String>       = GPU affinity (default: automatic)
        - 'fpga_affinity'    = <String>       = FPGA affinity (default: automatic)
        - 'fpga_reprogram'   = <String>       = FPGA reprogram command (default: '')
        - 'profile_input'    = <String>       = profiling input
        - 'profile_output'   = <String>       = profiling output
        - 'scheduler_config'    = <String>    = Path to the file which contains the scheduler configuration.
        - 'external_adaptation' = <String>    = Enable external adaptation. This option will disable the Resource Optimizer
        - 'python_interpreter'  = <String>    = Python interpreter
        - 'python_version'      = <String>    = Python interpreter version
        - 'python_virtual_environment'            = <String>  = Python virtual environment path
        - 'python_propagate_virtual_environment'  = <Boolean> = Propagate python virtual environment to workers

    :param config: Configuration parameters dictionary
    :return: None
    """
    from tempfile import mkstemp
    fd, temp_path = mkstemp()
    jvm_options_file = open(temp_path, 'w')

    # JVM GENERAL OPTIONS
    jvm_options_file.write('-XX:+PerfDisableSharedMem\n')
    jvm_options_file.write('-XX:-UsePerfData\n')
    jvm_options_file.write('-XX:+UseG1GC\n')
    jvm_options_file.write('-XX:+UseThreadPriorities\n')
    jvm_options_file.write('-XX:ThreadPriorityPolicy=42\n')
    if config['debug']:
        jvm_options_file.write('-Dlog4j.configurationFile=' + config[
            'compss_home'] + '/Runtime/configuration/log/COMPSsMaster-log4j.debug\n')  # DEBUG
    else:
        jvm_options_file.write('-Dlog4j.configurationFile=' + config[
            'compss_home'] + '/Runtime/configuration/log/COMPSsMaster-log4j\n')  # NO DEBUG
    jvm_options_file.write('-Dcompss.to.file=false\n')
    jvm_options_file.write('-Dcompss.project.file=' + config['project_xml'] + '\n')
    jvm_options_file.write('-Dcompss.resources.file=' + config['resources_xml'] + '\n')
    jvm_options_file.write(
        '-Dcompss.project.schema=' + config['compss_home'] + '/Runtime/configuration/xml/projects/project_schema.xsd\n')
    jvm_options_file.write(
        '-Dcompss.resources.schema=' + config[
            'compss_home'] + '/Runtime/configuration/xml/resources/resources_schema.xsd\n')
    jvm_options_file.write('-Dcompss.lang=python\n')
    if config['summary']:
        jvm_options_file.write('-Dcompss.summary=true\n')
    else:
        jvm_options_file.write('-Dcompss.summary=false\n')
    jvm_options_file.write('-Dcompss.task.execution=' + config['task_execution'] + '\n')
    if config['storage_conf'] is None:
        jvm_options_file.write('-Dcompss.storage.conf=null\n')
    else:
        jvm_options_file.write('-Dcompss.storage.conf=' + config['storage_conf'] + '\n')

    jvm_options_file.write('-Dcompss.core.count=' + str(config['task_count']) + '\n')

    jvm_options_file.write('-Dcompss.appName=' + config['app_name'] + '\n')

    if config['uuid'] is None:
        import uuid
        my_uuid = str(uuid.uuid4())
    else:
        my_uuid = config['uuid']

    jvm_options_file.write('-Dcompss.uuid=' + my_uuid + '\n')

    if config['base_log_dir'] is None:
        # it will be within $HOME/.COMPSs
        jvm_options_file.write('-Dcompss.baseLogDir=\n')
    else:
        jvm_options_file.write('-Dcompss.baseLogDir=' + config['base_log_dir'] + '\n')

    if config['specific_log_dir'] is None:
        jvm_options_file.write('-Dcompss.specificLogDir=\n')
    else:
        jvm_options_file.write('-Dcompss.specificLogDir=' + config['specific_log_dir'] + '\n')

    jvm_options_file.write('-Dcompss.appLogDir=/tmp/' + my_uuid + '/\n')

    if config['graph']:
        jvm_options_file.write('-Dcompss.graph=true\n')
    else:
        jvm_options_file.write('-Dcompss.graph=false\n')

    if config['monitor'] is None:
        jvm_options_file.write('-Dcompss.monitor=0\n')
    else:
        jvm_options_file.write('-Dcompss.monitor=' + str(config['monitor']) + '\n')

    if not config['trace'] or config['trace'] == 0:
        jvm_options_file.write('-Dcompss.tracing=0' + '\n')
    elif config['trace'] == 1:
        jvm_options_file.write('-Dcompss.tracing=1\n')
        os.environ['EXTRAE_CONFIG_FILE'] = config['compss_home'] + '/Runtime/configuration/xml/tracing/extrae_basic.xml'
    elif config['trace'] == 2:
        jvm_options_file.write('-Dcompss.tracing=2\n')
        os.environ['EXTRAE_CONFIG_FILE'] = config[
                                               'compss_home'] + '/Runtime/configuration/xml/tracing/extrae_advanced.xml'
    else:
        jvm_options_file.write('-Dcompss.tracing=0' + '\n')

    if config['extrae_cfg'] is None:
        jvm_options_file.write('-Dcompss.extrae.file=null\n')
    else:
        jvm_options_file.write('-Dcompss.extrae.file=' + config['extrae_cfg'] + '\n')

    if config['comm'] == 'GAT':
        jvm_options_file.write('-Dcompss.comm=es.bsc.compss.gat.master.GATAdaptor\n')
    else:
        jvm_options_file.write('-Dcompss.comm=es.bsc.compss.nio.master.NIOAdaptor\n')

    jvm_options_file.write('-Dcompss.conn=' + config['conn'] + '\n')
    jvm_options_file.write('-Dcompss.masterName=' + config['master_name'] + '\n')
    jvm_options_file.write('-Dcompss.masterPort=' + config['master_port'] + '\n')
    jvm_options_file.write('-Dcompss.scheduler=' + config['scheduler'] + '\n')
    jvm_options_file.write('-Dgat.adaptor.path=' + config['compss_home'] + '/Dependencies/JAVA_GAT/lib/adaptors\n')
    if config['debug']:
        jvm_options_file.write('-Dgat.debug=true\n')
    else:
        jvm_options_file.write('-Dgat.debug=false\n')
    jvm_options_file.write('-Dgat.broker.adaptor=sshtrilead\n')
    jvm_options_file.write('-Dgat.file.adaptor=sshtrilead\n')
    jvm_options_file.write(
        '-Dcompss.worker.cp=' + config['cp'] + ':' + config['compss_home'] + '/Runtime/compss-engine.jar:' + config[
            'classpath'] + '\n')
    jvm_options_file.write('-Dcompss.worker.jvm_opts=' + config['jvm_workers'] + '\n')
    jvm_options_file.write('-Dcompss.worker.cpu_affinity=' + config['cpu_affinity'] + '\n')
    jvm_options_file.write('-Dcompss.worker.gpu_affinity=' + config['gpu_affinity'] + '\n')
    jvm_options_file.write('-Dcompss.worker.fpga_affinity=' + config['fpga_affinity'] + '\n')
    jvm_options_file.write('-Dcompss.worker.fpga_reprogram=' + config['fpga_reprogram'] + '\n')
    jvm_options_file.write('-Dcompss.profile.input=' + config['profile_input'] + '\n')
    jvm_options_file.write('-Dcompss.profile.output=' + config['profile_output'] + '\n')
    jvm_options_file.write('-Dcompss.scheduler.config=' + config['scheduler_config'] + '\n')
    jvm_options_file.write('-Dcompss.external.adaptation=' + config['external_adaptation'] + '\n')

    # JVM OPTIONS - PYTHON
    jvm_options_file.write(
        '-Djava.class.path=' + config['cp'] + ':' + config['compss_home'] + '/Runtime/compss-engine.jar:' + config[
            'classpath'] + '\n')
    jvm_options_file.write('-Djava.library.path=' + config['ld_library_path'] + '\n')
    jvm_options_file.write('-Dcompss.worker.pythonpath=' + config['cp'] + ':' + config['pythonpath'] + '\n')
    jvm_options_file.write('-Dcompss.python.interpreter=' + config['python_interpreter'] + '\n')
    jvm_options_file.write('-Dcompss.python.version=' + config['python_version'] + '\n')
    jvm_options_file.write('-Dcompss.python.virtualenvironment=' + config['python_virtual_environment'] + '\n')
    if config['python_propagate_virtual_environment']:
        jvm_options_file.write('-Dcompss.python.propagate_virtualenvironment=true\n')
    else:
        jvm_options_file.write('-Dcompss.python.propagate_virtualenvironment=false\n')

    # Uncomment for debugging purposes
    # jvm_options_file.write('-Xcheck:jni\n')
    # jvm_options_file.write('-verbose:jni\n')

    # Close the file
    jvm_options_file.close()
    os.close(fd)
    os.environ['JVM_OPTIONS_FILE'] = temp_path

    # print("Uncomment if you want to check the configuration file path.")
    # print("JVM_OPTIONS_FILE: %s" % temp_path)


if __name__ == '__main__':
    """
    This is the PyCOMPSs entry point
    """
    compss_main()
