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
PyCOMPSs Binding - Interactive API
==================================
Provides the current start and stop for the use of pycompss interactively.
"""

import os
import sys
import logging
from tempfile import mkdtemp
import time

# Let the Python binding know we are during libraries initialization
from pycompss.util.location import set_pycompss_context
set_pycompss_context('INITIALIZATION')

import pycompss.runtime.binding as binding
from pycompss.api.api import compss_start
from pycompss.api.api import compss_stop
from pycompss.runtime.binding import get_log_path
from pycompss.runtime.binding import pending_to_synchronize
from pycompss.runtime.launch import initialize_compss
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

# Warning! The name should start with 'InteractiveMode' due to @task checks
# it explicitly. If changed, it is necessary to update the task decorator.
app_path = 'InteractiveMode'
persistent_storage = False
myUuid = 0
process = None
log_path = '/tmp/'
graphing = False


def start(log_level='off',
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
          app_name='Interactive',
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
          propagate_virtual_environment=False,
          verbose=False
          ):
    """
    Start the runtime in interactive mode.

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
    :param cpu_affinity: CPU Core affinity (default: 'automatic')
    :param gpu_affinity: GPU Core affinity (default: 'automatic')
    :param fpga_affinity: FPA Core affinity (default: 'automatic')
    :param fpga_reprogram: FPGA repogram command (default: '')
    :param profile_input: Input profile  (default: '')
    :param profile_output: Output profile  (default: '')
    :param scheduler_config: Scheduler configuration  (default: '')
    :param external_adaptation: External adaptation [ True | False ] (default: False)
    :param propagate_virtual_environment: Propagate virtual environment [ True | False ] (default: False)
    :param verbose: Verbose mode [ True | False ] (default: False)
    :return: None
    """

    # Let the Python binding know we are at master
    set_pycompss_context('MASTER')

    # Prepare the environment
    launch_path = os.path.dirname(os.path.realpath(__file__))
    # compss_home = launch_path without the last 4 folders:
    # Bindings/python/version/pycompss
    compss_home = os.path.sep.join(launch_path.split(os.path.sep)[:-4])
    os.environ['COMPSS_HOME'] = compss_home

    # Get environment variables
    cp = os.getcwd() + '/'
    pythonpath = os.environ['PYTHONPATH']
    classpath = os.environ['CLASSPATH']
    ld_library_path = os.environ['LD_LIBRARY_PATH']

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

    # Include extrae into ld_library_path
    ld_library_path = extrae_lib + ':' + ld_library_path
    os.environ['LD_LIBRARY_PATH'] = ld_library_path

    if monitor is not None:
        # Enable the graph if the monitoring is enabled
        graph = True

    # Export global variables
    global graphing
    graphing = graph

    __export_globals__()

    print("******************************************************")
    print("*************** PyCOMPSs Interactive *****************")
    print("******************************************************")
    print("*          .-~~-.--.            ____        _    _   *")
    print("*         :         )          |___ \      | |  | |  *")
    print("*   .~ ~ -.\       /.- ~~ .      __) |     | |__| |  *")
    print("*   >       `.   .'       <     / __/   _  |____  |  *")
    print("*  (         .- -.         )   |_____| |_|      |_|  *")
    print("*   `- -.-~  `- -'  ~-.- -'                          *")
    print("*     (        :        )           _ _ .-:          *")
    print("*      ~--.    :    .--~        .-~  .-~  }          *")
    print("*          ~-.-^-.-~ \_      .~  .-~   .~            *")
    print("*                   \ \ '     \ '_ _ -~              *")
    print("*                    \`.\`.    //                    *")
    print("*           . - ~ ~-.__\`.\`-.//                     *")
    print("*       .-~   . - ~  }~ ~ ~-.~-.                     *")
    print("*     .' .-~      .-~       :/~-.~-./:               *")
    print("*    /_~_ _ . - ~                 ~-.~-._            *")
    print("*                                     ~-.<           *")
    print("******************************************************")

    ##############################################################
    # INITIALIZATION
    ##############################################################

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
        trace = get_tracing()
        if verbose:
            print("- Overridden project xml with: " + project_xml)
            print("- Overridden resources xml with: " + resources_xml)
            print("- Overridden master name with: " + master_name)
            print("- Overridden master port with: " + master_port)
            print("- Overridden uuid with: " + uuid)
            print("- Overridden base log dir with: " + base_log_dir)
            print("- Overridden specific log dir with: " + specific_log_dir)
            print("- Overridden storage conf with: " + storage_conf)
            print("- Overridden log level with: " + str(log_level))
            print("- Overridden debug with: " + str(debug))
            print("- Overridden trace with: " + str(trace))

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

    # Build a dictionary with all variables needed for initializing the runtime
    config = dict()
    config['compss_home'] = compss_home
    config['debug'] = debug
    if project_xml is None:
        project_path = 'Runtime/configuration/xml/projects/default_project.xml'
        config['project_xml'] = compss_home + os.path.sep + project_path
    else:
        config['project_xml'] = project_xml
    if resources_xml is None:
        resources_path = 'Runtime/configuration/xml/resources/default_resources.xml'
        config['resources_xml'] = compss_home + os.path.sep + resources_path
    else:
        config['resources_xml'] = resources_xml
    config['summary'] = summary
    config['task_execution'] = task_execution
    config['storage_conf'] = storage_conf
    config['task_count'] = task_count
    if app_name is None:
        config['app_name'] = 'Interactive'
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
    config['pythonpath'] = pythonpath
    config['classpath'] = classpath
    config['ld_library_path'] = ld_library_path
    config['jvm_workers'] = jvm_workers
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

    major_version = sys.version_info[0]
    python_interpreter = 'python' + str(major_version)
    config['python_interpreter'] = python_interpreter
    config['python_version'] = str(major_version)

    if 'VIRTUAL_ENV' in os.environ:
        # Running within a virtual environment
        python_virtual_environment = os.environ['VIRTUAL_ENV']
    else:
        python_virtual_environment = 'null'
    config['python_virtual_environment'] = python_virtual_environment
    config['python_propagate_virtual_environment'] = propagate_virtual_environment

    initialize_compss(config)

    ##############################################################
    # RUNTIME START
    ##############################################################

    print("* - Starting COMPSs runtime...                       *")
    sys.stdout.flush()  # Force flush
    compss_start()

    if o_c is True:
        # set cross-module variable
        binding.object_conversion = True
    else:
        # set cross-module variable
        binding.object_conversion = False

    # Remove launch.py, log_level and object_conversion from sys.argv,
    # It will be inherited by the app through execfile
    # sys.argv = sys.argv[3:]
    # Get application execution path
    # app_path = sys.argv[0]  # not needed in interactive mode

    global log_path
    log_path = get_log_path()
    binding.temp_dir = mkdtemp(prefix='pycompss', dir=log_path + '/tmpFiles/')
    print("* - Log path : " + log_path)

    # Logging setup - messages before this step are ignored (need log_path to configure the logger).
    if debug or log_level == "debug":
        json_path = '/Bindings/python/' + str(major_version) + '/log/logging_debug.json'
        init_logging(os.getenv('COMPSS_HOME') + json_path, log_path)
    elif log_level == "info":
        json_path = '/Bindings/python/' + str(major_version) + '/log/logging_off.json'
        init_logging(os.getenv('COMPSS_HOME') + json_path, log_path)
    elif log_level == "off":
        json_path = '/Bindings/python/' + str(major_version) + '/log/logging_off.json'
        init_logging(os.getenv('COMPSS_HOME') + json_path, log_path)
    else:
        # Default
        json_path = '/Bindings/python/' + str(major_version) + '/log/logging.json'
        init_logging(os.getenv('COMPSS_HOME') + json_path, log_path)
    logger = logging.getLogger("pycompss.runtime.launch")

    __print_setup__(verbose,
                    log_level, o_c, debug, graph, trace, monitor,
                    project_xml, resources_xml, summary, task_execution, storage_conf,
                    pythonpath, classpath, ld_library_path,
                    task_count, app_name, uuid, base_log_dir, specific_log_dir, extrae_cfg,
                    comm, conn, master_name, master_port, scheduler, jvm_workers,
                    cpu_affinity, gpu_affinity, fpga_affinity, fpga_reprogram, profile_input, profile_output,
                    scheduler_config, external_adaptation, python_interpreter, major_version,
                    python_virtual_environment, propagate_virtual_environment)

    logger.debug("--- START ---")
    logger.debug("PyCOMPSs Log path: %s" % log_path)
    if storage_conf is not None and not storage_conf == 'null':
        logger.debug("Storage configuration file: %s" % storage_conf)
        from storage.api import init as init_storage
        init_storage(config_file_path=storage_conf)
        global persistent_storage
        persistent_storage = True

    # MAIN EXECUTION
    # let the user write an interactive application
    print("* - PyCOMPSs Runtime started... Have fun!            *")
    print("******************************************************")


def __print_setup__(verbose, log_level, o_c, debug, graph, trace, monitor,
                    project_xml, resources_xml, summary, task_execution, storage_conf,
                    pythonpath, classpath, ld_library_path,
                    task_count, app_name, uuid, base_log_dir, specific_log_dir, extrae_cfg,
                    comm, conn, master_name, master_port, scheduler, jvm_workers,
                    cpu_affinity, gpu_affinity, fpga_affinity, fpga_reprogram, profile_input, profile_output,
                    scheduler_config, external_adaptation, python_interpreter, python_version,
                    python_virtual_environment, python_propagate_virtual_environment):

    logger = logging.getLogger("pycompss.runtime.launch")
    output = ""
    output += "******************************************************\n"
    output += " CONFIGURATION: \n"
    output += "  - Log level           : " + str(log_level) + "\n"
    output += "  - Object conversion   : " + str(o_c) + "\n"
    output += "  - Debug               : " + str(debug) + "\n"
    output += "  - Graph               : " + str(graph) + "\n"
    output += "  - Trace               : " + str(trace) + "\n"
    output += "  - Monitor             : " + str(monitor) + "\n"
    output += "  - Project XML         : " + str(project_xml) + "\n"
    output += "  - Resources XML       : " + str(resources_xml) + "\n"
    output += "  - Summary             : " + str(summary) + "\n"
    output += "  - Task execution      : " + str(task_execution) + "\n"
    output += "  - Storage conf.       : " + str(storage_conf) + "\n"
    output += "  - Pythonpath          : " + str(pythonpath) + "\n"
    output += "  - Classpath           : " + str(classpath) + "\n"
    output += "  - Ld_library_path     : " + str(ld_library_path) + "\n"
    output += "  - Task count          : " + str(task_count) + "\n"
    output += "  - Application name    : " + str(app_name) + "\n"
    output += "  - UUID                : " + str(uuid) + "\n"
    output += "  - Base log dir.       : " + str(base_log_dir) + "\n"
    output += "  - Specific log dir.   : " + str(specific_log_dir) + "\n"
    output += "  - Extrae CFG          : " + str(extrae_cfg) + "\n"
    output += "  - COMM library        : " + str(comm) + "\n"
    output += "  - CONN library        : " + str(conn) + "\n"
    output += "  - Master name         : " + str(master_name) + "\n"
    output += "  - Master port         : " + str(master_port) + "\n"
    output += "  - Scheduler           : " + str(scheduler) + "\n"
    output += "  - JVM Workers         : " + str(jvm_workers) + "\n"
    output += "  - CPU affinity        : " + str(cpu_affinity) + "\n"
    output += "  - GPU affinity        : " + str(gpu_affinity) + "\n"
    output += "  - FPGA affinity       : " + str(fpga_affinity) + "\n"
    output += "  - FPGA reprogram      : " + str(fpga_reprogram) + "\n"
    output += "  - Profile input       : " + str(profile_input) + "\n"
    output += "  - Profile output      : " + str(profile_output) + "\n"
    output += "  - Scheduler config    : " + str(scheduler_config) + "\n"
    output += "  - External adaptation : " + str(external_adaptation) + "\n"
    output += "  - Python interpreter  : " + str(python_interpreter) + "\n"
    output += "  - Python version      : " + str(python_version) + "\n"
    output += "  - Python virtualenv   : " + str(python_virtual_environment) + "\n"
    output += "  - Python propagate virtualenv : " + str(python_propagate_virtual_environment) + "\n"
    output += "******************************************************"
    if verbose:
        print(output)
    logger.debug(output)


def stop(sync=False):
    """
    Runtime stop.

    :param sync: Scope variables synchronization [ True | False ] (default: False)
    :return: None
    """
    print("****************************************************")
    print("*************** STOPPING PyCOMPSs ******************")
    print("****************************************************")

    logger = logging.getLogger("pycompss.runtime.launch")

    if sync:
        print("Synchronizing all future objects left on the user scope.")
        logger.debug("Synchronizing all future objects left on the user scope.")
        from pycompss.api.api import compss_wait_on

        ipython = globals()['__builtins__']['get_ipython']()
        # import pprint
        # pprint.pprint(ipython.__dict__, width=1)
        raw_code = ipython.__dict__['user_ns']
        for k in raw_code:
            obj_k = raw_code[k]
            if not k.startswith('_'):   # not internal objects
                if type(obj_k) == binding.Future:
                    print("Found a future object: %s" % str(k))
                    logger.debug("Found a future object: %s" % (k,))
                    ipython.__dict__['user_ns'][k] = compss_wait_on(obj_k)
                elif obj_k in pending_to_synchronize.values():
                    print("Found an object to synchronize: %s" % str(k))
                    logger.debug("Found an object to synchronize: %s" % (k,))
                    ipython.__dict__['user_ns'][k] = compss_wait_on(obj_k)
                else:
                    pass
    else:
        print("Warning: some of the variables used with PyCOMPSs may")
        print("         have not been brought to the master.")

    if persistent_storage is True:
        from storage.api import finish as finish_storage
        finish_storage()

    compss_stop()

    __clean_temp_files__()

    print("****************************************************")
    logger.debug("--- END ---")
    # os._exit(00)  # Explicit kernel restart # breaks Jupyter-notebook

    # --- Execution finished ---


def __show_current_graph__(fit=False):
    """
    Show current graph.

    :param fit: Fit to width [ True | False ] (default: False)
    :return: None
    """

    if graphing:
        return __show_graph__(name='current_graph', fit=fit)
    else:
        print('Oops! Graph is not enabled in this execution.')
        print('      Please, enable it by setting the graph flag when starting PyCOMPSs.')


def __show_complete_graph__(fit=False):
    """
    Show complete graph.

    :param fit: Fit to width [ True | False ] (default: False)
    :return: None
    """

    if graphing:
        return __show_graph__(name='complete_graph', fit=fit)
    else:
        print('Oops! Graph is not enabled in this execution.')
        print('      Please, enable it by setting the graph flag when starting PyCOMPSs.')


def __show_graph__(name='complete_graph', fit=False):
    """
    Show graph.

    :param name: Graph to show (default: 'complete_graph')
    :param fit: Fit to width [ True | False ] (default: False)
    :return: None
    """

    try:
        from graphviz import Source
    except ImportError:
        print('Oops! graphviz is not available.')
        raise
    monitor_file = open(log_path + '/monitor/' + name + '.dot', 'r')
    text = monitor_file.read()
    monitor_file.close()
    if fit:
        try:
            # Convert to png and show full picture
            filename = log_path + '/monitor/' + name
            extension = 'png'
            import os
            if os.path.exists(filename + '.' + extension):
                os.remove(filename + '.' + extension)
            s = Source(text, filename=filename, format=extension)
            s.render()
            from IPython.display import Image
            image = Image(filename=filename + '.' + extension)
            return image
        except Exception:
            print('Oops! Failed rendering the graph.')
            raise
    else:
        return Source(text)


###############################################################################
###############################################################################
###############################################################################


def __export_globals__():
    """
    Export globals into interactive environment.

    :return: None
    """

    # Super ugly, but I see no other way to define the app_path across the
    # interactive execution without making the user to define it explicitly.
    # It is necessary to define only one app_path because of the two decorators
    # need to access the same information.
    # if the file is created per task, the constraint will not be able to work.
    # Get ipython globals
    ipython = globals()['__builtins__']['get_ipython']()
    # import pprint
    # pprint.pprint(ipython.__dict__, width=1)
    # Extract user globals from ipython
    user_globals = ipython.__dict__['ns_table']['user_global']
    # Inject app_path variable to user globals so that task and constraint
    # decorators can get it.
    temp_app_filename = os.getcwd() + '/' + "InteractiveMode_" + str(time.strftime('%d%m%y_%H%M%S')) + '.py'
    user_globals['app_path'] = temp_app_filename
    global app_path
    app_path = temp_app_filename


def __clean_temp_files__():
    """
    Remove any temporary files that may exist.
    Currently: app_path, which contains the file path where all interactive code required by the worker is.

    :return: None
    """

    try:
        if os.path.exists(app_path):
            os.remove(app_path)
        if os.path.exists(app_path + 'c'):
            os.remove(app_path + 'c')
    except OSError:
        print("[ERROR] An error has occurred when cleaning temporary files.")
