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
from pycompss.api.api import compss_start
from pycompss.api.api import compss_stop
from pycompss.runtime.binding import get_log_path
from pycompss.runtime.binding import get_pending_to_synchronize
from pycompss.runtime.launch import initialize_compss
from pycompss.util.logs import init_logging
import pycompss.runtime.binding as binding


# Warning! The name should start with 'InteractiveMode' due to @task checks
# it explicitly. If changed, it is necessary to update the task decorator.
app_path = 'InteractiveMode'
persistent_storage = False
myUuid = 0
running = False
process = None
log_path = '/tmp/'
graphing = False
tracing = False
monitoring = False


def start(log_level='off',
          o_c=False,
          debug=False,
          graph=False,
          trace=False,
          monitor=None,
          project_xml=None,
          resources_xml=None,
          summary=False,
          taskExecution='compss',
          storageConf=None,
          taskCount=50,
          appName='Interactive',
          uuid=None,
          baseLogDir=None,
          specificLogDir=None,
          extraeCfg=None,
          comm='NIO',
          conn='es.bsc.compss.connectors.DefaultSSHConnector',
          masterName='',
          masterPort='',
          scheduler='es.bsc.compss.scheduler.loadBalancingScheduler.LoadBalancingScheduler',
          jvmWorkers='-Xms1024m,-Xmx1024m,-Xmn400m',
          cpuAffinity='automatic',
          gpuAffinity='automatic',
          profileInput='',
          profileOutput='',
          scheduler_config='',
          external_adaptation=False,
          verbose=False
          ):
    launchPath = os.path.dirname(os.path.realpath(__file__))
    # compss_home = launchPath without the last 4 folders:
    # Bindings/python/version/pycompss
    compss_home = os.path.sep.join(launchPath.split(os.path.sep)[:-4])
    os.environ['COMPSS_HOME'] = compss_home

    # Get environment variables
    cp = os.getcwd() + '/'
    pythonPath = os.environ['PYTHONPATH']
    classPath = os.environ['CLASSPATH']
    ld_library_path = os.environ['LD_LIBRARY_PATH']

    # Set extrae dependencies
    extrae_home = compss_home + '/Dependencies/extrae'
    extrae_lib = extrae_home + '/lib'
    os.environ['EXTRAE_HOME'] = extrae_home
    os.environ['LD_LIBRARY_PATH'] = extrae_lib + ':' + ld_library_path

    if trace is False:
        trace = 0
    elif trace == 'basic' or trace is True:
        trace = 1
        os.environ['LD_PRELOAD'] = extrae_lib + '/libpttrace.so'
    elif trace == 'advanced':
        trace = 2
        os.environ['LD_PRELOAD'] = extrae_lib + '/libpttrace.so'
    else:
        print('ERROR: Wrong tracing parameter ( [ True | basic ] | \
               advanced | False)')
        return -1

    if monitor is not None:
        # Enable the graph if the monitoring is enabled
        graph = True

    global graphing
    graphing = graph
    global tracing
    tracing = trace
    global monitoring
    monitoring = monitor

    __export_globals__()

    print("********************************************************")
    print("*************** PyCOMPSs Interactive *******************")
    print("********************************************************")
    print("*          .-~~-.--.                ____        ____   *")
    print("*         :         )              |___ \      |___ \  *")
    print("*   .~ ~ -.\       /.- ~~ .          __) |       __) | *")
    print("*   >       `.   .'       <         / __/   _   / __/  *")
    print("*  (         .- -.         )       |_____| |_| |_____| *")
    print("*   `- -.-~  `- -'  ~-.- -'                            *")
    print("*     (        :        )           _ _ .-:            *")
    print("*      ~--.    :    .--~        .-~  .-~  }            *")
    print("*          ~-.-^-.-~ \_      .~  .-~   .~              *")
    print("*                   \ \ '     \ '_ _ -~                *")
    print("*                    \`.\`.    //                      *")
    print("*           . - ~ ~-.__\`.\`-.//                       *")
    print("*       .-~   . - ~  }~ ~ ~-.~-.                       *")
    print("*     .' .-~      .-~       :/~-.~-./:                 *")
    print("*    /_~_ _ . - ~                 ~-.~-._              *")
    print("*                                     ~-.<             *")
    print("********************************************************")

    # print("*          .-~~-.--.                ____       _____   *")
    # print("*         :         )              |___ \     |___ /   *")
    # print("*   .~ ~ -.\       /.- ~~ .          __) |      |_ \   *")
    # print("*   >       `.   .'       <         / __/   _   __) |  *")
    # print("*  (         .- -.         )       |_____| |_| |___/   *")

    ##############################################################
    # INITIALIZATION
    ##############################################################

    # Build a dictionary with all variables needed for initializing the runtime
    config = {}
    config['compss_home'] = compss_home
    config['debug'] = debug
    if project_xml is None:
        projPath = 'Runtime/configuration/xml/projects/default_project.xml'
        config['project_xml'] = compss_home + os.path.sep + projPath
    else:
        config['project_xml'] = project_xml
    if resources_xml is None:
        resPath = 'Runtime/configuration/xml/resources/default_resources.xml'
        config['resources_xml'] = compss_home + os.path.sep + resPath
    else:
        config['resources_xml'] = resources_xml
    config['summary'] = summary
    config['taskExecution'] = taskExecution
    config['storageConf'] = storageConf
    config['taskCount'] = taskCount
    if appName is None:
        config['appName'] = 'Interactive'
    else:
        config['appName'] = appName
    config['uuid'] = uuid
    config['baseLogDir'] = baseLogDir
    config['specificLogDir'] = specificLogDir
    config['graph'] = graph
    config['monitor'] = monitor
    config['trace'] = trace
    config['extraeCfg'] = extraeCfg
    config['comm'] = comm
    config['conn'] = conn
    config['masterName'] = masterName
    config['masterPort'] = masterPort
    config['scheduler'] = scheduler
    config['cp'] = cp
    config['classpath'] = classPath
    config['jvmWorkers'] = jvmWorkers
    config['pythonPath'] = pythonPath
    config['cpuAffinity'] = cpuAffinity
    config['gpuAffinity'] = gpuAffinity
    config['profileInput'] = profileInput
    config['profileOutput'] = profileOutput
    config['scheduler_config'] = scheduler_config
    if external_adaptation:
        config['external_adaptation'] = 'true'
    else:
        config['external_adaptation'] = 'false'

    major_version = sys.version_info[0]
    python_interpreter = 'python' + str(major_version)
    config['python_interpreter'] = python_interpreter

    if 'VIRTUAL_ENV' in os.environ:
        # Running within a virtual environment
        python_virtual_environment = os.environ['VIRTUAL_ENV']
    else:
        python_virtual_environment = 'null'

    initialize_compss(config)

    ##############################################################
    # RUNTIME START
    ##############################################################

    print("* - Starting COMPSs runtime...                         *")
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

    # Logging setup
    if log_level == "debug":
        jsonPath = '/Bindings/python/' + str(major_version) + '/log/logging.json.debug'
        init_logging(os.getenv('COMPSS_HOME') + jsonPath, log_path)
    elif log_level == "info":
        jsonPath = '/Bindings/python/' + str(major_version) + '/log/logging.json.off'
        init_logging(os.getenv('COMPSS_HOME') + jsonPath, log_path)
    elif log_level == "off":
        jsonPath = '/Bindings/python/' + str(major_version) + '/log/logging.json.off'
        init_logging(os.getenv('COMPSS_HOME') + jsonPath, log_path)
    else:
        # Default
        jsonPath = '/Bindings/python/' + str(major_version) + '/log/logging.json'
        init_logging(os.getenv('COMPSS_HOME') + jsonPath, log_path)
    logger = logging.getLogger("pycompss.runtime.launch")

    printSetup(verbose,
               log_level, o_c, debug, graph, trace, monitor,
               project_xml, resources_xml, summary, taskExecution, storageConf,
               taskCount, appName, uuid, baseLogDir, specificLogDir, extraeCfg,
               comm, conn, masterName, masterPort, scheduler, jvmWorkers,
               cpuAffinity, gpuAffinity, profileInput, profileOutput,
               scheduler_config, external_adaptation, python_interpreter,
               python_virtual_environment)

    logger.debug("--- START ---")
    logger.debug("PyCOMPSs Log path: %s" % log_path)
    if storageConf is not None:
        logger.debug("Storage configuration file: %s" % storageConf)
        from storage.api import init as initStorage
        initStorage(config_file_path=storageConf)
        global persistent_storage
        persistent_storage = True

    # MAIN EXECUTION
    # let the user write an interactive application
    print("* - PyCOMPSs Runtime started... Have fun!              *")
    print("********************************************************")


def printSetup(verbose, log_level, o_c, debug, graph, trace, monitor,
               project_xml, resources_xml, summary, taskExecution, storageConf,
               taskCount, appName, uuid, baseLogDir, specificLogDir, extraeCfg,
               comm, conn, masterName, masterPort, scheduler, jvmWorkers,
               cpuAffinity, gpuAffinity, profileInput, profileOutput,
               scheduler_config, external_adaptation, python_interpreter,
               python_virtual_environment):
    logger = logging.getLogger("pycompss.runtime.launch")
    output = ""
    output += "********************************************************\n"
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
    output += "  - Task execution      : " + str(taskExecution) + "\n"
    output += "  - Storage conf.       : " + str(storageConf) + "\n"
    output += "  - Task count          : " + str(taskCount) + "\n"
    output += "  - Application name    : " + str(appName) + "\n"
    output += "  - UUID                : " + str(uuid) + "\n"
    output += "  - Base log dir.       : " + str(baseLogDir) + "\n"
    output += "  - Specific log dir.   : " + str(specificLogDir) + "\n"
    output += "  - Extrae CFG          : " + str(extraeCfg) + "\n"
    output += "  - COMM library        : " + str(comm) + "\n"
    output += "  - CONN library        : " + str(conn) + "\n"
    output += "  - Master name         : " + str(masterName) + "\n"
    output += "  - Master port         : " + str(masterPort) + "\n"
    output += "  - Scheduler           : " + str(scheduler) + "\n"
    output += "  - JVM Workers         : " + str(jvmWorkers) + "\n"
    output += "  - CPU affinity        : " + str(cpuAffinity) + "\n"
    output += "  - GPU affinity        : " + str(gpuAffinity) + "\n"
    output += "  - Profile input       : " + str(profileInput) + "\n"
    output += "  - Profile output      : " + str(profileOutput) + "\n"
    output += "  - Scheduler config    : " + str(scheduler_config) + "\n"
    output += "  - External adaptation : " + str(external_adaptation) + "\n"
    output += "  - Python interpreter  : " + str(python_interpreter) + "\n"
    output += "  - Python virtualenv   : " + str(python_virtual_environment) + "\n"
    output += "********************************************************"
    if verbose:
        print(output)
    logger.debug(output)


def stop(sync=False):
    print("******************************************************")
    print("**************** STOPPING PyCOMPSs *******************")
    print("******************************************************")

    logger = logging.getLogger("pycompss.runtime.launch")

    if sync:
        print("Synchronizing all future objects left on the user scope.")
        logger.debug("Synchronizing all future objects left on the user scope.")
        from pycompss.api.api import compss_wait_on
        pending_to_synchronize = get_pending_to_synchronize()

        ipython = globals()['__builtins__']['get_ipython']()
        # import pprint
        # pprint.pprint(ipython.__dict__, width=1)
        raw_code = ipython.__dict__['user_ns']
        for k in raw_code:
            objK = raw_code[k]
            if not k.startswith('_'):   # not internal objects
                if type(objK) == binding.Future:
                    print("Found a future object: %s" % str(k))
                    logger.debug("Found a future object: %s" % (k,))
                    ipython.__dict__['user_ns'][k] = compss_wait_on(objK)
                elif objK in pending_to_synchronize.values():
                    print("Found an object to synchronize: %s" % str(k))
                    logger.debug("Found an object to synchronize: %s" % (k,))
                    ipython.__dict__['user_ns'][k] = compss_wait_on(objK)
                else:
                    pass
    else:
        print("Warning: some of the variables used with PyCOMPSs may")
        print("         have not been brought to the master.")

    if persistent_storage is True:
        from storage.api import finish as finishStorage
        finishStorage()

    compss_stop()

    __clean_temp_files__()

    print("******************************************************")
    logger.debug("--- END ---")
    # os._exit(00)  # Explicit kernel restart # breaks Jupyter-notebook

    # --- Execution finished ---


def __show_current_graph__(fit=False):
    if graphing:
        return __show_graph__(name='current_graph', fit=fit)
    else:
        print('Oops! Graph is not enabled in this execution.')
        print('      Please, enable it by setting the graph flag when starting PyCOMPSs.')


def __show_complete_graph__(fit=False):
    if graphing:
        return __show_graph__(name='complete_graph', fit=fit)
    else:
        print('Oops! Graph is not enabled in this execution.')
        print('      Please, enable it by setting the graph flag when starting PyCOMPSs.')


def __show_graph__(name='complete_graph', fit=False):
    try:
        from graphviz import Source
    except ImportError:
        print('Oops! graphviz is not available.')
        raise
    file = open(log_path + '/monitor/' + name + '.dot', 'r')
    text = file.read()
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
        except:
            print('Oops! Failed rendering the graph.')
            raise
    else:
        return Source(text)


###############################################################################
###############################################################################
###############################################################################


def __export_globals__():
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
    userGlobals = ipython.__dict__['ns_table']['user_global']
    # Inject app_path variable to user globals so that task and constraint
    # decorators can get it.
    temp_app_filename = os.getcwd() + '/' + "InteractiveMode_" + str(time.strftime('%d%m%y_%H%M%S')) + '.py'
    userGlobals['app_path'] = temp_app_filename
    global app_path
    app_path = temp_app_filename


def __clean_temp_files__():
    '''
    Remove any temporary files that may exist.
    Currently: app_path, which contains the file path where all interactive
    code required by the worker is.
    '''
    try:
        if os.path.exists(app_path):
            os.remove(app_path)
        if os.path.exists(app_path + 'c'):
            os.remove(app_path + 'c')
    except OSError:
        print("[ERROR] An error has occurred when cleaning temporary files.")
