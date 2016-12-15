"""
@author: fconejer

PyCOMPSs Binding - Interactive API
==================================
Provides the current start and stop for the use of pycompss interactively.
"""

import os
import logging
from tempfile import mkdtemp
from pycompss.api.api import compss_start, compss_stop
from pycompss.runtime.binding import get_logPath
from pycompss.util.logs import init_logging
import pycompss.runtime.binding as binding
from pycompss.runtime.binding import get_task_objects
from random import randint
import time
from multiprocessing import Process


try:
    # Import storage libraries if possible
    from storage.api import init as initStorage
    from storage.api import finish as finishStorage
except ImportError:
    # If not present, import dummy functions
    from pycompss.storage.api import init as initStorage
    from pycompss.storage.api import finish as finishStorage


storage = False
myUuid = 0
app_path = "InteractiveMode"
running = False
process = None

# os.environ['IT_HOME'] + '/Runtime/configuration/xml/projects/default_project.xml',
# os.environ['IT_HOME'] + '/Runtime/configuration/xml/resources/default_resources.xml',

'''
def startP(log_level="off",
          o_c=False,
          debug=False,
          graph=False,
          trace=False,
          monitor=None,
          project_xml='/opt/COMPSs/Runtime/configuration/xml/projects/default_project.xml',
          resources_xml='/opt/COMPSs/Runtime/configuration/xml/resources/default_resources.xml',
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
          masterName='',
          masterPort='43000',
          scheduler='integratedtoolkit.scheduler.defaultscheduler.DefaultScheduler',
          jvmWorkers='-Xms1024m,-Xmx1024m,-Xmn400m'
          ):
    global running
    global process
    if running:
        print "You have currently a running PyCOMPSs instance."
    else:
        print "[iPyCOMPSs] Starting process..."

        exportGlobals()

        process = Process(target=start, args=(log_level, o_c, debug, graph, trace, monitor,
                                              project_xml, resources_xml, summary, taskExecution,
                                              storageConf, taskCount, appName, uuid, baseLogDir,
                                              specificLogDir, extraeCfg, comm, masterName,
                                              masterPort, scheduler, jvmWorkers, True))
        process.daemon = True
        process.start()
        print "[iPyCOMPSs] Process started."
        running = True
'''



def start(log_level="off",
          o_c=False,
          debug=False,
          graph=False,
          trace=False,
          monitor=None,
          project_xml='/opt/COMPSs/Runtime/configuration/xml/projects/default_project.xml',
          resources_xml='/opt/COMPSs/Runtime/configuration/xml/resources/default_resources.xml',
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
          masterName='',
          masterPort='43000',
          scheduler='integratedtoolkit.scheduler.defaultscheduler.DefaultScheduler',
          jvmWorkers='-Xms1024m,-Xmx1024m,-Xmn400m',
          forked=False,
          ):
    it_home = os.environ['IT_HOME']
    pythonPath = os.environ['PYTHONPATH'] + ':' + os.getcwd() + '/'
    classpath = os.environ['CLASSPATH']
    extrae_home = it_home + '/Dependencies/extrae'
    extrae_lib = extrae_home + '/lib'
    ld_library_path = os.environ['LD_LIBRARY_PATH']
    os.environ['EXTRAE_HOME'] = extrae_home
    os.environ['LD_LIBRARY_PATH'] = extrae_lib + ':' + ld_library_path

    if trace == False:
        trace = 0
    elif trace == 'basic' or trace == True:
        trace = 1
    elif trace == 'advanced':
        trace = 2
    else:
        print 'ERROR: Wrong tracing parameter ( [ True | basic ] | advanced | False)'
        return -1

    '''
    # Super ugly, but I see no other way to define the app_path across the interactive execution without
    # making the user to define it explicitly.
    # It is necessary to define only one app_path because of the two decorators need to access the same information.
    # if the file is created per task, the constraint will not be able to work.
    # Get ipython globals
    ipython = globals()['__builtins__']['get_ipython']()
    # import pprint
    # pprint.pprint(ipython.__dict__, width=1)
    # Extract user globals from ipython
    userGlobals = ipython.__dict__['ns_table']['user_global']
    # Inject app_path variable to user globals so that task and constraint decorators can get it.
    # userGlobals['app_path'] = os.getcwd() + '/' + "InteractiveMode" + str(randint(20000, 40000)) + '.py'
    userGlobals['app_path'] = os.getcwd() + '/' + "InteractiveMode" + str(time.strftime('%d%m%y_%H%M%S')) + '.py'
    '''
    exportGlobals()

    print "******************************************************"
    print "*************** PyCOMPSs Interactive *****************"
    print "******************************************************"
    print "** Initialized in localhost"
    print "** IT_HOME = " + it_home
    print "** PYTHONPATH = " + pythonPath
    print "** CLASSPATH = " + classpath
    print "******************************************************"

    ##############################################################
    # INITIALIZATION
    ##############################################################

    from tempfile import mkstemp
    fd, temp_path = mkstemp()
    jvm_options_file = open(temp_path, 'w')

    jvm_options_file.write('-XX:+PerfDisableSharedMem\n')
    jvm_options_file.write('-XX:-UsePerfData\n')
    jvm_options_file.write('-XX:+UseG1GC\n')
    jvm_options_file.write('-XX:+UseThreadPriorities\n')
    jvm_options_file.write('-XX:ThreadPriorityPolicy=42\n')
    if debug:
        jvm_options_file.write('-Dlog4j.configurationFile=' + it_home + '/Runtime/configuration/log/COMPSsMaster-log4j.debug\n')  # DEBUG
        log_level = "debug"
    else:
        jvm_options_file.write('-Dlog4j.configurationFile=' + it_home + '/Runtime/configuration/log/COMPSsMaster-log4j\n')       # NO DEBUG
    jvm_options_file.write('-Dit.to.file=false\n')
    jvm_options_file.write('-Dit.project.file=' + project_xml + '\n')
    jvm_options_file.write('-Dit.resources.file=' + resources_xml + '\n')
    jvm_options_file.write('-Dit.project.schema=' + it_home + '/Runtime/configuration/xml/projects/project_schema.xsd\n')
    jvm_options_file.write('-Dit.resources.schema=' + it_home + '/Runtime/configuration/xml/resources/resources_schema.xsd\n')
    jvm_options_file.write('-Dit.lang=python\n')
    if summary:
        jvm_options_file.write('-Dit.summary=true\n')
    else:
        jvm_options_file.write('-Dit.summary=false\n')
    jvm_options_file.write('-Dit.task.execution=' + taskExecution + '\n')
    if storageConf is None:
        jvm_options_file.write('-Dit.storage.conf=\n')
    else:
        jvm_options_file.write('-Dit.storage.conf=' + storageConf + '\n')
    jvm_options_file.write('-Dit.core.count=' + str(taskCount) + '\n')
    jvm_options_file.write('-Dit.appName=' + appName + '\n')
    if uuid is None:
        myUuid = str(randint(0,1000))
    else:
        myUuid = uuid
    jvm_options_file.write('-Dit.uuid=' + myUuid + '\n')
    if baseLogDir != None:
        jvm_options_file.write('-Dit.baseLogDir=' + baseLogDir + '\n')
    else:
        pass   # it will be within $HOME/.COMPSs
    if specificLogDir is None:
        jvm_options_file.write('-Dit.specificLogDir=\n')
    else:
        jvm_options_file.write('-Dit.specificLogDir=' + specificLogDir + '\n')
    jvm_options_file.write('-Dit.appLogDir=/tmp/' + myUuid + '/\n')
    if graph:
        jvm_options_file.write('-Dit.graph=true\n')
    else:
        jvm_options_file.write('-Dit.graph=false\n')
    if monitor is None:
        jvm_options_file.write('-Dit.monitor=0\n')
    else:
        jvm_options_file.write('-Dit.monitor=' + str(monitor) + '\n')
    jvm_options_file.write('-Dit.tracing=' + str(trace) + '\n')
    if trace > 0:
        #jvm_options_file.write('-Dit.tracing=1\n') # basic tracing
        #jvm_options_file.write('-Dit.tracing=2\n') # advanced tracing
        if trace == 1:
            os.environ['EXTRAE_CONFIG_FILE'] = it_home + '/Runtime/configuration/xml/tracing/extrae_basic.xml'
        elif trace == 2:
            os.environ['EXTRAE_CONFIG_FILE'] = it_home + '/Runtime/configuration/xml/tracing/extrae_advanced.xml'
    if extraeCfg is None:
        jvm_options_file.write('-Dit.extrae.file=null\n')
    else:
        jvm_options_file.write('-Dit.extrae.file=' + extraeCfg + '\n')
    if comm == 'GAT':
        jvm_options_file.write('-Dit.comm=integratedtoolkit.gat.master.GATAdaptor\n')
    else:
        jvm_options_file.write('-Dit.comm=integratedtoolkit.nio.master.NIOAdaptor\n')
    jvm_options_file.write('-Dit.masterName=' + masterName + '\n')
    jvm_options_file.write('-Dit.masterPort=' + masterPort + '\n')
    jvm_options_file.write('-Dit.scheduler=' + scheduler + '\n')
    jvm_options_file.write('-Dgat.adaptor.path=' + it_home + '/Dependencies/JAVA_GAT/lib/adaptors\n')
    jvm_options_file.write('-Dit.gat.broker.adaptor=sshtrilead\n')
    jvm_options_file.write('-Dit.gat.file.adaptor=sshtrilead\n')
    jvm_options_file.write('-Dit.worker.cp=' + classpath + '\n')
    jvm_options_file.write('-Dit.worker.pythonpath=' + pythonPath + '\n')
    jvm_options_file.write('-Dit.worker.jvm_opts=' + jvmWorkers + '\n')
    jvm_options_file.write('-Djava.class.path=' + it_home + '/Runtime/compss-engine.jar:' + classpath + '\n')
    jvm_options_file.close()
    os.close(fd)
    os.environ['JVM_OPTIONS_FILE'] = temp_path

    ##############################################################

    print "Starting COMPSs runtime."
    compss_start()
    print "COMPSs runtime started."

    if o_c == True:
        # set cross-module variable
        binding.object_conversion = True
    else:
        # set cross-module variable
        binding.object_conversion = False

    # Enable or disable the use of mmap
    # serializer.mmap_file_storage = False
    # Remove launch.py, log_level and object_conversion from sys.argv,
    # It will be inherited by the app through execfile
    # sys.argv = sys.argv[3:]
    # Get application execution path
    # app_path = sys.argv[0]  ############ not needed --> interactive mode

    logPath = get_logPath()
    binding.temp_dir = mkdtemp(prefix='pycompss', dir=logPath + '/tmpFiles/')

    print "Log path : ", logPath

    # 2.0 logging
    if log_level == "debug":
        init_logging(os.getenv('IT_HOME') + '/Bindings/python/log/logging.json.debug', logPath)
    elif log_level == "info":
        init_logging(os.getenv('IT_HOME') + '/Bindings/python/log/logging.json.off', logPath)
    elif log_level == "off":
        init_logging(os.getenv('IT_HOME') + '/Bindings/python/log/logging.json.off', logPath)
    else:
        # Default
        init_logging(os.getenv('IT_HOME') + '/Bindings/python/log/logging.json', logPath)

    logger = logging.getLogger("pycompss.runtime.launch")
    printSetup(log_level, o_c, debug, graph, trace, monitor,
               project_xml, resources_xml, summary, taskExecution, storageConf,
               taskCount, appName, uuid, baseLogDir, specificLogDir, extraeCfg,
               comm, masterName, masterPort, scheduler, jvmWorkers)

    logger.debug("--- START ---")
    logger.debug("PyCOMPSs Log path: %s" % logPath)
    if storageConf != None:
        logger.debug("Storage configuration file: %s" % storageConf)
        initStorage(config_file_path=storageConf)
        storage = True

    # MAIN EXECUTION
    # let the user write an interactive application
    print "PyCOMPSs Runtime started... Have fun!"

    if forked:
        while True:
            pass


def printSetup(log_level, o_c, debug, graph, trace, monitor,
          project_xml, resources_xml, summary, taskExecution, storageConf,
          taskCount, appName, uuid, baseLogDir, specificLogDir, extraeCfg,
          comm, masterName, masterPort, scheduler, jvmWorkers):
    logger = logging.getLogger("pycompss.runtime.launch")
    output = ""
    output += "******************************************************\n"
    output += "Log level         : " + str(log_level) + "\n"
    output += "Object conversion : " + str(o_c) + "\n"
    output += "Debug             : " + str(debug) + "\n"
    output += "Graph             : " + str(graph) + "\n"
    output += "Trace             : " + str(trace) + "\n"
    output += "Monitor           : " + str(monitor) + "\n"
    output += "Project XML       : " + str(project_xml) + "\n"
    output += "Resources XML     : " + str(resources_xml) + "\n"
    output += "Summary           : " + str(summary) + "\n"
    output += "Task execution    : " + str(taskExecution) + "\n"
    output += "Storage conf.     : " + str(storageConf) + "\n"
    output += "Task count        : " + str(taskCount) + "\n"
    output += "Application name  : " + str(appName) + "\n"
    output += "UUID              : " + str(uuid) + "\n"
    output += "Base log dir.     : " + str(baseLogDir) + "\n"
    output += "Specific log dir. : " + str(specificLogDir) + "\n"
    output += "Extrae CFG        : " + str(extraeCfg) + "\n"
    output += "COMM library      : " + str(comm) + "\n"
    output += "Master name       : " + str(masterName) + "\n"
    output += "Master port       : " + str(masterPort) + "\n"
    output += "Scheduler         : " + str(scheduler) + "\n"
    output += "JVM Workers       : " + str(jvmWorkers) + "\n"
    output += "******************************************************\n"
    print output
    logger.debug(output)


'''
def stopP(sync=False):
    # stopProcess(sync)
    global running
    global process
    if running:
        print "[iPyCOMPSs] Terminating process..."
        process.terminate()
        process = Process(target=stop, args=(sync))
        process.start()
        process.join()
        process.terminate()
        print "[iPyCOMPSs] Process terminated."
        running = False
    else:
        print "[iPyCOMPSs] There is not PyCOMPSs instance running."
'''


def stop(sync=False):
    print "******************************************************"
    print "**************** STOPPING PyCOMPSs *******************"
    print "******************************************************"

    logger = logging.getLogger("pycompss.runtime.launch")

    if sync:
        print "Synchronizing all future objects left on the user scope."
        logger.debug("Synchronizing all future objects left on the user scope.")
        from pycompss.api.api import compss_wait_on
        task_objects = get_task_objects()

        ipython = globals()['__builtins__']['get_ipython']()
        # import pprint
        # pprint.pprint(ipython.__dict__, width=1)
        raw_code = ipython.__dict__['user_ns']
        for k in raw_code:
            objK = raw_code[k]
            obj_id = id(objK)
            if not k.startswith('_'):   # not internal objects
                if type(objK) == binding.Future:
                    print "Found a future object: ", str(k)
                    logger.debug("Found a future object: %s" % (k,))
                    ipython.__dict__['user_ns'][k] = compss_wait_on(objK)
                elif obj_id in task_objects:
                    print "Found an object to synchronize: ", str(k)
                    logger.debug("Found an object to synchronize: %s" % (k,))
                    ipython.__dict__['user_ns'][k] = compss_wait_on(objK)
                else:
                    pass

    if storage == True:
        finishStorage()

    compss_stop()

    logger.debug("--- END ---")
    print "--- END ---"
    # os._exit(00)  # Explicit kernel restart

    # --- Execution finished ---


###################################################################################################
###################################################################################################
###################################################################################################

def exportGlobals():
    # Super ugly, but I see no other way to define the app_path across the interactive execution without
    # making the user to define it explicitly.
    # It is necessary to define only one app_path because of the two decorators need to access the same information.
    # if the file is created per task, the constraint will not be able to work.
    # Get ipython globals
    ipython = globals()['__builtins__']['get_ipython']()
    # import pprint
    # pprint.pprint(ipython.__dict__, width=1)
    # Extract user globals from ipython
    userGlobals = ipython.__dict__['ns_table']['user_global']
    # Inject app_path variable to user globals so that task and constraint decorators can get it.
    # userGlobals['app_path'] = os.getcwd() + '/' + "InteractiveMode" + str(randint(20000, 40000)) + '.py'
    userGlobals['app_path'] = os.getcwd() + '/' + "InteractiveMode" + str(time.strftime('%d%m%y_%H%M%S')) + '.py'