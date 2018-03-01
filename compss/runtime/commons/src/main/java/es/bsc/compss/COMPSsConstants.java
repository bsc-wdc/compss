package es.bsc.compss;

import java.io.File;
import java.util.UUID;

import es.bsc.compss.types.exceptions.NonInstantiableException;


/**
 * COMPSs Runtime Constants
 *
 */
public class COMPSsConstants {

    /**
     * Languages
     */
    public static enum Lang {
        JAVA, // For Java applications
        C, // For C or C++ applications
        PYTHON // For Python applications
    }


    /*
     * Environment Properties
     */
    public static final String COMPSS_HOME = "COMPSS_HOME";
    public static final String LD_LIBRARY_PATH = "LD_LIBRARY_PATH";
    public static final String COMPSS_WORKING_DIR = "COMPSS_WORKING_DIR";
    public static final String COMPSS_APP_DIR = "COMPSS_APP_DIR";
    public static final String GAT_LOC = "GAT_LOCATION";

    /*
     * Component names
     */
    public static final String TA = "Task Analyser";
    public static final String TS = "Task Scheduler";
    public static final String JM = "Job Manager";
    public static final String DM = "Data Manager";
    public static final String DIP = "Data Information Provider";
    public static final String FTM = "File Transfer Manager";

    /*
     * Dynamic system properties
     */
    public static final String APP_NAME = "compss.appName";
    public static final String SERVICE_NAME = "compss.serviceName";
    public static final String MASTER_NAME = "compss.masterName";
    public static final String MASTER_PORT = "compss.masterPort";
    public static final String DEPLOYMENT_ID = "compss.uuid";

    public static final String BASE_LOG_DIR = "compss.baseLogDir";
    public static final String SPECIFIC_LOG_DIR = "compss.specificLogDir";
    public static final String APP_LOG_DIR = "compss.appLogDir";

    public static final String PROJ_FILE = "compss.project.file";
    public static final String PROJ_SCHEMA = "compss.project.schema";
    public static final String RES_FILE = "compss.resources.file";
    public static final String RES_SCHEMA = "compss.resources.schema";

    public static final String LANG = "compss.lang";
    public static final String TASK_SUMMARY = "compss.summary";

    public static final String CONSTR_FILE = "compss.constraints.file";
    public static final String SCHEDULER = "compss.scheduler";
    public static final String SCHEDULER_CONFIG_FILE = "compss.scheduler.config";
    public static final String TRACING = "compss.tracing";
    public static final String EXTRAE_CONFIG_FILE = "compss.extrae.file";
    public static final String PRESCHED = "compss.presched";
    public static final String GRAPH = "compss.graph";
    public static final String MONITOR = "compss.monitor";
    public static final String INPUT_PROFILE = "compss.profile.input";
    public static final String OUTPUT_PROFILE = "compss.profile.output";
    public static final String EXTERNAL_ADAPTATION= "compss.external.adaptation";
    
    public static final String WORKER_CP = "compss.worker.cp";
    public static final String WORKER_PP = "compss.worker.pythonpath";
    public static final String WORKER_JVM_OPTS = "compss.worker.jvm_opts";
    public static final String WORKER_REMOVE_WD = "compss.worker.removeWD";
    public static final String WORKER_CPU_AFFINITY = "compss.worker.cpu_affinity";
    public static final String WORKER_GPU_AFFINITY = "compss.worker.gpu_affinity";

    public static final String COMM_ADAPTOR = "compss.comm";
    public static final String CONN = "compss.conn";

    // GAT
    public static final String GAT_ADAPTOR_PATH = "gat.adaptor.path";
    public static final String GAT_DEBUG = "gat.debug";
    public static final String GAT_BROKER_ADAPTOR = "gat.broker.adaptor";
    public static final String GAT_FILE_ADAPTOR = "gat.file.adaptor";

    // Storage properties
    public static final String STORAGE_CONF = "compss.storage.conf";
    public static final String TASK_EXECUTION = "compss.task.execution";
    public static final String EXECUTION_INTERNAL = "compss";

    // Persistent worker c property
    public static final String WORKER_PERSISTENT_C = "compss.worker.persistent.c";

    // System properties for Instrumentation flags
    public static final String COMPSS_TO_FILE = "compss.to.file";
    public static final String COMPSS_IS_WS = "compss.is.ws";
    public static final String COMPSS_IS_MAINCLASS = "compss.is.mainclass";

    // Properties for locating the it.properties file
    public static final String COMPSS_CONFIG = "compss.properties";
    public static final String COMPSS_CONFIG_LOCATION = "compss.properties.location";
    public static final String COMPSS_CONTEXT = "compss.context";
    

    // LOG 4J
    public static final String LOG4J = "log4j.configurationFile";

    /*
     * DEFAULT VALUES: According to runcompss script !!!!
     */
    public static final String DEFAULT_SCHEDULER = "es.bsc.compss.components.impl.TaskScheduler";

    public static final String SERVICE_ADAPTOR = "es.bsc.compss.ws.master.WSAdaptor";

    // private static final String DEFAULT_ADAPTOR = "es.bsc.compss.gat.master.GATAdaptor";
    public static final String DEFAULT_ADAPTOR = "es.bsc.compss.nio.master.NIOAdaptor";

    public static final String DEFAULT_CONNECTOR = "es.bsc.compss.connectors.DefaultSSHConnector";

    public static final String DEFAULT_TRACING = "0";
    public static final String DEFAULT_CUSTOM_EXTRAE_FILE = "null";

    public static final long DEFAULT_MONITOR_INTERVAL = 0;

    public static final String DEFAULT_DEPLOYMENT_ID = UUID.randomUUID().toString();

    public static final String DEFAULT_RES_SCHEMA = System.getenv(COMPSS_HOME) + File.separator + "Runtime" + File.separator
            + "configuration" + File.separator + "xml" + File.separator + "resources" + File.separator + "resource_schema.xsd";
    public static final String DEFAULT_PROJECT_SCHEMA = System.getenv(COMPSS_HOME) + File.separator + "Runtime" + File.separator
            + "configuration" + File.separator + "xml" + File.separator + "projects" + File.separator + "project_schema.xsd";

    public static final String DEFAULT_GAT_ADAPTOR_LOCATION = System.getenv(GAT_LOC) + File.separator + "lib" + File.separator + "adaptors";
    public static final String DEFAULT_PERSISTENT_C = "false";


    private COMPSsConstants() {
        throw new NonInstantiableException("COMPSsConstants");
    }

}
