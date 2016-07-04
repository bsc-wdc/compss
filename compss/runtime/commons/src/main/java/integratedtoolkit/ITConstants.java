package integratedtoolkit;

import java.io.File;
import java.util.UUID;

import integratedtoolkit.types.exceptions.NonInstantiableException;


public class ITConstants {

    // Environment Properties
    public static final String IT_HOME = "IT_HOME";
    public static final String LD_LIBRARY_PATH = "LD_LIBRARY_PATH";

    // Component names
    public static final String TA = "Task Analyser";
    public static final String TS = "Task Scheduler";
    public static final String JM = "Job Manager";
    public static final String DM = "Data Manager";
    public static final String DIP = "Data Information Provider";
    public static final String FTM = "File Transfer Manager";

    // Dynamic system properties
    public static final String IT_APP_NAME 			= "it.appName";
    public static final String IT_MASTER_PORT 		= "it.masterPort";
    public static final String IT_DEPLOYMENT_ID 	= "it.uuid";
    public static final String IT_BASE_LOG_DIR 		= "it.baseLogDir";
    public static final String IT_SPECIFIC_LOG_DIR  = "it.specificLogDir";
    public static final String IT_APP_LOG_DIR 		= "it.appLogDir";
    public static final String IT_PROJ_FILE 		= "it.project.file";
    public static final String IT_PROJ_SCHEMA 		= "it.project.schema";
    public static final String IT_RES_FILE 			= "it.resources.file";
    public static final String IT_RES_SCHEMA 		= "it.resources.schema";
    public static final String IT_CONSTR_FILE 		= "it.constraints.file";
    public static final String IT_SCHEDULER 		= "it.scheduler";
    public static final String IT_TRACING 			= "it.tracing";
    public static final String IT_PRESCHED 			= "it.presched";
    public static final String IT_GRAPH 			= "it.graph";
    public static final String IT_MONITOR 			= "it.monitor";
    public static final String IT_SERVICE_NAME 		= "it.serviceName";

    public static final String IT_LANG 				= "it.lang";
    public static final String IT_WORKER_CP 		= "it.worker.cp";
    public static final String IT_WORKER_PP			= "it.worker.pythonpath";
    public static final String IT_WORKER_JVM_OPTS   = "it.worker.jvm_opts"; 
    public static final String IT_CORE_COUNT 		= "it.core.count";

    public static final String COMM_ADAPTOR 		= "it.comm";
    
    // SERVICES
    public static final String SERVICE_ADAPTOR 		= "integratedtoolkit.ws.master.WSAdaptor";
    
    // GAT
    public static final String GAT_ADAPTOR_PATH		= "gat.adaptor.path";
    public static final String GAT_DEBUG 			= "gat.debug";
    public static final String GAT_BROKER_ADAPTOR 	= "it.gat.broker.adaptor";
    public static final String GAT_FILE_ADAPTOR 	= "it.gat.file.adaptor";
    
    // System properties for Instrumentation flags
    public static final String IT_TO_FILE 			= "it.to.file";
    public static final String IT_IS_WS 			= "it.is.ws";
    public static final String IT_IS_MAINCLASS 		= "it.is.mainclass";

    // Properties for locating the it.properties file
    public static final String IT_CONFIG 			= "it.properties";
    public static final String IT_CONFIG_LOCATION 	= "it.properties.location";
    //I think this is used just for optimis case
    public static final String IT_CONTEXT 			= "it.context";

    // LOG 4J
    public static final String LOG4J 				= "log4j.configuration";

    // Deployment
    public static final String IT_JVM 				= "ITJvm";

    // Initialization
    public static final String INIT_OK 				= "OK";
    
    // Connectors
    public static final String PASSWORD 			= "Password";
    
    // Storage properties
	public static final String IT_STORAGE_CONF 		= "it.storage.conf";
	public static final String IT_TASK_EXECUTION 	= "it.task.execution";
	
	// Execution Type
    public static final String COMPSs = "compss";

    // Languages
    public static enum Lang {
        JAVA,
        C,
        PYTHON
    }

    //According to runcompss script default value
    //private static final String DEFAULT_ADAPTOR 			= "integratedtoolkit.gat.master.GATAdaptor";
    public static final String DEFAULT_ADAPTOR 				= "integratedtoolkit.nio.master.NIOAdaptor";
    public static final String DEFAULT_SCHEDULER 			= "integratedtoolkit.components.impl.TaskScheduler";
    public static final String DEFAULT_TRACING 				= "0";
    public static final String DEFAULT_DEPLOYMENT_ID 		= UUID.randomUUID().toString();
    public static final String DEFAULT_RES_SCHEMA 			= System.getenv(IT_HOME) + File.separator + "Runtime" + File.separator + 
    															"configuration" + File.separator + "xml" + File.separator 
    															+ "resources" + File.separator + "resource_schema.xsd";
    public static final String DEFAULT_PROJECT_SCHEMA 		= System.getenv(IT_HOME) + File.separator + "Runtime" + File.separator + 
																"configuration" + File.separator + "xml" + File.separator 
																+ "projects" + File.separator + "project_schema.xsd";
    public static final String DEFAULT_GAT_ADAPTOR_LOCATION = System.getenv("GAT_LOCATION") + "/lib/adaptors";
    public static final long DEFAULT_MONITOR_INTERVAL 		= 0;
    
    private ITConstants() {
        throw new NonInstantiableException("ITConstants");
    }
    
}
