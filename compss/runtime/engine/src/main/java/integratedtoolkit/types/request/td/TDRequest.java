package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.request.Request;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.types.resources.WorkerResourceDescription;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The TDRequest class represents any interaction with the TaskDispatcher component.
 */
public abstract class TDRequest<P extends Profile, T extends WorkerResourceDescription> extends Request {

    public enum TDRequestType {
        ACTION_UPDATE, 
        CE_REGISTRATION,
        EXECUTE_TASKS, 
        GET_CURRENT_SCHEDULE,
        PRINT_CURRENT_GRAPH, 
        MONITORING_DATA, 
        TD_SHUTDOWN, 
        UPDATE_CEI_LOCAL, 
        WORKER_UPDATE_REQUEST
    }


    // Logging
    protected static final Logger logger = LogManager.getLogger(Loggers.TD_COMP);
    protected static final boolean debug = logger.isDebugEnabled();

    protected static final Logger resourcesLogger = LogManager.getLogger(Loggers.RESOURCES);
    protected static final boolean resourcesLoggerDebug = resourcesLogger.isDebugEnabled();


    public abstract TDRequestType getType();

    public abstract void process(TaskScheduler<P, T> ts) throws ShutdownException;

}
