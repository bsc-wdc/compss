package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.request.Request;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.types.resources.WorkerResourceDescription;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The TDRequest class represents any interaction with the TaskDispatcher component.
 */
public abstract class TDRequest<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> extends Request {

    public enum TDRequestType {
        ACTION_UPDATE, // Update action
        CE_REGISTRATION, // Register new coreElement
        EXECUTE_TASKS, // Execute task
        GET_CURRENT_SCHEDULE, // get the current schedule status
        PRINT_CURRENT_GRAPH, // print current task graph
        MONITORING_DATA, // print data for monitoring
        TD_SHUTDOWN, // shutdown
        UPDATE_CEI_LOCAL, // Updates CEI locally
        WORKER_UPDATE_REQUEST // Updates a worker definition
    }


    // Logging
    protected static final Logger logger = LogManager.getLogger(Loggers.TD_COMP);
    protected static final boolean debug = logger.isDebugEnabled();

    protected static final Logger resourcesLogger = LogManager.getLogger(Loggers.RESOURCES);
    protected static final boolean resourcesLoggerDebug = resourcesLogger.isDebugEnabled();


    public abstract TDRequestType getType();

    public abstract void process(TaskScheduler<P, T, I> ts) throws ShutdownException;

}
