package es.bsc.compss.types.request.td;

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.request.Request;
import es.bsc.compss.types.request.exceptions.ShutdownException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The TDRequest class represents any interaction with the TaskDispatcher
 * component.
 *
 */
public abstract class TDRequest extends Request {

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
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TD_COMP);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    protected static final Logger RESOURCES_LOGGER = LogManager.getLogger(Loggers.RESOURCES);
    protected static final boolean RESOURCES_LOGGER_DEBUG = RESOURCES_LOGGER.isDebugEnabled();

    public abstract TDRequestType getType();

    public abstract void process(TaskScheduler ts) throws ShutdownException;

}
