package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.JobManager;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.request.Request;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import org.apache.log4j.Logger;


/**
 * The TDRequest class represents any interaction with the TaskDispatcher
 * component.
 */
public abstract class TDRequest extends Request {

    /**
     * Contains the different types of request that the Task Dispatcher can
     * response.
     */
    public enum TDRequestType {

        UPDATE_LOCAL_CEI,
        UPDATED_WORKER_POOL,
        SCHEDULE_TASKS,
        FINISHED_TASK,
        RESCHEDULE_TASK,
        NEW_WAITING_TASK,
        GET_STATE,
        SET_STATE,
        MONITOR_DATA,
        SHUTDOWN,
        DEBUG
    }

    // Logging
    protected static final Logger logger = Logger.getLogger(Loggers.TD_COMP);
    protected static final boolean debug = logger.isDebugEnabled();

    protected static final Logger resourcesLogger = Logger.getLogger(Loggers.RESOURCES);
    protected static final boolean resourcesLoggerDebug = resourcesLogger.isDebugEnabled();

    /**
     * returns the type of request for this instance
     *
     * @return return the request type name of this instance
     *
     */
    public abstract TDRequestType getRequestType();

    public abstract void process(TaskScheduler ts, JobManager jm) throws ShutdownException;
}
