package integratedtoolkit.types.request.td;

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

    // Logging
    protected static final Logger logger = Logger.getLogger(Loggers.TD_COMP);
    protected static final boolean debug = logger.isDebugEnabled();

    protected static final Logger resourcesLogger = Logger.getLogger(Loggers.RESOURCES);
    protected static final boolean resourcesLoggerDebug = resourcesLogger.isDebugEnabled();


    public abstract void process(TaskScheduler ts) throws ShutdownException;
}
