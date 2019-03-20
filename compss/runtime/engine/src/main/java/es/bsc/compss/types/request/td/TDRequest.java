/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.types.request.td;

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.request.Request;
import es.bsc.compss.types.request.exceptions.ShutdownException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The TDRequest class represents any interaction with the TaskDispatcher component.
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
