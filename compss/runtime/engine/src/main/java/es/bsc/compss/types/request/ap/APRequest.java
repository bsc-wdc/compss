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
package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.request.Request;
import es.bsc.compss.types.request.exceptions.ShutdownException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The TPRequest class represents any interaction with the TaskProcessor component.
 */
public abstract class APRequest extends Request {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TP_COMP);


    /**
     * Contains the different types of request that the Access Processor can response.
     */
    public enum APRequestType {
        ANALYSE_TASK, // Analyse a new task
        UPDATE_GRAPH, // Update task graph
        WAIT_FOR_TASK, // Waits for task completion
        WAIT_FOR_CONCURRENT, // Waits for concurrent task completion
        WAIT_FOR_ALL_TASKS, // Waits for all submitted tasks
        END_OF_APP, // End of application
        ALREADY_ACCESSED, // Data has already been accessed
        REGISTER_DATA_ACCESS, // Register a new data access
        TRANSFER_OPEN_FILE, // Request an open file transfer
        TRANSFER_RAW_FILE, // Request a raw file transfer
        TRANSFER_OBJECT, // Request an object transfer
        NEW_VERSION_SAME_VALUE, // Creates a new version
        IS_OBJECT_HERE, // Checks if the given object is available
        SET_OBJECT_VERSION_VALUE, // Sets a new version to a given object
        GET_LAST_RENAMING, // Returns the last renaming of an object
        BLOCK_AND_GET_RESULT_FILES, // Locks the execution until the result files are transferred
        UNBLOCK_RESULT_FILES, // Unlocks the result files
        SHUTDOWN, // Shutdown request
        GRAPHSTATE, // Requests the task graph state
        TASKSTATE, // Requests a task state
        DELETE_FILE, // Deletes the given file
        FINISH_ACCESS_FILE, // Marks as finished the access to a file
        DEBUG, // Enables the debug
        DEREGISTER_OBJECT // Unregisters a given object
    }


    /**
     * Returns the type of request for this instance
     *
     * @return returns the request type name of this instance
     * @result returns the request type name of this instance
     *
     */
    public abstract APRequestType getRequestType();

    /**
     * Processes the Request
     *
     * @param ap
     *            AccessProcessor processing the request
     * @param ta
     *            Task Analyser of the processing AccessProcessor
     * @param dip
     *            DataInfoProvider of the processing AccessProcessor
     * @param td
     *            Task Dispatcher attached to the processing AccessProcessor
     * @throws compss.types.request.exceptions.ShutdownException
     */
    public abstract void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) throws ShutdownException;

}
