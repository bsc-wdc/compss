/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.request.Request;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.worker.COMPSsException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The TPRequest class represents any interaction with the TaskProcessor component.
 */
public interface APRequest extends Request {

    Logger LOGGER = LogManager.getLogger(Loggers.TP_COMP);
    boolean DEBUG = LOGGER.isDebugEnabled();


    /**
     * Processes the Request.
     *
     * @param ap AccessProcessor processing the request.
     * @param td Task Dispatcher attached to the processing AccessProcessor.
     * @throws ShutdownException If the component has been shutdown.
     * @throws COMPSsException Exception thrown by user
     */
    void process(AccessProcessor ap, TaskDispatcher td) throws ShutdownException, COMPSsException;

}
