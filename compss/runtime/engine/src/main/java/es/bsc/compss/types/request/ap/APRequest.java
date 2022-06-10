/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.worker.COMPSsException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The TPRequest class represents any interaction with the TaskProcessor component.
 */
public abstract class APRequest {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TP_COMP);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();


    /**
     * Returns the event to be traced for this instance.
     * 
     * @return event to trace
     */
    public abstract TraceEvent getEvent();

    /**
     * Processes the Request.
     *
     * @param ap AccessProcessor processing the request.
     * @param ta Task Analyser of the processing AccessProcessor.
     * @param dip DataInfoProvider of the processing AccessProcessor.
     * @param td Task Dispatcher attached to the processing AccessProcessor.
     * @throws ShutdownException If the component has been shutdown unexpectedly.
     * @throws COMPSsException Exception thrown by user
     */
    public abstract void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td)
        throws ShutdownException, COMPSsException;

}
