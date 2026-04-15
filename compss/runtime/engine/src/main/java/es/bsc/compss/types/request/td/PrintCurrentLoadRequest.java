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
package es.bsc.compss.types.request.td;

import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TDRequestEvent;
import es.bsc.compss.util.ResourceManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The DeleteIntermediateFilesRequest represents a request to delete the intermediate files of the execution from all
 * the worker nodes of the resource pool.
 */
public class PrintCurrentLoadRequest extends TaskDispatcher.AsynchTDRequest {

    private static final Logger RESOURCES_LOGGER = LogManager.getLogger(Loggers.RESOURCES);
    private static final boolean RESOURCES_LOGGER_DEBUG = RESOURCES_LOGGER.isDebugEnabled();


    /**
     * Constructs a PrintCurrentLoadRequest.
     *
     * @param td TaskDispatcher processing the event
     */
    public PrintCurrentLoadRequest(TaskDispatcher td) {
        td.super();
    }

    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        RESOURCES_LOGGER.info(ts.getWorkload().toString());
        ResourceManager.printResourcesState();
    }

    @Override
    public TDRequestEvent getEvent() {
        return TDRequestEvent.PRINT_CURRENT_GRAPH;
    }

}
