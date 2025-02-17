/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.ResourceManager;


/**
 * The DeleteIntermediateFilesRequest represents a request to delete the intermediate files of the execution from all
 * the worker nodes of the resource pool.
 */
public class PrintCurrentLoadRequest extends TDRequest {

    /**
     * Constructs a PrintCurrentLoadRequest.
     */
    public PrintCurrentLoadRequest() {
    }

    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        RESOURCES_LOGGER.info(ts.getWorkload().toString());
        ResourceManager.printResourcesState();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.PRINT_CURRENT_GRAPH;
    }

}
