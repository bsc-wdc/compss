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
package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.tracing.TraceEvent;


public class SnapshotRequest implements APRequest {

    private final Application app;


    /**
     * Creates a new snapshot request.
     *
     * @param app Application.
     */
    public SnapshotRequest(Application app) {
        this.app = app;
    }

    /**
     * Returns the application of the request.
     *
     * @return The application of the request.
     */
    public Application getApp() {
        return this.app;
    }

    @Override
    public void process(AccessProcessor ap, TaskDispatcher td) {
        app.getCP().snapshot();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.AP_SNAPSHOT;
    }

}
