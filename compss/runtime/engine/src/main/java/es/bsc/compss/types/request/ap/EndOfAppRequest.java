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

import es.bsc.compss.types.Application;
import es.bsc.compss.types.tracing.TraceEvent;


public class EndOfAppRequest extends BarrierRequest {

    /**
     * Creates a new request to end the application.
     *
     * @param app Application Id.
     */
    public EndOfAppRequest(Application app) {
        super(app, "No more tasks");
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.END_OF_APP;
    }

    @Override
    public void handleBarrier() {
        LOGGER.info("TA Processes no More tasks for app " + this.getApp().getId());
        Application app = this.getApp();
        app.endReached(this);
        LOGGER.info("TA Processed no More tasks for app " + this.getApp().getId());

    }
}
