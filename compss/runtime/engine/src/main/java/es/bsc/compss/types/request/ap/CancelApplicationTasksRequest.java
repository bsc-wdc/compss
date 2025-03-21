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
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.worker.COMPSsException;
import java.util.concurrent.Semaphore;


public class CancelApplicationTasksRequest extends CancelTaskGroupRequest {

    /**
     * Creates a request to cancel all tasks of an application.
     * 
     * @param app Application.
     * @param sem Synchronising semaphore.
     */
    public CancelApplicationTasksRequest(Application app, Semaphore sem) {
        super(app, "App" + app.getId(), sem);
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.CANCEL_ALL_TASKS;
    }

    @Override
    public void process(AccessProcessor ap, TaskDispatcher td) throws ShutdownException, COMPSsException {
        LOGGER.debug("Cancelling tasks of application " + getApp().getId());
        super.cancelGroup(td);
    }
}
