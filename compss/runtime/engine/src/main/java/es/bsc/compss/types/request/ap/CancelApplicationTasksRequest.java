/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.worker.COMPSsException;
import java.util.concurrent.Semaphore;


public class CancelApplicationTasksRequest extends APRequest {

    private Long appId;
    private Semaphore sem;


    /**
     * Creates a request to cancel all tasks of an application.
     * 
     * @param appId Application Id.
     * @param sem Synchronising semaphore.
     */
    public CancelApplicationTasksRequest(Long appId, Semaphore sem) {
        this.appId = appId;
        this.sem = sem;
    }

    /**
     * Returns the waiting semaphore.
     * 
     * @return The waiting semaphore.
     */
    public Semaphore getSemaphore() {
        return this.sem;
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.CANCEL_ALL_TASKS;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td)
        throws ShutdownException, COMPSsException {
        ta.cancelApplicationTasks(this);
    }

    /**
     * Returns the associated application Id.
     * 
     * @return The associated application Id.
     */
    public Long getAppId() {
        return appId;
    }
}
