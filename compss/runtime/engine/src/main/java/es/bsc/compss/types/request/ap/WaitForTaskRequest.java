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
import es.bsc.compss.types.data.accessparams.AccessParams.AccessMode;

import java.util.concurrent.Semaphore;


public class WaitForTaskRequest extends APRequest {

    private final int dataId;
    private final AccessMode am;
    private final Semaphore sem;


    /**
     * Creates a new request to wait for a task completion.
     * 
     * @param dataId Data Id.
     * @param mode Access mode.
     * @param sem Waiting semaphore.
     */
    public WaitForTaskRequest(int dataId, AccessMode mode, Semaphore sem) {
        this.dataId = dataId;
        this.am = mode;
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

    /**
     * Returns the associated data Id.
     * 
     * @return The associated data Id.
     */
    public int getDataId() {
        return this.dataId;
    }

    /**
     * Returns the associated access mode to the data.
     * 
     * @return The associated access mode to the data.
     */
    public AccessMode getAccessMode() {
        return this.am;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        ta.findWaitedTask(this);
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.WAIT_FOR_TASK;
    }

}
