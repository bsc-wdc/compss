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


public class WaitForConcurrentRequest extends APRequest {

    private final int dataId;
    private final Semaphore sem;
    private final Semaphore semTask;
    private final AccessMode am;

    private int numWaits;


    /**
     * Creates a new request to wait for a concurrent data.
     * 
     * @param dataId Data Id.
     * @param mode Access mode.
     * @param sem Waiting semaphore.
     * @param semTasks Tasks semaphore.
     */
    public WaitForConcurrentRequest(int dataId, AccessMode mode, Semaphore sem, Semaphore semTasks) {
        this.dataId = dataId;
        this.sem = sem;
        this.semTask = semTasks;
        this.am = mode;
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
     * Returns the tasks semaphore.
     * 
     * @return The tasks semaphore.
     */
    public Semaphore getTaskSemaphore() {
        return this.semTask;
    }

    /**
     * Returns the concurrent access mode.
     * 
     * @return The concurrent access mode.
     */
    public AccessMode getAccessMode() {
        return this.am;
    }

    /**
     * Returns the number of tasks waiting for the request.
     * 
     * @return The number of tasks waiting for the request.
     */
    public int getNumWaitedTasks() {
        return this.numWaits;
    }

    /**
     * Sets a new number of tasks waiting for the request.
     * 
     * @param n Number of tasks waiting for the request.
     */
    public void setNumWaitedTasks(int n) {
        this.numWaits = n;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        ta.findWaitedConcurrent(this);
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.WAIT_FOR_CONCURRENT;
    }

}
