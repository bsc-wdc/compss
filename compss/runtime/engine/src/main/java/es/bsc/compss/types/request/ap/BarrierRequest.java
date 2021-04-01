/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.types.Application;
import es.bsc.compss.types.Barrier;
import es.bsc.compss.worker.COMPSsException;

import java.util.concurrent.Semaphore;


public class BarrierRequest extends APRequest implements Barrier {

    private final Semaphore sem;
    private final Application app;

    private boolean released;
    private boolean stalled;


    /**
     * Creates a new barrier request.
     *
     * @param app Application.
     * @param sem Waiting semaphore.
     */
    public BarrierRequest(Application app, Semaphore sem) {
        this.app = app;
        this.sem = sem;
        this.released = false;
        this.stalled = false;
    }

    /**
     * Returns the application of the request.
     *
     * @return The application of the request.
     */
    public Application getApp() {
        return this.app;
    }

    /**
     * Returns the waiting semaphore of the request.
     *
     * @return The waiting semaphore of the request.
     */
    public Semaphore getSemaphore() {
        return this.sem;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        ta.barrier(this);
        synchronized (this) {
            if (!released) {
                this.app.stalled();
                this.stalled = true;
            }
        }
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.WAIT_FOR_ALL_TASKS;
    }

    @Override
    public void setException(COMPSsException exception) {
        // Barrier does not support COMPSsExceptions
    }

    @Override
    public COMPSsException getException() {
        // Barrier does not support COMPSsExceptions
        return null;
    }

    @Override
    public void release() {
        synchronized (this) {
            released = true;
            if (stalled) {
                app.readyToContinue(sem);
            } else {
                sem.release();
            }
        }
    }

}
