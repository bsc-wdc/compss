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
import es.bsc.compss.types.Application;
import es.bsc.compss.types.Barrier;
import es.bsc.compss.worker.COMPSsException;

import java.util.concurrent.Semaphore;


public class BarrierGroupRequest extends APRequest implements Barrier {

    private String groupName;
    private Semaphore sem;
    private Application app;
    private COMPSsException exception;

    private boolean released;
    private boolean stalled;


    /**
     * Creates a new group barrier request.
     * 
     * @param app Application Id.
     * @param groupName Name of the group.
     * @param sem Waiting semaphore.
     */
    public BarrierGroupRequest(Application app, String groupName, Semaphore sem) {
        this.app = app;
        this.groupName = groupName;
        this.sem = sem;
        this.exception = null;

        this.released = false;
        this.stalled = false;
    }

    public Application getApp() {
        return this.app;
    }

    public String getGroupName() {
        return this.groupName;
    }

    public Semaphore getSemaphore() {
        return this.sem;
    }

    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }

    @Override
    public COMPSsException getException() {
        return this.exception;
    }

    @Override
    public void setException(COMPSsException exception) {
        this.exception = exception;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        ta.barrierGroup(this);
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
