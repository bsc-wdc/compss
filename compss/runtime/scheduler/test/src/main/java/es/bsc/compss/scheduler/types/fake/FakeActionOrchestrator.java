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
package es.bsc.compss.scheduler.types.fake;

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.allocatableactions.StartWorkerAction;
import es.bsc.compss.worker.COMPSsException;

import java.util.concurrent.Semaphore;


public class FakeActionOrchestrator implements ActionOrchestrator {

    private TaskScheduler ts;
    private final Semaphore startedWorkers = new Semaphore(0);
    private final Semaphore startingWorker = new Semaphore(0);


    public FakeActionOrchestrator() {
        super();
    }

    public void setTaskScheduler(TaskScheduler ts) {
        this.ts = ts;
    }

    // Notification thread
    @Override
    public void actionRunning(AllocatableAction action) {
        this.ts.actionRunning(action);
    }

    // Notification thread
    @Override
    public void actionCompletion(AllocatableAction action) {
        startingWorker.acquireUninterruptibly();
        this.ts.actionCompleted(action);
        if (action instanceof StartWorkerAction) {
            startedWorkers.release();
        }
    }

    public void waitForResourcesStarted() {
        startingWorker.release();
        startedWorkers.acquireUninterruptibly(1);
    }

    // Notification thread
    @Override
    public void actionError(AllocatableAction action) {
        this.ts.errorOnAction(action);
    }

    // Notification thread
    @Override
    public void actionException(AllocatableAction action, COMPSsException e) {
        this.ts.exceptionOnAction(action, e);
    }

    @Override
    public void actionUpgrade(AllocatableAction action) {
        this.ts.upgradeAction(action);
    }
}
