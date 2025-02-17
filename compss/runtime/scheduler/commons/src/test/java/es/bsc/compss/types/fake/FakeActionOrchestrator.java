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
package es.bsc.compss.types.fake;

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.worker.COMPSsException;


public class FakeActionOrchestrator implements ActionOrchestrator {

    private final TaskScheduler ts;


    public FakeActionOrchestrator() {
        super();
        this.ts = new TaskScheduler(this);
        ;
    }

    // Notification thread
    @Override
    public void actionRunning(AllocatableAction action) {
        this.ts.actionRunning(action);
    }

    // Notification thread
    @Override
    public void actionCompletion(AllocatableAction action) {
        this.ts.actionCompleted(action);
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

    /**
     * Stops the Fake Orchestrator
     */
    public void shutdown() {
        ts.shutdown();
    }
}
