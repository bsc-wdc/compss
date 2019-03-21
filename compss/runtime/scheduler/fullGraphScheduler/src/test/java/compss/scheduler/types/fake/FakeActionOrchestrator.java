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
package es.bsc.compss.scheduler.types.fake;

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.es.bsc.compss.scheduler.types.AllocatableAction;


public class FakeActionOrchestrator implements ActionOrchestrator<FakeProfile, FakeResourceDescription, FakeImplementation> {

    private final TaskScheduler<FakeProfile, FakeResourceDescription, FakeImplementation> ts;


    public FakeActionOrchestrator(TaskScheduler<FakeProfile, FakeResourceDescription, FakeImplementation> ts) {
        super();
        this.ts = ts;
    }

    // Notification thread
    @Override
    public void actionCompletion(AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation> action) {
        ts.actionCompleted(action);
    }

    // Notification thread
    @Override
    public void actionError(AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation> action) {
        ts.errorOnAction(action);
    }

}
