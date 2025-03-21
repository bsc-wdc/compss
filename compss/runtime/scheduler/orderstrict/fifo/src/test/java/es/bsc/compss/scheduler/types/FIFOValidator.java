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
package es.bsc.compss.scheduler.types;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.types.fake.FakeAllocatableAction;
import es.bsc.compss.scheduler.types.fake.FakeResourceDescription;
import es.bsc.compss.scheduler.types.fake.FakeWorker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import org.junit.Assert;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;


public class FIFOValidator extends Validator {

    private final PriorityQueue<FakeAllocatableAction> ready;


    public FIFOValidator(TaskScheduler ts) {
        super(ts);
        ready = new PriorityQueue<>(new Comparator<FakeAllocatableAction>() {

            @Override
            public int compare(FakeAllocatableAction action, FakeAllocatableAction t1) {
                return Integer.compare(action.getFakeId(), t1.getFakeId());
            }
        });
    }

    @Override
    public void workerAdded(FakeWorker worker, List<FakeAllocatableAction> unblockedActions) {
        for (FakeAllocatableAction action : unblockedActions) {
            ready.add(action);
        }
    }

    @Override
    public void workerRemoved(FakeWorker worker, List<FakeAllocatableAction> blockedActions) {
        for (FakeAllocatableAction action : blockedActions) {
            ready.remove(action);
        }
    }

    @Override
    public void actionRegistered(FakeAllocatableAction action, ActionState state) {
        if (state == ActionState.READY) {
            ready.add(action);
        }
    }

    @Override
    public void actionSubmitted(FakeAllocatableAction action) {

        ResourceScheduler<? extends WorkerResourceDescription> rs = action.getAssignedResource();
        if (action.hasDataPredecessors()) {
            // Has dependencies
            Assert.assertNull(action + " has data predecessors but it is assigned to a resource", rs);
        } else {
            // No resources available
            FakeResourceDescription requirements;
            requirements = (FakeResourceDescription) action.getImplementations()[0].getDescription().getConstraints();

            boolean hasCompatibles = false;
            boolean hasAvailable = false;
            for (Map.Entry<String, FakeWorker> entry : resources.entrySet()) {
                FakeWorker worker = entry.getValue();
                FakeResourceDescription description = worker.getDescription();
                FakeResourceDescription available = worker.getAvailable();
                if (description.getCoreCount() >= requirements.getCoreCount()) {
                    hasCompatibles = true;
                    if (requirements.getCoreCount() <= available.getCoreCount()) {
                        hasAvailable = true;
                    }
                }
            }

            if (hasCompatibles) {
                // Is not blocked --> Action can be running or waiting in ready
                if (ready.contains(action)) {
                    if (ready.peek() == action) { // Action has the highest priority and there are no resources
                        Assert.assertFalse(action + " is unassigned and there are enough resources", hasAvailable);
                    }
                    Assert.assertNull(action + " is ready and shouldn't be assigned", rs);
                } else {
                    // Action should be running and assigned to a worker
                    Assert.assertNotNull(action + " should be running", rs);
                }
            }
        }
    }

    @Override
    public void actionExecuting(FakeAllocatableAction action, List<FakeAllocatableAction> depFreeActions) {
        Assert.assertEquals(action + " started executing but is not the highest priority", ready.poll(), action);
        for (FakeAllocatableAction successor : depFreeActions) {
            ready.add(successor);
        }
    }

    @Override
    public void actionFinished(FakeAllocatableAction action, List<FakeAllocatableAction> depFreeActions) {
        for (FakeAllocatableAction successor : depFreeActions) {
            ready.add(successor);
        }
    }

}
