/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.components.impl.ResourceScheduler;
import java.util.LinkedList;

import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.types.resources.WorkerResourceDescription;


public class FakeSI extends SchedulingInformation {

    // Allocatable actions that the action depends on due to resource availability
    private final LinkedList<AllocatableAction> resourcePredecessors;

    // Allocatable actions depending on the allocatable action due to resource availability
    private final LinkedList<AllocatableAction> resourceSuccessors;


    public <T extends WorkerResourceDescription> FakeSI(ResourceScheduler<T> enforcedTargetResource) {
        super(enforcedTargetResource);
        resourcePredecessors = new LinkedList<>();
        resourceSuccessors = new LinkedList<>();
    }

    public void addPredecessor(AllocatableAction predecessor) {
        resourcePredecessors.add(predecessor);
    }

    public boolean hasPredecessors() {
        return !resourcePredecessors.isEmpty();
    }

    @Override
    public final boolean isExecutable() {
        return resourcePredecessors.isEmpty();
    }

    public LinkedList<AllocatableAction> getPredecessors() {
        return resourcePredecessors;
    }

    public void removePredecessor(AllocatableAction successor) {
        resourcePredecessors.remove(successor);
    }

    public void clearPredecessors() {
        resourcePredecessors.clear();
    }

    public void addSuccessor(AllocatableAction successor) {
        resourceSuccessors.add(successor);
    }

    public LinkedList<AllocatableAction> getSuccessors() {
        return resourceSuccessors;
    }

    public synchronized void removeSuccessor(AllocatableAction successor) {
        resourceSuccessors.remove(successor);
    }

    public void clearSuccessors() {
        resourceSuccessors.clear();
    }

}
