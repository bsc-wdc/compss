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
package es.bsc.compss.scheduler.types;

import es.bsc.compss.scheduler.fullGraphScheduler.FullGraphResourceScheduler;
import es.bsc.compss.scheduler.fullGraphScheduler.ScheduleOptimizer;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.PriorityQueue;


public class OptimizationWorker<T extends WorkerResourceDescription> {

    private FullGraphResourceScheduler<T> resource;
    private PriorityQueue<AllocatableAction> donorActions;


    public OptimizationWorker(FullGraphResourceScheduler<T> resource) {
        this.resource = resource;
    }

    public void localOptimization(long optimizationTS) {
        this.donorActions = this.resource.localOptimization(optimizationTS, ScheduleOptimizer.getSelectionComparator(),
                ScheduleOptimizer.getDonationComparator());
    }

    public AllocatableAction pollDonorAction() {
        return this.donorActions.poll();
    }

    public long getDonationIndicator() {
        return this.resource.getLastGapExpectedStart();
    }

    public String getName() {
        return this.resource.getName();
    }

    public FullGraphResourceScheduler<T> getResource() {
        return this.resource;
    }

}
