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

import es.bsc.es.bsc.compss.scheduler.fullGraphScheduler.FullGraphResourceScheduler;
import es.bsc.es.bsc.compss.scheduler.fullGraphScheduler.ScheduleOptimizer;
import es.bsc.es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.PriorityQueue;


public class OptimizationWorker<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> {

    private FullGraphResourceScheduler<P,T,I> resource;
    private PriorityQueue<AllocatableAction<P,T,I>> donorActions;


    public OptimizationWorker(FullGraphResourceScheduler<P,T,I> resource) {
        this.resource = resource;
    }

    public void localOptimization(long optimizationTS) {
        donorActions = resource.localOptimization(optimizationTS, 
                ScheduleOptimizer.getSelectionComparator(),
                ScheduleOptimizer.getDonationComparator());
    }

    public AllocatableAction<P,T,I> pollDonorAction() {
        return donorActions.poll();
    }

    public long getDonationIndicator() {
        return resource.getLastGapExpectedStart();
    }

    public String getName() {
        return resource.getName();
    }

    public FullGraphResourceScheduler<P,T,I> getResource() {
        return resource;
    }

}
