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

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.es.bsc.compss.scheduler.fullGraphScheduler.FullGraphSchedulingInformation;
import es.bsc.es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.es.bsc.compss.scheduler.types.Profile;
import es.bsc.es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.LinkedList;


public class OptimizationAction<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends AllocatableAction<P, T, I> {

    public OptimizationAction(ActionOrchestrator<P, T, I> orchestrator) {
        super(new FullGraphSchedulingInformation<P, T, I>(), orchestrator);
    }

    @Override
    public boolean areEnoughResources() {
        return true;
    }

    @Override
    protected void reserveResources() {
    }

    @Override
    protected void releaseResources() {

    }

    @Override
    protected void doAction() {

    }

    @Override
    protected void doCompleted() {

    }

    @Override
    protected void doError() throws FailedActionException {

    }

    @Override
    protected void doFailed() {

    }

    @Override
    public Integer getCoreId() {
        return null;
    }

    @Override
    public LinkedList<ResourceScheduler<P, T, I>> getCompatibleWorkers() {
        return null;
    }

    @Override
    public Implementation<T>[] getImplementations() {
        return null;
    }

    @Override
    public boolean isCompatible(Worker<T, I> r) {
        return true;
    }

    @Override
    public LinkedList<I> getCompatibleImplementations(ResourceScheduler<P, T, I> r) {
        return null;
    }

    @Override
    public int getPriority() {
        return 0;
    }

    @Override
    public Score schedulingScore(ResourceScheduler<P, T, I> targetWorker, Score actionScore) {
        return null;
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public void schedule(ResourceScheduler<P, T, I> targetWorker, Score actionScore)
            throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public void schedule(ResourceScheduler<P, T, I> targetWorker, I impl) throws BlockedActionException, UnassignedActionException {

    }

}
