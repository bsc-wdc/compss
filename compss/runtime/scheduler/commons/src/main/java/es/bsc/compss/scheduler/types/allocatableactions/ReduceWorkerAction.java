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
package es.bsc.compss.scheduler.types.allocatableactions;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.implementations.ServiceImplementation;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.DynamicMethodWorker;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.resources.updates.PendingReduction;
import es.bsc.compss.types.resources.updates.ResourceUpdate;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ResourceManager;

import java.util.LinkedList;


public class ReduceWorkerAction<T extends WorkerResourceDescription> extends AllocatableAction {

    private final ResourceScheduler<T> worker;
    private final TaskScheduler ts;
    private final Implementation impl;
    private final PendingReduction<T> ru;


    /*
     * ***************************************************************************************************************
     * CONSTRUCTOR
     * ***************************************************************************************************************
     */
    public ReduceWorkerAction(SchedulingInformation schedulingInformation, ResourceScheduler<T> worker, TaskScheduler ts,
            ResourceUpdate<T> modification) {
        super(schedulingInformation, ts.getOrchestrator());
        this.worker = worker;
        this.ts = ts;
        this.ru = (PendingReduction<T>) modification;
        if (modification.getModification() instanceof MethodResourceDescription) {
            impl = new MethodImplementation("", "", null, null, (MethodResourceDescription) modification.getModification());
        } else {
            impl = new ServiceImplementation(null, "", "", "", "");
        }
    }

    /*
     * ***************************************************************************************************************
     * EXECUTION AND LIFECYCLE MANAGEMENT
     * ***************************************************************************************************************
     */
    @Override
    public boolean isToReserveResources() {
        return true;
    }

    @Override
    public boolean isToReleaseResources() {
        return false;
    }

    @Override
    public boolean isToStopResource() {
        return false;
    }

    @Override
    protected void doAction() {
        (new Thread() {

            @SuppressWarnings("unchecked")
            @Override
            public void run() {
                Thread.currentThread().setName(worker.getName() + " stopper");
                DynamicMethodWorker w = (DynamicMethodWorker) worker.getResource();
                PendingReduction<WorkerResourceDescription> crd = (PendingReduction<WorkerResourceDescription>) ru;
                ResourceManager.reduceResource(w, crd);
                w.endTask((MethodResourceDescription) getResourceConsumption());
                try {
                    ru.waitForCompletion();
                } catch (Exception e) {
                    LOGGER.error("ERROR: Exception raised on worker reduction", e);
                    ErrorManager.warn("Exception reducing worker. Check runtime.log for more details", e);
                    notifyError();
                }
                notifyCompleted();
            }
        }).start();
    }

    /*
     * ***************************************************************************************************************
     * EXECUTION TRIGGERS
     * ***************************************************************************************************************
     */
    @Override
    protected void doCompleted() {
        ts.completedResourceUpdate(worker, ru);
    }

    @Override
    protected void doError() throws FailedActionException {
        throw new FailedActionException();
    }

    @Override
    protected void doFailed() {
        LOGGER.error("Error waiting for tasks to end");
        ts.completedResourceUpdate(worker, ru);
    }

    @Override
    public Integer getCoreId() {
        return null;
    }

    @Override
    public LinkedList<ResourceScheduler<? extends WorkerResourceDescription>> getCompatibleWorkers() {
        LinkedList<ResourceScheduler<? extends WorkerResourceDescription>> workers = new LinkedList<>();
        workers.add(worker);
        return workers;
    }

    @Override
    public Implementation[] getImplementations() {
        Implementation[] impls = new Implementation[]{impl};
        return impls;
    }

    @Override
    public <R extends WorkerResourceDescription> boolean isCompatible(Worker<R> r) {
        return (r == worker.getResource());
    }

    @Override
    public <R extends WorkerResourceDescription> LinkedList<Implementation> getCompatibleImplementations(ResourceScheduler<R> r) {
        LinkedList<Implementation> impls = new LinkedList<>();
        if (r == worker) {
            impls.add(impl);
        }
        return impls;
    }

    @Override
    public <R extends WorkerResourceDescription> Score schedulingScore(ResourceScheduler<R> targetWorker, Score actionScore) {
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        schedule((ResourceScheduler<WorkerResourceDescription>) worker, impl);
    }

    @Override
    public void tryToSchedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        this.schedule(actionScore);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R extends WorkerResourceDescription> void schedule(ResourceScheduler<R> targetWorker, Score actionScore)
            throws BlockedActionException, UnassignedActionException {

        schedule((ResourceScheduler<WorkerResourceDescription>) targetWorker, impl);
    }

    @Override
    public <R extends WorkerResourceDescription> void schedule(ResourceScheduler<R> targetWorker, Implementation impl)
            throws BlockedActionException, UnassignedActionException {

        if (targetWorker != getEnforcedTargetResource()) {
            throw new UnassignedActionException();
        }
        // WARN: Parameter impl is ignored
        assignResource(targetWorker);
        assignImplementation(impl);
        targetWorker.scheduleAction(this);
    }

    @Override
    public String toString() {
        return "ReduceWorkerAction (Worker " + this.worker.getName() + ")";
    }

    @Override
    public int getPriority() {
        return Integer.MAX_VALUE;
    }

}
