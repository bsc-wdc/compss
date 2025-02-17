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
package es.bsc.compss.scheduler.types.allocatableactions;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.HTTPImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceType;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;
import es.bsc.compss.types.resources.updates.PerformedReduction;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ResourceManager;
import es.bsc.compss.worker.COMPSsException;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;


public class StopWorkerAction extends AllocatableAction {

    private final ResourceScheduler<? extends WorkerResourceDescription> worker;
    private final Implementation impl;
    private final PerformedReduction<WorkerResourceDescription> ru;


    /*
     * ***************************************************************************************************************
     * CONSTRUCTOR
     * ***************************************************************************************************************
     */
    /**
     * Creates a new StopWorkerAction instance.
     *
     * @param schedulingInformation Associated scheduling information.
     * @param worker Associated worker ResourceScheduler.
     * @param ts Associated TaskScheduler.
     * @param modification Stop modification.
     */
    @SuppressWarnings("unchecked")
    public StopWorkerAction(SchedulingInformation schedulingInformation,
        ResourceScheduler<? extends WorkerResourceDescription> worker, TaskScheduler ts,
        PerformedReduction<? extends WorkerResourceDescription> modification) {

        super(schedulingInformation, ts.getOrchestrator());
        this.worker = worker;
        this.ru = (PerformedReduction<WorkerResourceDescription>) modification;
        if (worker.getResource().getType() == ResourceType.WORKER) {
            this.impl = AbstractMethodImplementation.generateDummy(new MethodResourceDescription());
        } else {
            this.impl = HTTPImplementation.generateDummy();
        }
    }

    /*
     * ***************************************************************************************************************
     * EXECUTION AND LIFECYCLE MANAGEMENT
     * ***************************************************************************************************************
     */
    @Override
    public boolean isToReserveResources() {
        return false;
    }

    @Override
    public boolean isToReleaseResources() {
        return false;
    }

    @Override
    public boolean isToStopResource() {
        return true;
    }

    @Override
    protected void doAction() {
        (new Thread() {

            @SuppressWarnings("unchecked")
            @Override
            public void run() {
                Worker<WorkerResourceDescription> wResource = (Worker<WorkerResourceDescription>) worker.getResource();
                Thread.currentThread().setName(wResource.getName() + " stopper");
                wResource.retrieveUniqueDataValues();
                wResource.disableExecution();
                wResource.retrieveTracingAndDebugData();
                Semaphore sem = new Semaphore(0);
                ShutdownListener sl = new ShutdownListener(sem);
                wResource.stop(sl);
                sl.enable();
                try {
                    sem.acquire();
                } catch (Exception e) {
                    LOGGER.error("ERROR: Exception raised on worker shutdown", e);
                    ErrorManager.warn("Exception stopping worker. Check runtime.log for more details", e);
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
    protected void doAbort() {
    }

    @Override
    protected void doCompleted() {
        removeResource();
    }

    private void removeResource() {
        Worker<? extends WorkerResourceDescription> w = worker.getResource();
        if (w instanceof CloudMethodWorker) {
            CloudMethodWorker cmw = (CloudMethodWorker) w;
            ResourceManager.terminateCloudResource(cmw, (CloudMethodResourceDescription) ru.getModification());
        }
        ResourceManager.removeWorker(w);
    }

    @Override
    protected void doError() throws FailedActionException {
        throw new FailedActionException();
    }

    @Override
    protected void doFailed() {
        removeResource();
    }

    @Override
    protected boolean doCanceled() {
        removeResource();
        return true;
    }

    @Override
    protected void doFailIgnored() {

    }

    @Override
    protected Collection<AllocatableAction> doException(COMPSsException e) {
        return new LinkedList<>();
    }

    /*
     * ***************************************************************************************************************
     * SCHEDULING MANAGEMENT
     * ***************************************************************************************************************
     */
    @Override
    public Integer getCoreId() {
        return null;
    }

    @Override
    public LinkedList<ResourceScheduler<? extends WorkerResourceDescription>> getCompatibleWorkers() {
        LinkedList<ResourceScheduler<? extends WorkerResourceDescription>> workers = new LinkedList<>();
        workers.add(this.worker);
        return workers;
    }

    @Override
    public Implementation[] getImplementations() {
        Implementation[] impls = new Implementation[1];
        impls[0] = this.impl;
        return impls;
    }

    @Override
    public <W extends WorkerResourceDescription> boolean isCompatible(Worker<W> r) {
        return (r == this.worker.getResource());
    }

    @Override
    public <T extends WorkerResourceDescription> LinkedList<Implementation>
        getCompatibleImplementations(ResourceScheduler<T> r) {
        LinkedList<Implementation> impls = new LinkedList<>();
        if (r == this.worker) {
            impls.add(this.impl);
        }
        return impls;
    }

    @Override
    public <T extends WorkerResourceDescription> Score schedulingScore(ResourceScheduler<T> targetWorker,
        Score actionScore) {

        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        schedule((ResourceScheduler<WorkerResourceDescription>) this.worker, this.impl);
    }

    @Override
    public void schedule(Collection<ResourceScheduler<? extends WorkerResourceDescription>> candidates,
        Score actionScore) throws UnassignedActionException {
        if (!candidates.contains(this.worker)) {
            throw new UnassignedActionException();
        }
        schedule(this.worker, this.impl);
    }

    @Override
    public void schedule(ResourceScheduler<? extends WorkerResourceDescription> targetWorker, Score actionScore)
        throws UnassignedActionException {
        if (targetWorker != this.worker) {
            throw new UnassignedActionException();
        }
        schedule(targetWorker, this.impl);
    }

    @Override
    public void schedule(ResourceScheduler<? extends WorkerResourceDescription> targetWorker, Implementation impl)
        throws UnassignedActionException {

        if (targetWorker != this.worker) {
            throw new UnassignedActionException();
        }
        // WARN: Parameter impl is ignored
        assignResource(targetWorker);
        assignImplementation(impl);
        targetWorker.scheduleAction(this);
    }

    @Override
    public String toString() {
        return "StopWorkerAction (Worker " + this.worker.getName() + ")";
    }

    @Override
    public int getPriority() {
        return Integer.MAX_VALUE;
    }

    @Override
    public long getGroupPriority() {
        return ACTION_STOP_WORKER;
    }

    @Override
    public OnFailure getOnFailure() {
        return OnFailure.RETRY;
    }

    @Override
    public boolean checkIfCanceled(AllocatableAction aa) {
        return false;
    }

    @Override
    protected void stopAction() throws Exception {
    }

}
