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
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.implementations.ServiceImplementation;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.ErrorManager;

import java.util.LinkedList;


public class StartWorkerAction<T extends WorkerResourceDescription> extends AllocatableAction {

    private final ResourceScheduler<T> worker;
    private final Implementation impl;


    /*
     * ***************************************************************************************************************
     * CONSTRUCTOR
     * ***************************************************************************************************************
     */
    public StartWorkerAction(SchedulingInformation schedulingInformation, ResourceScheduler<T> worker, TaskScheduler ts) {
        super(schedulingInformation, ts.getOrchestrator());
        this.worker = worker;
        switch (worker.getResource().getType()) {
            case WORKER:
            case MASTER:
                Worker<T> mw = worker.getResource();
                impl = new MethodImplementation("", "", null, null, (MethodResourceDescription) mw.getDescription());
                break;
            default:
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
        return true;
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
                Worker<WorkerResourceDescription> workerResource = (Worker<WorkerResourceDescription>) worker.getResource();
                Thread.currentThread().setName(workerResource.getName() + " starter");
                try {
                    workerResource.start();
                    notifyCompleted();
                } catch (InitNodeException e) {
                    LOGGER.error("Error starting resource", e);
                    ErrorManager.warn("Exception creating worker. Check runtime.log for more details", e);
                    notifyError();
                }
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
        // Notify worker available
        LOGGER.info("Worker " + worker.getName() + " is ready to execute tasks.");
    }

    @Override
    protected void doError() throws FailedActionException {
        throw new FailedActionException();
    }

    @Override
    protected void doFailed() {
        // Notify worker available
        LOGGER.info("Worker " + worker.getName() + " could not be started.");
        // Worker<T, I> wNode = this.worker.getResource();

        // Remove from the pool
        // ResourceManager.removeWorker(wNode);
        // Remove all resources assigned to the node
        // ResourceDescription rd = wNode.getDescription();
        // rd.reduce(rd);
        // Update the CE and Implementations that can run (none)
        // this.worker.getResource().updatedFeatures();
        // this.ts.updatedWorker(wNode);
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
        Implementation[] impls = new Implementation[]{impl};
        return impls;
    }

    @Override
    public <W extends WorkerResourceDescription> boolean isCompatible(Worker<W> r) {
        return (r == this.worker.getResource());
    }

    @Override
    public <R extends WorkerResourceDescription> LinkedList<Implementation> getCompatibleImplementations(ResourceScheduler<R> r) {
        LinkedList<Implementation> impls = new LinkedList<>();
        if (r == this.worker) {
            impls.add(this.impl);
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
        schedule((ResourceScheduler<WorkerResourceDescription>) this.worker, this.impl);
    }

    @Override
    public void tryToSchedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        this.schedule(actionScore);
    }

    @Override
    public <R extends WorkerResourceDescription> void schedule(ResourceScheduler<R> targetWorker, Score actionScore)
            throws BlockedActionException, UnassignedActionException {
        schedule(targetWorker, this.impl);
    }

    @Override
    public <R extends WorkerResourceDescription> void schedule(ResourceScheduler<R> targetWorker, Implementation impl)
            throws BlockedActionException, UnassignedActionException {

        if (targetWorker != getEnforcedTargetResource()) {
            throw new UnassignedActionException();
        }
        // WARN: Parameter impl is ignored
        assignResource(targetWorker);
        assignImplementation(this.impl);
        targetWorker.scheduleAction(this);
    }

    @Override
    public String toString() {
        return "StartWorkerAction (Worker " + this.worker.getName() + ")";
    }

    @Override
    public int getPriority() {
        return Integer.MAX_VALUE;
    }

}
