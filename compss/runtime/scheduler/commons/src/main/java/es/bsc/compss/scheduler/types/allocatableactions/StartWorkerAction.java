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

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.exceptions.InitNodeException;
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
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ResourceManager;
import es.bsc.compss.worker.COMPSsException;

import java.util.Collection;
import java.util.LinkedList;


public class StartWorkerAction<T extends WorkerResourceDescription> extends AllocatableAction {

    private final ResourceScheduler<T> worker;
    private final Implementation impl;
    private final TaskScheduler ts;
    private static final boolean STOP_EXEC_IN_NODE_FAIL =
        Boolean.parseBoolean(System.getProperty(COMPSsConstants.SHUTDOWN_IN_NODE_FAILURE));


    /*
     * ***************************************************************************************************************
     * CONSTRUCTOR
     * ***************************************************************************************************************
     */
    /**
     * Creates a new StartWorkerAction instance.
     * 
     * @param schedulingInformation Associated scheduling information.
     * @param worker Associated worker ResourceScheduler.
     * @param ts Associated Task Scheduler
     */
    public StartWorkerAction(SchedulingInformation schedulingInformation, ResourceScheduler<T> worker,
        TaskScheduler ts) {

        super(schedulingInformation, ts.getOrchestrator());
        this.ts = ts;
        this.worker = worker;
        this.worker.getResource().startingNode();

        switch (worker.getResource().getType()) {
            case WORKER:
            case MASTER:
                Worker<T> mw = worker.getResource();
                if (mw.getDescription() instanceof MethodResourceDescription) {
                    this.impl =
                        AbstractMethodImplementation.generateDummy((MethodResourceDescription) mw.getDescription());
                } else {
                    this.impl = new Implementation() {

                        @Override
                        public TaskType getTaskType() {
                            return null;
                        }

                        @Override
                        public WorkerResourceDescription getRequirements() {
                            return mw.getDescription();
                        }
                    };
                }
                break;
            default: // HTTP
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
                Worker<WorkerResourceDescription> workerResource =
                    (Worker<WorkerResourceDescription>) worker.getResource();
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
    protected void doAbort() {
    }

    @Override
    protected void doCompleted() {
        // Notify worker available
        LOGGER.info("Worker " + this.worker.getName() + " is ready to execute tasks.");
    }

    @Override
    protected void doError() throws FailedActionException {
        throw new FailedActionException();
    }

    @Override
    protected void doFailed() {
        LOGGER.error("Worker " + this.worker.getName() + " could not be started.");
        if (STOP_EXEC_IN_NODE_FAIL) {
            ErrorManager.fatal(" Execution stopped due to node: " + this.worker.getName() + " failure.");
        }

        this.ts.removeResource(this.worker);

        ResourceDescription rd = this.worker.getResource().getDescription();
        rd.reduce(rd);
        this.worker.getResource().updatedFeatures();
        SchedulingInformation.changesOnWorker(this.worker);

        Worker wNode = this.worker.getResource();
        ResourceManager.removeWorker(wNode);
    }

    @Override
    protected boolean doCanceled() {
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
        Implementation[] impls = new Implementation[] { this.impl };
        return impls;
    }

    @Override
    public <W extends WorkerResourceDescription> boolean isCompatible(Worker<W> r) {
        return (r == this.worker.getResource());
    }

    @Override
    public <R extends WorkerResourceDescription> LinkedList<Implementation>
        getCompatibleImplementations(ResourceScheduler<R> r) {
        LinkedList<Implementation> impls = new LinkedList<>();
        if (r == this.worker) {
            impls.add(this.impl);
        }
        return impls;
    }

    @Override
    public <R extends WorkerResourceDescription> Score schedulingScore(ResourceScheduler<R> targetWorker,
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

    @Override
    public long getGroupPriority() {
        return ACTION_START_WORKER;
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
