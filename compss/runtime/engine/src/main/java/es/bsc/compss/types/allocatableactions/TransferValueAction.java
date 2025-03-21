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
package es.bsc.compss.types.allocatableactions;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.parameter.impl.CollectiveParameter;
import es.bsc.compss.types.parameter.impl.DependencyParameter;
import es.bsc.compss.types.parameter.impl.ExternalPSCOParameter;
import es.bsc.compss.types.parameter.impl.Parameter;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.worker.COMPSsException;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TransferValueAction<T extends WorkerResourceDescription> extends AllocatableAction {

    // LOGGER
    private static final Logger JOB_LOGGER = LogManager.getLogger(Loggers.FTM_COMP);

    private static final Implementation DUMMY_IMPL = AbstractMethodImplementation.generateDummy(null);

    private final DependencyParameter dataToTransfer;
    private final ResourceScheduler<T> receiver;


    /**
     * Creates a new transfer value action.
     *
     * @param schedulingInformation Associated scheduling information.
     * @param orchestrator Task orchestrator.
     * @param dp Dependency parameter to transfer.
     * @param receiver ResourceScheduler representing the worker receiver.
     */
    public TransferValueAction(SchedulingInformation schedulingInformation, ActionOrchestrator orchestrator,
        DependencyParameter dp, ResourceScheduler<T> receiver) {

        super(schedulingInformation, orchestrator);
        this.receiver = receiver;
        this.dataToTransfer = dp;
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
        return false;
    }

    @Override
    protected void doAction() {
        JOB_LOGGER.info("Ordering transfers of " + this.dataToTransfer.getName() + " to " + receiver.getName());
        ObtainDataListener listener = new ObtainDataListener();
        transferData(this.dataToTransfer, listener);
        listener.enable();
    }

    // Private method that performs data transfers
    private void transferData(DependencyParameter dataToTransfer, ObtainDataListener listener) {
        if (dataToTransfer.isCollective()) {
            CollectiveParameter cp = (CollectiveParameter) dataToTransfer;
            JOB_LOGGER.debug("Detected CollectionParameter " + cp);
            // TODO: Handle basic data types
            for (Parameter p : cp.getElements()) {
                DependencyParameter dp = (DependencyParameter) p;
                transferData(dp, listener);
            }
        }

        Worker<? extends WorkerResourceDescription> w = getAssignedResource().getResource();
        EngineDataAccessId access = dataToTransfer.getDataAccessId();
        if (access.isRead()) {
            listener.addOperation();
            LogicalData srcData = ((ReadingDataAccessId) access).getReadDataInstance().getData();
            if (access.isWrite()) {
                w.getData(srcData, dataToTransfer, listener);
            } else {
                // Is RWAccess
                String tgtName = ((WritingDataAccessId) access).getWrittenDataInstance().getRenaming();
                w.getData(srcData, tgtName, (LogicalData) null, dataToTransfer, listener);
            }
        } else {
            String tgtName = ((WritingDataAccessId) access).getWrittenDataInstance().getRenaming();
            // Workaround for return objects in bindings converted to PSCOs inside tasks
            if (dataToTransfer instanceof ExternalPSCOParameter) {
                ExternalPSCOParameter epp = (ExternalPSCOParameter) dataToTransfer;
                tgtName = epp.getId();
            }
            if (DEBUG) {
                JOB_LOGGER.debug(
                    "Setting data target job transfer: " + w.getCompleteRemotePath(dataToTransfer.getType(), tgtName));
            }
            dataToTransfer.setDataTarget(w.getCompleteRemotePath(dataToTransfer.getType(), tgtName).getPath());
            return;
        }
    }

    /*
     * ***************************************************************************************************************
     * EXECUTED SUPPORTING THREAD ON JOB_TRANSFERS_LISTENER
     * ***************************************************************************************************************
     */
    /**
     * Flushes the current copies.
     */
    public void flushCopies() {
        Worker<? extends WorkerResourceDescription> w = getAssignedResource().getResource();
        FlushCopyListener listener = new FlushCopyListener();
        w.enforceDataObtaning(this.dataToTransfer, listener);
    }

    /**
     * Code executed when the value transfer has been completed.
     */
    public final void completedTransfer() {
        // Notify completion
        notifyCompleted();
    }

    /*
     * ***************************************************************************************************************
     * EXECUTION TRIGGERS
     * ***************************************************************************************************************
     */
    @Override
    protected void doCompleted() {

    }

    @Override
    protected void doError() throws FailedActionException {
        throw new FailedActionException();
    }

    @Override
    protected void doAbort() {
        // Do nothing
    }

    @Override
    protected void doFailed() {
        ErrorManager.warn("Transfer of data " + dataToTransfer.getName() + " to " + receiver + " has failed.");
    }

    @Override
    protected boolean doCanceled() {
        ErrorManager.warn("Transfer of data " + dataToTransfer.getName() + " to " + receiver + " has been cancelled.");
        return true;
    }

    @Override
    protected void doFailIgnored() {
        // Failed log message
        ErrorManager.warn("Transfer of data " + dataToTransfer.getName() + " to " + receiver + " has failed.");
    }

    @Override
    protected Collection<AllocatableAction> doException(COMPSsException e) {
        ErrorManager.warn(
            "Transfer of data " + dataToTransfer.getName() + " to " + receiver + " has raised a COMPSs Exception.");
        return new LinkedList<>();
    }

    /*
     * ***************************************************************************************************************
     * SCHEDULING MANAGEMENT
     * ***************************************************************************************************************
     */
    @Override
    public final List<ResourceScheduler<? extends WorkerResourceDescription>> getCompatibleWorkers() {
        List<ResourceScheduler<? extends WorkerResourceDescription>> compatible = new LinkedList<>();
        compatible.add(receiver);
        return compatible;
    }

    @Override
    public final Implementation[] getImplementations() {
        return new Implementation[] { DUMMY_IMPL };
    }

    @Override
    public <W extends WorkerResourceDescription> boolean isCompatible(Worker<W> r) {
        return r == this.receiver.getResource();
    }

    @Override
    public final <W extends WorkerResourceDescription> List<Implementation>
        getCompatibleImplementations(ResourceScheduler<W> r) {

        List<Implementation> impls = new LinkedList<>();
        impls.add(DUMMY_IMPL);
        return impls;
    }

    @Override
    public final Integer getCoreId() {
        return null;
    }

    @Override
    public final int getPriority() {
        return 1;
    }

    @Override
    public long getGroupPriority() {
        return ACTION_VALUE_TRANSFER;
    }

    @Override
    public OnFailure getOnFailure() {
        return OnFailure.IGNORE;
    }

    @Override
    public final <W extends WorkerResourceDescription> Score schedulingScore(ResourceScheduler<W> targetWorker,
        Score actionScore) {
        if (targetWorker == this.receiver) {
            return actionScore;
        }
        return null;
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        schedule(this.receiver, this.DUMMY_IMPL);
    }

    @Override
    public void schedule(Collection<ResourceScheduler<? extends WorkerResourceDescription>> candidates,
        Score actionScore) throws UnassignedActionException {
        if (!candidates.contains(this.receiver)) {
            throw new UnassignedActionException();
        }
        schedule(this.receiver, this.DUMMY_IMPL);
    }

    @Override
    public void schedule(ResourceScheduler<? extends WorkerResourceDescription> targetWorker, Score actionScore)
        throws UnassignedActionException {
        if (targetWorker != this.receiver) {
            schedule(targetWorker, this.DUMMY_IMPL);
        }
    }

    @Override
    public void schedule(ResourceScheduler<? extends WorkerResourceDescription> targetWorker, Implementation impl)
        throws UnassignedActionException {

        if (targetWorker != this.receiver) {
            throw new UnassignedActionException();
        }
        // WARN: Parameter impl is ignored
        assignResource(targetWorker);
        assignImplementation(impl);
        targetWorker.scheduleAction(this);
    }

    /*
     * ***************************************************************************************************************
     * OTHER
     * ***************************************************************************************************************
     */
    @Override
    public String toString() {
        return "TransferAction ( Data " + this.dataToTransfer.getName() + ", receiver " + this.receiver.getName() + ")";
    }


    private class ObtainDataListener extends EventListener {

        private int operation = 0;
        private int errors = 0;
        private boolean enabled = false;


        public ObtainDataListener() {
        }

        @Override
        public void notifyEnd(DataOperation d) {
            boolean enabled;
            boolean finished;
            boolean failed;
            synchronized (this) {
                operation--;
                finished = operation == 0;
                failed = errors > 0;
                enabled = this.enabled;
            }
            if (finished && enabled) {
                if (failed) {
                    TransferValueAction.this.notifyError();
                } else {
                    flushCopies();
                }
            }
        }

        @Override
        public void notifyFailure(DataOperation d, Exception excptn) {
            ErrorManager.warn("Transfer for data " + TransferValueAction.this.dataToTransfer + " to "
                + TransferValueAction.this.receiver.getName() + " has failed.");

            boolean enabled;
            boolean finished;
            synchronized (this) {
                errors++;
                operation--;
                finished = operation == 0;
                enabled = this.enabled;
            }
            if (enabled && finished) {
                TransferValueAction.this.notifyError();
            }
        }

        public void enable() {
            boolean finished;
            boolean failed;
            synchronized (this) {
                enabled = true;
                finished = (operation == 0);
                failed = (errors > 0);
            }
            if (finished) {
                if (failed) {
                    TransferValueAction.this.notifyError();
                } else {
                    flushCopies();
                }
            }
        }

        public synchronized void addOperation() {
            operation++;
        }
    }

    private class FlushCopyListener extends EventListener {

        @Override
        public void notifyEnd(DataOperation d) {
            TransferValueAction.this.completedTransfer();
        }

        @Override
        public void notifyFailure(DataOperation d, Exception excptn) {
            TransferValueAction.this.notifyError();
        }

    }


    @Override
    public boolean checkIfCanceled(AllocatableAction aa) {
        return false;
    }

    @Override
    protected void stopAction() throws Exception {

    }
}
