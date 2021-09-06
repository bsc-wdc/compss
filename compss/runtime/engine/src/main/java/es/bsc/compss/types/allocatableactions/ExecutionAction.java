/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.CommutativeGroupTask;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.TaskGroup;
import es.bsc.compss.types.TaskState;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.operation.JobTransfersListener;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.types.job.JobHistory;
import es.bsc.compss.types.job.JobStatusListener;
import es.bsc.compss.types.parameter.CollectionParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.DictCollectionParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.JobDispatcher;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.worker.COMPSsException;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;


public class ExecutionAction extends AllocatableAction {

    // Fault tolerance parameters
    private static final int TRANSFER_CHANCES = 2;
    private static final int SUBMISSION_CHANCES = 2;
    private static final int SCHEDULING_CHANCES = 2;

    // LOGGER
    private static final Logger JOB_LOGGER = LogManager.getLogger(Loggers.JM_COMP);

    // Execution Info
    protected final AccessProcessor ap;
    protected final Task task;
    private final LinkedList<Integer> jobs;
    private int transferErrors = 0;
    protected int executionErrors = 0;
    protected Job<?> currentJob;
    boolean cancelledBeforeSubmit = false;
    boolean extraResubmit = false;


    /**
     * Creates a new execution action.
     *
     * @param schedulingInformation Associated scheduling information.
     * @param orchestrator Task orchestrator.
     * @param ap Access processor.
     * @param task Associated task.
     */
    public ExecutionAction(SchedulingInformation schedulingInformation, ActionOrchestrator orchestrator,
        AccessProcessor ap, Task task) {

        super(schedulingInformation, orchestrator);

        this.ap = ap;
        this.task = task;
        this.jobs = new LinkedList<>();
        this.transferErrors = 0;
        this.executionErrors = 0;

        // Add execution to task
        this.task.addExecution(this);

        synchronized (this.task) {
            // Register data dependencies
            registerDataDependencies();
            // Register stream producers
            registerStreamProducers();
        }

        // Scheduling constraints
        // Restricted resource
        Task resourceConstraintTask = this.task.getEnforcingTask();
        if (resourceConstraintTask != null) {
            for (AllocatableAction e : resourceConstraintTask.getExecutions()) {
                addResourceConstraint(e);
            }
        }
    }

    private void registerStreamProducers() {
        for (AbstractTask predecessor : this.task.getStreamProducers()) {
            for (AllocatableAction e : ((Task) predecessor).getExecutions()) {
                if (e != null && e.isPending()) {
                    addStreamProducer(e);
                }
            }
        }

    }

    private void registerDataDependencies() {
        List<AbstractTask> predecessors = this.task.getPredecessors();
        for (AbstractTask predecessor : predecessors) {
            if (!(predecessor instanceof CommutativeGroupTask)) {
                treatStandardPredecessor(predecessor);
            } else {
                treatCommutativePredecessor(predecessor);
            }
        }

    }

    private void treatCommutativePredecessor(AbstractTask predecessor) {
        if (DEBUG) {
            LOGGER.debug("Task has a commutative group as a predecessor");
        }
        for (Task t : ((CommutativeGroupTask) predecessor).getCommutativeTasks()) {
            for (AllocatableAction com : t.getExecutions()) {
                if (!com.getDataPredecessors().contains(this)) {
                    this.addDataPredecessor(com);
                }
            }
        }
    }

    private void treatStandardPredecessor(AbstractTask predecessor) {
        for (AllocatableAction e : predecessor.getExecutions()) {
            if (e != null && e.isPending()) {
                addDataPredecessor(e);
            } else {
                addAlreadyDoneAction(e);

            }
        }

    }

    /**
     * Returns the associated task.
     *
     * @return The associated task.
     */
    public final Task getTask() {
        return this.task;
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
        JOB_LOGGER.info("Ordering transfers to " + getAssignedResource() + " to run task: " + this.task.getId());
        this.transferErrors = 0;
        this.executionErrors = 0;
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onSubmission();
        doInputTransfers();
        for (CommutativeGroupTask com : this.getTask().getCommutativeGroupList()) {
            com.taskBeingExecuted(this.getTask().getId());
        }
    }

    @Override
    public boolean taskIsReadyForExecution() {
        return this.task.canBeExecuted();
    }

    @Override
    public boolean checkIfCanceled(AllocatableAction aa) {
        return (aa instanceof ExecutionAction) && (((ExecutionAction) aa).getTask().getStatus() == TaskState.CANCELED);
    }

    private void doInputTransfers() {
        JobTransfersListener listener = new JobTransfersListener(this);
        transferInputData(listener);
        listener.enable();
    }

    private void transferInputData(JobTransfersListener listener) {
        TaskDescription taskDescription = this.task.getTaskDescription();
        for (Parameter p : taskDescription.getParameters()) {
            if (DEBUG) {
                JOB_LOGGER.debug("    * " + p);
            }
            if (p instanceof DependencyParameter) {
                DependencyParameter dp = (DependencyParameter) p;
                switch (taskDescription.getType()) {
                    case METHOD:
                        transferJobData(dp, listener);
                        break;

                    case HTTP:
                    case SERVICE:
                        if (dp.getDirection() != Direction.INOUT) {
                            // For services we only transfer IN parameters because the only
                            // parameter that can be INOUT is the target
                            transferJobData(dp, listener);
                        }
                        break;
                }
            }
        }
    }

    // Private method that performs data transfers
    private void transferJobData(DependencyParameter param, JobTransfersListener listener) {
        switch (param.getType()) {
            case COLLECTION_T:
                CollectionParameter cp = (CollectionParameter) param;
                JOB_LOGGER.debug("Detected CollectionParameter " + cp);
                // Recursively send all the collection parameters
                for (Parameter p : cp.getParameters()) {
                    if (p instanceof DependencyParameter) {
                        DependencyParameter dp = (DependencyParameter) p;
                        transferJobData(dp, listener);
                    }
                }
                // Send the collection parameter itself
                transferSingleParameter(param, listener);
                break;
            case DICT_COLLECTION_T:
                DictCollectionParameter dcp = (DictCollectionParameter) param;
                JOB_LOGGER.debug("Detected DictCollectionParameter " + dcp);
                // Recursively send all the dictionary collection parameters
                for (Map.Entry<Parameter, Parameter> entry : dcp.getParameters().entrySet()) {
                    Parameter k = entry.getKey();
                    if (k instanceof DependencyParameter) {
                        DependencyParameter dpKey = (DependencyParameter) k;
                        transferJobData(dpKey, listener);
                    }
                    Parameter v = entry.getValue();
                    if (v instanceof DependencyParameter) {
                        DependencyParameter dpValue = (DependencyParameter) v;
                        transferJobData(dpValue, listener);
                    }
                }
                // Send the collection parameter itself
                transferSingleParameter(param, listener);
                break;
            case STREAM_T:
            case EXTERNAL_STREAM_T:
                // Stream stubs are always transferred independently of their access
                transferStreamParameter(param, listener);
                break;
            default:
                transferSingleParameter(param, listener);
                break;
        }
    }

    private void transferSingleParameter(DependencyParameter param, JobTransfersListener listener) {
        Worker<? extends WorkerResourceDescription> w = getAssignedResource().getResource();
        DataAccessId access = param.getDataAccessId();
        if (access instanceof WAccessId) {
            String dataTarget =
                w.getOutputDataTargetPath(((WAccessId) access).getWrittenDataInstance().getRenaming(), param);
            param.setDataTarget(dataTarget);

        } else {
            if (access instanceof RAccessId) {
                // Read Access, transfer object
                listener.addOperation();

                String srcName = ((RAccessId) access).getReadDataInstance().getRenaming();
                w.getData(srcName, srcName, param, listener);
            } else {
                // ReadWrite Access, transfer object
                listener.addOperation();
                String srcName = ((RWAccessId) access).getReadDataInstance().getRenaming();
                String tgtName = ((RWAccessId) access).getWrittenDataInstance().getRenaming();
                LogicalData tmpData = Comm.registerData("tmp" + tgtName, null);
                w.getData(srcName, tgtName, tmpData, param, listener);
            }
        }
    }

    private void transferStreamParameter(DependencyParameter param, JobTransfersListener listener) {
        DataAccessId access = param.getDataAccessId();
        String source;
        String target;
        if (access instanceof WAccessId) {
            WAccessId wAccess = (WAccessId) access;
            source = wAccess.getWrittenDataInstance().getRenaming();
            target = source;
        } else {
            if (access instanceof RAccessId) {
                RAccessId rAccess = (RAccessId) access;
                source = rAccess.getReadDataInstance().getRenaming();
                target = source;
            } else {
                RWAccessId rwAccess = (RWAccessId) access;
                source = rwAccess.getReadDataInstance().getRenaming();
                target = rwAccess.getWrittenDataInstance().getRenaming();
            }
        }

        // Ask for transfer
        Worker<? extends WorkerResourceDescription> w = getAssignedResource().getResource();
        if (DEBUG) {
            JOB_LOGGER.debug("Requesting stream transfer from " + source + " to " + target + " at " + w.getName());
        }
        listener.addOperation();
        w.getData(source, target, param, listener);
    }

    /*
     * ***************************************************************************************************************
     * EXECUTED SUPPORTING THREAD ON JOB_TRANSFERS_LISTENER
     * ***************************************************************************************************************
     */
    /**
     * Code executed after some input transfers have failed.
     *
     * @param failedtransfers Number of failed transfers.
     */
    public final void failedTransfers(int failedtransfers) {
        JOB_LOGGER
            .debug("Received a notification for the transfers for task " + this.task.getId() + " with state FAILED");
        ++this.transferErrors;
        if (this.transferErrors < TRANSFER_CHANCES && this.task.getOnFailure() == OnFailure.RETRY) {
            JOB_LOGGER.debug("Resubmitting input files for task " + this.task.getId() + " to host "
                + getAssignedResource().getName() + " since " + failedtransfers + " transfers failed.");
            doInputTransfers();
        } else {
            ErrorManager.warn("Transfers for running task " + this.task.getId() + " on worker "
                + getAssignedResource().getName() + " have failed.");
            this.notifyError();
            removeJobTempData();

        }
    }

    /**
     * Code executed when all transfers have succeeded.
     *
     * @param transferGroupId Transferring group Id.
     */
    public final void doSubmit(int transferGroupId) {
        JOB_LOGGER.debug("Received a notification for the transfers of task " + this.task.getId() + " with state DONE");
        JobStatusListener listener = new JobStatusListener(this);
        Job<?> job = submitJob(transferGroupId, listener);
        if (!this.cancelledBeforeSubmit) {
            // Register job
            this.jobs.add(job.getJobId());
            JOB_LOGGER.info((getExecutingResources().size() > 1 ? "Rescheduled" : "New") + " Job " + job.getJobId()
                + " (Task: " + this.task.getId() + ")");
            JOB_LOGGER.info("  * Method name: " + this.task.getTaskDescription().getName());
            JOB_LOGGER.info("  * Target host: " + this.getAssignedResource().getName());

            this.profile.start();
            JobDispatcher.dispatch(job);
            JOB_LOGGER.info("Submitted Task: " + this.task.getId() + " Job: " + job.getJobId() + " Method: "
                + this.task.getTaskDescription().getName() + " Resource: " + this.getAssignedResource().getName());
        } else {
            JOB_LOGGER.info("Job" + job.getJobId() + " cancelled before submission.");
        }
    }

    private void removeJobTempData() {
        TaskDescription taskDescription = this.task.getTaskDescription();
        for (Parameter p : taskDescription.getParameters()) {
            if (DEBUG) {
                JOB_LOGGER.debug("    * " + p);
            }
            if (p instanceof DependencyParameter) {
                DependencyParameter dp = (DependencyParameter) p;
                switch (taskDescription.getType()) {
                    case METHOD:
                        removeTmpData(dp);
                        break;

                    case HTTP:
                    case SERVICE:
                        if (dp.getDirection() != Direction.INOUT) {
                            // For services we only transfer IN parameters because the only
                            // parameter that can be INOUT is the target
                            removeTmpData(dp);
                        }
                        break;
                }
            }
        }

    }

    private void removeTmpData(DependencyParameter param) {
        if (param.getType() != DataType.STREAM_T && param.getType() != DataType.EXTERNAL_STREAM_T) {
            if (param.getType() == DataType.COLLECTION_T) {
                CollectionParameter cp = (CollectionParameter) param;
                JOB_LOGGER.debug("Detected CollectionParameter " + cp);
                // Recursively send all the collection parameters
                for (Parameter p : cp.getParameters()) {
                    if (p instanceof DependencyParameter) {
                        DependencyParameter dp = (DependencyParameter) p;
                        removeTmpData(dp);
                    }
                }
            }
            if (param.getType() == DataType.DICT_COLLECTION_T) {
                DictCollectionParameter dcp = (DictCollectionParameter) param;
                JOB_LOGGER.debug("Detected DictCollectionParameter " + dcp);
                // Recursively send all the dictionary collection parameters
                for (Map.Entry<Parameter, Parameter> entry : dcp.getParameters().entrySet()) {
                    Parameter k = entry.getKey();
                    if (k instanceof DependencyParameter) {
                        DependencyParameter dpKey = (DependencyParameter) k;
                        removeTmpData(dpKey);
                    }
                    Parameter v = entry.getValue();
                    if (v instanceof DependencyParameter) {
                        DependencyParameter dpValue = (DependencyParameter) v;
                        removeTmpData(dpValue);
                    }
                }
            }

            DataAccessId access = param.getDataAccessId();
            if (access instanceof RWAccessId) {
                String tgtName = "tmp" + ((RWAccessId) access).getWrittenDataInstance().getRenaming();
                Comm.removeDataKeepingValue(tgtName);
            }
        }

    }

    protected Job<?> submitJob(int transferGroupId, JobStatusListener listener) {
        // Create job
        if (DEBUG) {
            LOGGER.debug(this.toString() + " starts job creation");
        }
        Worker<? extends WorkerResourceDescription> w = getAssignedResource().getResource();
        List<String> slaveNames = new ArrayList<>(); // No salves

        // Get predecessors for task dependency tracing
        List<Integer> predecessors = null;
        if (Tracer.isActivated() && Tracer.isTracingTaskDependencies()) {
            predecessors = Tracer.getPredecessors(this.task.getId());
        }
        Job<?> job = w.newJob(this.task.getId(), this.task.getTaskDescription(), this.getAssignedImplementation(),
            slaveNames, listener, predecessors, this.task.getSuccessors().size());
        // Remove predecessors from map for task dependency tracing
        if (Tracer.isActivated() && Tracer.isTracingTaskDependencies()) {
            Tracer.removePredecessor(this.task.getId());
        }
        this.currentJob = job;
        job.setTransferGroupId(transferGroupId);
        job.setHistory(JobHistory.NEW);

        return job;
    }

    /**
     * Code executed to cancel a running execution.
     *
     * @throws Exception Unstarted node exception.
     */
    @Override
    protected void stopAction() throws Exception {
        // Submit stop petition
        if (DEBUG) {
            LOGGER.debug("Task " + this.task.getId() + " starts cancelling running job");
        }
        if (this.currentJob != null) {
            this.currentJob.cancelJob();
            // Update info about the generated/updated data
            doOutputTransfers(this.currentJob);
        } else {
            this.cancelledBeforeSubmit = true;
        }
    }

    /**
     * Code executed when an exception has occurred on the job.
     *
     * @param job Job of exception.
     */
    public final void exceptionJob(Job<?> job, COMPSsException e) {
        this.profile.end();
        // Remove tmpData for IN/OUTS
        removeJobTempData();

        int jobId = job.getJobId();
        JOB_LOGGER.error("Received an exception notification for job " + jobId);
        if (this.task.getStatus() == TaskState.CANCELED) {
            ErrorManager
                .warn("Ingoring notification for job " + jobId + ". Task " + task.getId() + " already cancelled");
        } else {
            if (e instanceof COMPSsException && this.task.hasTaskGroups()) {
                for (TaskGroup t : this.task.getTaskGroupList()) {
                    t.setException((COMPSsException) e);
                }
            }

            // Update info about the generated/updated data
            doOutputTransfers(job);

            notifyException(e);
        }
    }

    /**
     * Code executed when the job execution has failed.
     *
     * @param job Failed job.
     * @param endStatus Exit status.
     */
    public final void failedJob(Job<?> job, JobEndStatus endStatus) {
        this.profile.end();

        // Remove tmpData for IN/OUTS
        removeJobTempData();
        if (this.task.getStatus() == TaskState.CANCELED) {
            JOB_LOGGER.debug("Ignoring notification for cancelled job " + job.getJobId());
        } else {
            if (this.isCancelling()) {
                JOB_LOGGER.debug("Received a notification for cancelled job " + job.getJobId());
                doOutputTransfers(job);

                notifyError();
            } else {
                int jobId = job.getJobId();
                JOB_LOGGER.error("Received a notification for job " + jobId + " with state FAILED");
                JOB_LOGGER.error("Job " + job.getJobId() + ", running Task " + this.task.getId() + " on worker "
                    + this.getAssignedResource().getName() + ", has failed.");
                ErrorManager.warn("Job " + job.getJobId() + ", running Task " + this.task.getId() + " on worker "
                    + this.getAssignedResource().getName() + ", has failed.");
                ++this.executionErrors;
                if (this.transferErrors + this.executionErrors < SUBMISSION_CHANCES
                    && this.task.getOnFailure() == OnFailure.RETRY) {
                    JOB_LOGGER.error("Resubmitting job to the same worker.");
                    ErrorManager.warn("Resubmitting job to the same worker.");
                    job.setHistory(JobHistory.RESUBMITTED);
                    this.profile.start();
                    JobDispatcher.dispatch(job);
                } else {
                    if (this.task.getOnFailure() == OnFailure.IGNORE) {
                        // Update info about the generated/updated data
                        ErrorManager.warn("Ignoring failure.");
                        doOutputTransfers(job);
                    }
                    notifyError();
                }
            }
        }
    }

    /**
     * Code executed when the job execution has been completed.
     *
     * @param job Completed job.
     */
    public final void completedJob(Job<?> job) {
        // End profile
        this.profile.end();

        // Remove tmpData for IN/OUTS
        removeJobTempData();

        // Notify end
        int jobId = job.getJobId();
        JOB_LOGGER.info("Received a notification for job " + jobId + " with state OK (avg. duration: "
            + this.profile.getAverageExecutionTime() + ")");

        if (this.task.getStatus() == TaskState.CANCELED) {
            ErrorManager
                .warn("Ingoring notification for job " + jobId + ". Task " + task.getId() + " already cancelled");
        } else {
            // Job finished, update info about the generated/updated data
            doOutputTransfers(job);
            // Notify completion
            notifyCompleted();
        }
    }

    private final void doOutputTransfers(Job<?> job) {
        switch (job.getType()) {
            case METHOD:
                doMethodOutputTransfers(job);
                break;
            case HTTP:
                doHttpOutputTransfers(job);
                break;
            case SERVICE:
                doServiceOutputTransfers(job);
                break;
        }
    }

    private final void doMethodOutputTransfers(Job<?> job) {
        Worker<? extends WorkerResourceDescription> w = getAssignedResource().getResource();
        TaskMonitor monitor = this.task.getTaskMonitor();
        List<Parameter> params = job.getTaskParams().getParameters();
        for (int i = 0; i < params.size(); ++i) {
            Parameter p = params.get(i);
            String dataName = getOuputRename(p);
            if (dataName != null) {
                DependencyParameter dp = (DependencyParameter) p;
                storeOutputParameter(job, w, dataName, dp);
                Object[] mp = buildMonitorParameter(p, dataName);
                monitor.valueGenerated(i, mp);
            }
        }
    }

    Object[] buildMonitorParameter(Parameter p, String dataName) {
        final int fieldsPerParameter = 3;
        Object[] result;
        if (p.getType() == DataType.COLLECTION_T) {
            result = new Object[fieldsPerParameter + 1];
            List<Parameter> subParams = ((CollectionParameter) p).getParameters();
            Object[][] subResults = new Object[subParams.size()][];
            for (int i = 0; i < subParams.size(); i++) {
                subResults[i] = buildMonitorParameter(subParams.get(i), getOuputRename(subParams.get(i)));
            }
            result[TaskMonitor.SUBPARAM_POS] = subResults;
        } else {
            result = new Object[fieldsPerParameter];
        }
        result[TaskMonitor.TYPE_POS] = p.getType();
        result[TaskMonitor.LOCATION_POS] = ((DependencyParameter) p).getDataTarget();
        result[TaskMonitor.DATA_ID_POS] = dataName;

        return result;
    }

    private String getOuputRename(Parameter p) {
        String name = null;
        if (p instanceof DependencyParameter) {
            // Notify the FileTransferManager about the generated/updated OUT/INOUT datums
            DependencyParameter dp = (DependencyParameter) p;
            DataInstanceId dId = null;
            switch (p.getDirection()) {
                case CONCURRENT:
                case IN_DELETE:
                case IN:
                    // FTM already knows about this datum
                    return null;
                case OUT:
                    dId = ((WAccessId) dp.getDataAccessId()).getWrittenDataInstance();
                    break;
                case COMMUTATIVE:
                    CommutativeGroupTask cgt =
                        this.getTask().getCommutativeGroup(((DependencyParameter) p).getDataAccessId().getDataId());
                    cgt.getCommutativeTasks().remove(this.getTask());
                    cgt.nextVersion();
                    dId = ((RWAccessId) dp.getDataAccessId()).getWrittenDataInstance();
                    break;
                case INOUT:
                    dId = ((RWAccessId) dp.getDataAccessId()).getWrittenDataInstance();
                    Comm.removeDataKeepingValue("tmp" + dId);
                    break;
            }

            // Retrieve parameter information
            name = dId.getRenaming();
        }
        return name;
    }

    private final DataLocation storeOutputParameter(Job<?> job, Worker<? extends WorkerResourceDescription> w,
        String dataName, DependencyParameter p) {
        DependencyParameter dp = (DependencyParameter) p;

        if (dp.getType() == DataType.COLLECTION_T) {
            CollectionParameter cp = (CollectionParameter) p;
            for (Parameter elem : cp.getParameters()) {
                String elemOutRename = getOuputRename(elem);
                if (elemOutRename != null) {
                    storeOutputParameter(job, w, elemOutRename, (DependencyParameter) elem);
                }
            }
        }
        if (dp.getType() == DataType.DICT_COLLECTION_T) {
            DictCollectionParameter dcp = (DictCollectionParameter) p;
            for (Map.Entry<Parameter, Parameter> entry : dcp.getParameters().entrySet()) {
                Parameter k = entry.getKey();
                String elemKeyOutRename = getOuputRename(k);
                if (elemKeyOutRename != null) {
                    storeOutputParameter(job, w, elemKeyOutRename, (DependencyParameter) k);
                }
                Parameter v = entry.getValue();
                String elemValueOutRename = getOuputRename(v);
                if (elemValueOutRename != null) {
                    storeOutputParameter(job, w, elemValueOutRename, (DependencyParameter) v);
                }
            }

        }
        // Request transfer
        DataLocation outLoc = null;
        try {
            String dataTarget = dp.getDataTarget();
            if (DEBUG) {
                JOB_LOGGER.debug("Proposed URI for storing output param: " + dataTarget);
            }
            SimpleURI resultURI = new SimpleURI(dataTarget);
            SimpleURI targetURI = new SimpleURI(resultURI.getSchema() + resultURI.getPath());
            outLoc = DataLocation.createLocation(w, targetURI);
            // Data target has been stored as URI but final target data should be just the path
            dp.setDataTarget(outLoc.getPath());
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + dp.getDataTarget(), e);
        }
        Comm.registerLocation(dataName, outLoc);

        // Return location
        return outLoc;
    }

    private final void doServiceOutputTransfers(Job<?> job) {
        TaskMonitor monitor = this.task.getTaskMonitor();

        // Search for the return object
        List<Parameter> params = job.getTaskParams().getParameters();
        for (int i = params.size() - 1; i >= 0; --i) {
            Parameter p = params.get(i);
            if (p instanceof DependencyParameter) {
                // Check parameter direction
                DataInstanceId dId = null;
                DependencyParameter dp = (DependencyParameter) p;
                switch (p.getDirection()) {
                    case IN:
                    case IN_DELETE:
                    case CONCURRENT:
                    case COMMUTATIVE:
                    case INOUT:
                        // Return value is OUT, skip the current parameter
                        continue;
                    case OUT:
                        dId = ((WAccessId) dp.getDataAccessId()).getWrittenDataInstance();
                        break;
                }

                // Parameter found, store it
                String name = dId.getRenaming();
                Object value = job.getReturnValue();
                LogicalData ld = Comm.registerValue(name, value);

                // Monitor one of its locations
                Set<DataLocation> locations = ld.getLocations();
                if (!locations.isEmpty()) {
                    Object[] mp = buildMonitorParameter(p, getOuputRename(p));
                    for (DataLocation loc : ld.getLocations()) {
                        if (loc != null) {
                            monitor.valueGenerated(i, mp);
                        }
                    }
                }

                // If we reach this point the return value has been registered, we can end
                return;
            }
        }
    }

    private void doHttpOutputTransfers(Job<?> job) {
        TaskMonitor monitor = this.task.getTaskMonitor();

        // Search for the return object
        List<Parameter> params = job.getTaskParams().getParameters();
        for (int i = 0; i < params.size(); ++i) {
            Parameter p = params.get(i);
            if (p instanceof DependencyParameter) {
                // Check parameter direction
                DataInstanceId dId = null;
                DependencyParameter dp = (DependencyParameter) p;
                switch (p.getDirection()) {
                    case IN:
                    case IN_DELETE:
                    case CONCURRENT:
                    case COMMUTATIVE:
                    case INOUT:
                        // Return value is OUT, skip the current parameter
                        continue;
                    case OUT:
                        dId = ((WAccessId) dp.getDataAccessId()).getWrittenDataInstance();
                        break;
                }
                Object value = null;
                JsonObject retValue = (JsonObject) job.getReturnValue();
                if (dp.getType().equals(DataType.FILE_T)) {
                    value = retValue.get(p.getName()).toString();
                    try {
                        FileWriter file = new FileWriter(dp.getDataTarget());
                        file.write("0004");
                        file.write(value.toString());
                        file.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    // it's a Java HTTP task, can have only single value of a primitive type
                    Gson gson = new Gson();
                    JsonPrimitive primValue = retValue.getAsJsonPrimitive("$return_0");
                    switch (dp.getType()) {
                        case INT_T:
                            value = gson.fromJson(primValue, int.class);
                            break;
                        case LONG_T:
                            value = gson.fromJson(primValue, long.class);
                            break;
                        case STRING_T:
                            value = gson.fromJson(primValue, String.class);
                            break;
                        case OBJECT_T:
                            if (dp.getContentType().equals("int")) {
                                value = gson.fromJson(primValue, int.class);
                            } else if (dp.getContentType().equals("long")) {
                                value = gson.fromJson(primValue, long.class);
                            } else if (dp.getContentType().equals("String")) {
                                value = gson.fromJson(primValue, String.class);
                            } else {
                                // todo: Strings fall here too.. why??
                                value = gson.fromJson(primValue, Object.class);
                            }
                            break;
                        default:
                            value = null;
                            break;
                    }
                }
                Worker<? extends WorkerResourceDescription> w = getAssignedResource().getResource();
                String dataName = getOuputRename(p);
                storeOutputParameter(job, w, dataName, dp);
                monitor.valueGenerated(i, p.getName(), p.getType(), dataName, dp.getDataTarget());

                // Parameter found, store it
                // todo: fix this
                String name = dId.getRenaming();
                if (value == null) {
                    LOGGER.info("___________________________value null");
                    value = job.getReturnValue();
                }
                LogicalData ld = Comm.registerValue(name, value);

                // Monitor one of its locations
                Set<DataLocation> locations = ld.getLocations();
                if (!locations.isEmpty()) {
                    for (DataLocation loc : ld.getLocations()) {
                        if (loc != null) {
                            monitor.valueGenerated(i, p.getName(), p.getType(), name, loc);
                        }
                    }
                }

            }
        }
    }

    /**
     * Notification announcing that all input data for a specific job is available on the node.
     *
     * @param job job whose input data is in the worker
     */
    public void allInputDataOnWorker(Job<?> job) {
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onDataReception();
    }

    /*
     * ***************************************************************************************************************
     * EXECUTION TRIGGERS
     * ***************************************************************************************************************
     */
    @Override
    protected void doCompleted() {
        // Profile the resource
        this.getAssignedResource().profiledExecution(this.getAssignedImplementation(), profile);

        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onSuccesfulExecution();

        for (CommutativeGroupTask com : this.getTask().getCommutativeGroupList()) {
            com.taskEndedExecution();
        }

        // Decrease the execution counter and set the task as finished and notify the producer
        this.task.decreaseExecutionCount();
        this.task.setStatus(TaskState.FINISHED);
        this.ap.notifyTaskEnd(this.task);
    }

    @Override
    protected void doError() throws FailedActionException {
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onErrorExecution();

        if (this.task.getOnFailure() == OnFailure.RETRY) {
            if (this.getExecutingResources().size() >= SCHEDULING_CHANCES) {
                LOGGER.warn("Task " + this.task.getId() + " has already been rescheduled; notifying task failure.");
                ErrorManager
                    .warn("Task " + this.task.getId() + " has already been rescheduled; notifying task failure.");
                throw new FailedActionException();
            } else {
                ErrorManager
                    .warn("Task " + this.task.getId() + " execution on worker " + this.getAssignedResource().getName()
                        + " has failed; rescheduling task execution. (changing worker)");
                LOGGER.warn("Task " + this.task.getId() + " execution on worker " + this.getAssignedResource().getName()
                    + " has failed; rescheduling task execution. (changing worker)");
            }
        } else {
            LOGGER.warn("Notifying task " + this.task.getId() + " failure");
            ErrorManager.warn("Notifying task " + this.task.getId() + " failure");
            throw new FailedActionException();
        }
    }

    @Override
    protected void doAbort() {
        ResourceScheduler target = this.getAssignedResource();
        if (target != null) {
            this.getExecutingResources().remove(target);
        }
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onAbortedExecution();
    }

    @Override
    protected void doFailed() {
        // Failed log message
        String taskName = this.task.getTaskDescription().getName();
        StringBuilder sb = new StringBuilder();
        sb.append("Task '").append(taskName).append("' TOTALLY FAILED.\n");
        sb.append("Possible causes:\n");
        sb.append("     -Exception thrown by task '").append(taskName).append("'.\n");
        sb.append("     -Expected output files not generated by task '").append(taskName).append("'.\n");
        sb.append("     -Could not provide nor retrieve needed data between master and worker.\n");
        sb.append("\n");
        sb.append("Check files '").append(Comm.getAppHost().getJobsDirPath()).append("job[");
        Iterator<Integer> j = this.jobs.iterator();
        while (j.hasNext()) {
            sb.append(j.next());
            if (!j.hasNext()) {
                break;
            }
            sb.append("|");
        }
        sb.append("'] to find out the error.\n");
        sb.append(" \n");

        ErrorManager.warn(sb.toString());
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onFailedExecution();

        // Notify task failure
        this.task.decreaseExecutionCount();
        this.task.setStatus(TaskState.FAILED);
        this.ap.notifyTaskEnd(this.task);
    }

    @Override
    protected void doException(COMPSsException e) {
        LinkedList<TaskGroup> taskGroups = this.task.getTaskGroupList();
        for (TaskGroup group : taskGroups) {
            if (!group.getName().equals("App" + this.task.getApplication().getId())) {
                group.setException((COMPSsException) e);
                for (Task t : group.getTasks()) {
                    if (t.getId() != this.getTask().getId()) {
                        for (AllocatableAction aa : t.getExecutions()) {
                            if (aa != null && aa.isPending()) {
                                LOGGER.debug(" Adding Task " + t.getId() + " to members group of task "
                                    + this.getTask().getId() + "(Group " + group.getName() + ")");
                                addGroupMember(aa);
                            }
                        }
                    }
                }
            }
        }

        // Failed log message
        String taskName = this.task.getTaskDescription().getName();
        StringBuilder sb = new StringBuilder();
        sb.append("COMPSs Exception raised : Task " + this.task.getId() + " (").append(taskName)
            .append(") has raised an exception with message ").append(e.getMessage())
            .append(". Members of the containing groups will be cancelled.\n");
        sb.append("\n");
        ErrorManager.warn(sb.toString());

        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onException();

        // Decrease the execution counter and set the task as finished and notify the producer
        this.task.decreaseExecutionCount();
        this.task.setStatus(TaskState.FINISHED);
        this.ap.notifyTaskEnd(this.task);
    }

    @Override
    protected void doCanceled() {
        // Cancelled log message
        String taskName = this.task.getTaskDescription().getName();
        ErrorManager.warn("Task " + this.task.getId() + "(Action: " + this.getId() + ") with name " + taskName
            + " has been cancelled.");

        // Notify task cancellation
        this.task.decreaseExecutionCount();
        this.task.setStatus(TaskState.CANCELED);
        this.ap.notifyTaskEnd(this.task);
    }

    @Override
    protected void doFailIgnored() {
        // Failed log message
        String taskName = this.task.getTaskDescription().getName();
        StringBuilder sb = new StringBuilder();
        sb.append("Task failure: Task " + this.task.getId() + " (").append(taskName)
            .append(") has failed. Successors keep running.\n");
        sb.append("\n");
        ErrorManager.warn(sb.toString());

        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onFailedExecution();

        // Notify task completion despite the failure
        this.task.decreaseExecutionCount();
        this.task.setStatus(TaskState.FINISHED);
        this.ap.notifyTaskEnd(this.task);
    }

    /*
     * ***************************************************************************************************************
     * SCHEDULING MANAGEMENT
     * ***************************************************************************************************************
     */
    @Override
    public final List<ResourceScheduler<? extends WorkerResourceDescription>> getCompatibleWorkers() {
        return getCoreElementExecutors(this.task.getTaskDescription().getCoreElement().getCoreId());
    }

    @Override
    public final Implementation[] getImplementations() {
        CoreElement ce = this.task.getTaskDescription().getCoreElement();
        List<Implementation> coreImpls = ce.getImplementations();

        int coreImplsSize = coreImpls.size();
        Implementation[] impls = (Implementation[]) new Implementation[coreImplsSize];
        for (int i = 0; i < coreImplsSize; ++i) {
            impls[i] = (Implementation) coreImpls.get(i);
        }
        return impls;
    }

    @Override
    public <W extends WorkerResourceDescription> boolean isCompatible(Worker<W> r) {
        return r.canRun(this.task.getTaskDescription().getCoreElement().getCoreId());
    }

    @Override
    public final <T extends WorkerResourceDescription> List<Implementation>
        getCompatibleImplementations(ResourceScheduler<T> r) {
        return r.getExecutableImpls(this.task.getTaskDescription().getCoreElement().getCoreId());
    }

    @Override
    public final Integer getCoreId() {
        return this.task.getTaskDescription().getCoreElement().getCoreId();
    }

    @Override
    public final int getPriority() {
        return this.task.getTaskDescription().hasPriority() ? 1 : 0;
    }

    @Override
    public long getGroupPriority() {
        return ACTION_SINGLE;
    }

    @Override
    public OnFailure getOnFailure() {
        return this.task.getOnFailure();
    }

    @Override
    public final <T extends WorkerResourceDescription> Score schedulingScore(ResourceScheduler<T> targetWorker,
        Score actionScore) {
        return targetWorker.generateResourceScore(this, this.task.getTaskDescription(), actionScore);
    }

    @SuppressWarnings("unchecked")
    @Override
    public final List<ResourceScheduler<?>> tryToSchedule(Score actionScore,
        Set<ResourceScheduler<?>> availableResources) throws BlockedActionException, UnassignedActionException {
        // COMPUTE RESOURCE CANDIDATES
        List<ResourceScheduler<? extends WorkerResourceDescription>> candidates = new LinkedList<>();
        List<ResourceScheduler<? extends WorkerResourceDescription>> uselessWorkers = new LinkedList<>();

        if (this.isTargetResourceEnforced()) {
            // The scheduling is forced to a given resource
            candidates.add((ResourceScheduler<WorkerResourceDescription>) this.getEnforcedTargetResource());
        } else {
            if (this.isSchedulingConstrained()) {
                // The scheduling is constrained by dependencies
                for (AllocatableAction a : this.getConstrainingPredecessors()) {
                    candidates.add((ResourceScheduler<WorkerResourceDescription>) a.getAssignedResource());
                }
            } else {
                // Free scheduling
                List<ResourceScheduler<? extends WorkerResourceDescription>> compatibleCandidates =
                    getCompatibleWorkers();
                if (compatibleCandidates.size() == 0) {
                    throw new BlockedActionException();
                }
                for (ResourceScheduler<? extends WorkerResourceDescription> currentWorker : availableResources) {
                    if (currentWorker.getResource().canRunSomething()) {
                        if (compatibleCandidates.contains(currentWorker)) {
                            candidates.add(currentWorker);
                        }
                    } else {
                        uselessWorkers.add(currentWorker);
                    }
                }
                if (candidates.size() == 0) {
                    throw new UnassignedActionException();
                }
            }
        }

        Collections.shuffle(candidates);
        schedule(actionScore, candidates);

        return uselessWorkers;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        // COMPUTE RESOURCE CANDIDATES
        List<ResourceScheduler<? extends WorkerResourceDescription>> candidates = new LinkedList<>();
        if (this.isTargetResourceEnforced()) {
            // The scheduling is forced to a given resource
            candidates.add((ResourceScheduler<WorkerResourceDescription>) this.getEnforcedTargetResource());
        } else {
            if (isSchedulingConstrained()) {
                // The scheduling is constrained by dependencies
                for (AllocatableAction a : this.getConstrainingPredecessors()) {
                    candidates.add((ResourceScheduler<WorkerResourceDescription>) a.getAssignedResource());
                }
            } else {
                // Free scheduling
                candidates = getCompatibleWorkers();
            }
        }
        this.schedule(actionScore, candidates);

    }

    private <T extends WorkerResourceDescription> void schedule(Score actionScore,
        List<ResourceScheduler<? extends WorkerResourceDescription>> candidates)
        throws BlockedActionException, UnassignedActionException {
        // COMPUTE BEST WORKER AND IMPLEMENTATION
        StringBuilder debugString = new StringBuilder("Scheduling " + this + " execution:\n");
        ResourceScheduler<? extends WorkerResourceDescription> bestWorker = null;
        Implementation bestImpl = null;
        Score bestScore = null;
        int usefulResources = 0;
        for (ResourceScheduler<? extends WorkerResourceDescription> worker : candidates) {
            if (this.getExecutingResources().contains(worker)) {
                if (DEBUG) {
                    LOGGER.debug("Task " + this.task.getId() + " already ran on worker " + worker.getName());
                }
                if (candidates.size() > 1) {
                    continue;
                } else {
                    LOGGER.debug("No more candidate resources for task " + this.task.getId() + ". Trying to use worker "
                        + worker.getName() + " again ... ");
                    extraResubmit = true;
                }
            }

            Score resourceScore = worker.generateResourceScore(this, this.task.getTaskDescription(), actionScore);
            ++usefulResources;
            if (resourceScore != null) {
                for (Implementation impl : getCompatibleImplementations(worker)) {
                    Score implScore =
                        worker.generateImplementationScore(this, this.task.getTaskDescription(), impl, resourceScore);
                    if (DEBUG) {
                        debugString.append("[Task ").append(this.task.getId()).append("] Resource ")
                            .append(worker.getName()).append(" ").append(" Implementation ")
                            .append(impl.getImplementationId()).append(" ").append(" Score ").append(implScore)
                            .append("\n");
                    }
                    if (Score.isBetter(implScore, bestScore)) {
                        bestWorker = worker;
                        bestImpl = impl;
                        bestScore = implScore;
                    }
                }
            }
        }

        // CHECK SCHEDULING RESULT
        if (DEBUG) {
            LOGGER.debug(debugString.toString());
        }

        if (bestWorker == null) {
            if (usefulResources == 0) {
                LOGGER.warn("No worker can run " + this);
                throw new BlockedActionException();
            } else {
                throw new UnassignedActionException();
            }
        }
        schedule(bestWorker, bestImpl);

    }

    @Override
    public final <T extends WorkerResourceDescription> void schedule(ResourceScheduler<T> targetWorker,
        Score actionScore) throws BlockedActionException, UnassignedActionException {
        if (targetWorker == null
            // Resource is not compatible with the Core
            || !targetWorker.getResource().canRun(this.task.getTaskDescription().getCoreElement().getCoreId())
            // already ran on the resource
            || this.getExecutingResources().contains(targetWorker)) {

            String message = "Worker " + (targetWorker == null ? "null" : targetWorker.getName())
                + " has not available resources to run " + this;
            LOGGER.warn(message);
            throw new UnassignedActionException();
        }

        Implementation bestImpl = null;
        Score bestScore = null;
        Score resourceScore = targetWorker.generateResourceScore(this, this.task.getTaskDescription(), actionScore);
        for (Implementation impl : getCompatibleImplementations(targetWorker)) {
            Score implScore =
                targetWorker.generateImplementationScore(this, this.task.getTaskDescription(), impl, resourceScore);
            if (Score.isBetter(implScore, bestScore)) {
                bestImpl = impl;
                bestScore = implScore;
            }
        }
        schedule(targetWorker, bestImpl);
    }

    @Override
    public final <T extends WorkerResourceDescription> void schedule(ResourceScheduler<T> targetWorker,
        Implementation impl) throws BlockedActionException, UnassignedActionException {
        if (targetWorker == null || impl == null) {
            this.assignResource(null); // to remove previous allocation when re-scheduling the action
            LOGGER.debug(" Action doesn't have targetWorker or implementation: "
                + "Throwing UnassignedActionException. action: " + this);
            throw new UnassignedActionException();
        }

        if (DEBUG) {
            LOGGER.debug("Scheduling " + this + " on worker " + (targetWorker == null ? "null" : targetWorker.getName())
                + " with implementation " + (impl == null ? "null" : impl.getImplementationId()));
        }

        if (!targetWorker.getResource().canRun(impl)) { // Resource is not compatible with the implementation
            LOGGER.warn("Worker " + targetWorker.getName() + " has not available resources to run " + this);
            throw new UnassignedActionException();
        }
        if (this.getExecutingResources().contains(targetWorker) && !extraResubmit) {
            LOGGER.warn("Task " + this.getTask().getId() + " has been scheduled to a host already used before."
                + targetWorker.getName());
            throw new UnassignedActionException();
        } else {
            extraResubmit = false;
        }

        LOGGER.info("Assigning action " + this + " to worker " + targetWorker.getName() + " with implementation "
            + impl.getImplementationId());

        this.assignImplementation(impl);
        assignResource(targetWorker);
        targetWorker.scheduleAction(this);

        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onSchedule();
    }

    @Override
    protected void treatDependencyFreeAction(List<AllocatableAction> freeTasks) {
        for (CommutativeGroupTask cgt : this.getTask().getCommutativeGroupList()) {
            for (Task t : cgt.getCommutativeTasks()) {
                if (t.getStatus() == TaskState.TO_EXECUTE) {
                    for (AllocatableAction aa : t.getExecutions()) {
                        if (!aa.hasDataPredecessors()) {
                            if (!freeTasks.contains(aa)) {
                                freeTasks.add(aa);
                            }
                        }
                    }
                }
            }
        }
    }

    /*
     * ***************************************************************************************************************
     * OTHER
     * ***************************************************************************************************************
     */
    @Override
    public String toString() {
        return "ExecutionAction (Task " + this.task.getId() + ", CE name " + this.task.getTaskDescription().getName()
            + ")";
    }

}
