/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.api.TaskMonitor.CollectionTaskResult;
import es.bsc.compss.api.TaskMonitor.TaskResult;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.ActionGroup.MutexGroup;
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
import es.bsc.compss.types.job.JobListener;
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
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ExecutionAction extends AllocatableAction implements JobListener {

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
            // Register mutex condition
            registerMutex();
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

    private void registerMutex() {
        for (CommutativeGroupTask group : this.task.getCommutativeGroupList()) {
            MutexGroup mGroup = group.getActions();
            this.addToMutexGroup(mGroup);
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
                treatCommutativePredecessor((CommutativeGroupTask) predecessor);
            }
        }
    }

    private void treatCommutativePredecessor(CommutativeGroupTask predecessor) {
        if (DEBUG) {
            LOGGER.debug("Task has a commutative group as a predecessor");
        }
        for (Task t : predecessor.getCommutativeTasks()) {
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
            if (p.isPotentialDependency()) {
                DependencyParameter dp = (DependencyParameter) p;
                switch (taskDescription.getType()) {
                    case HTTP:
                    case METHOD:
                        transferJobData(dp, listener);
                        break;
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
                    if (p.isPotentialDependency()) {
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
                    if (k.isPotentialDependency()) {
                        DependencyParameter dpKey = (DependencyParameter) k;
                        transferJobData(dpKey, listener);
                    }
                    Parameter v = entry.getValue();
                    if (v.isPotentialDependency()) {
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

                LogicalData srcData = ((RAccessId) access).getReadDataInstance().getData();
                w.getData(srcData, param, listener);
            } else {
                // ReadWrite Access, transfer object
                listener.addOperation();
                LogicalData srcData = ((RWAccessId) access).getReadDataInstance().getData();
                String tgtName = ((RWAccessId) access).getWrittenDataInstance().getRenaming();
                LogicalData tmpData = Comm.registerData("tmp" + tgtName);
                w.getData(srcData, tgtName, tmpData, param, listener);
            }
        }
    }

    private void transferStreamParameter(DependencyParameter param, JobTransfersListener listener) {
        DataAccessId access = param.getDataAccessId();
        LogicalData source;
        LogicalData target;
        if (access instanceof WAccessId) {
            WAccessId wAccess = (WAccessId) access;
            source = wAccess.getWrittenDataInstance().getData();
            target = source;
        } else {
            if (access instanceof RAccessId) {
                RAccessId rAccess = (RAccessId) access;
                source = rAccess.getReadDataInstance().getData();
                target = source;
            } else {
                RWAccessId rwAccess = (RWAccessId) access;
                source = rwAccess.getReadDataInstance().getData();
                target = rwAccess.getWrittenDataInstance().getData();
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
        Job<?> job = submitJob(transferGroupId);
        if (!this.cancelledBeforeSubmit) {
            // Register job
            this.jobs.add(job.getJobId());
            JOB_LOGGER.info((getExecutingResources().size() > 1 ? "Rescheduled" : "New") + " Job " + job.getJobId()
                + " (Task: " + this.task.getId() + ")");
            JOB_LOGGER.info("  * Method name: " + this.task.getTaskDescription().getName());
            JOB_LOGGER.info("  * Target host: " + this.getAssignedResource().getName());

            this.profile.setSubmissionTime(System.currentTimeMillis());
            JobDispatcher.dispatch(job);
            JOB_LOGGER.info("Submitted Task: " + this.task.getId() + " Job: " + job.getJobId() + " Method: "
                + this.task.getTaskDescription().getName() + " Resource: " + this.getAssignedResource().getName());
        } else {
            JOB_LOGGER.info("Job" + job.getJobId() + " cancelled before submission.");
        }
    }

    @Override
    public void arrived(Job<?> job) {
        arrivedAt(job, System.currentTimeMillis());
    }

    @Override
    public void arrivedAt(Job<?> job, long ts) {
        this.profile.setArrivalTime(ts);
    }

    private void removeJobTempData() {
        TaskDescription taskDescription = this.task.getTaskDescription();
        for (Parameter p : taskDescription.getParameters()) {
            if (DEBUG) {
                JOB_LOGGER.debug("    * " + p);
            }
            if (p.isPotentialDependency()) {
                DependencyParameter dp = (DependencyParameter) p;
                switch (taskDescription.getType()) {
                    case HTTP:
                    case METHOD:
                        removeTmpData(dp);
                        break;
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
                    if (p.isPotentialDependency()) {
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
                    if (k.isPotentialDependency()) {
                        DependencyParameter dpKey = (DependencyParameter) k;
                        removeTmpData(dpKey);
                    }
                    Parameter v = entry.getValue();
                    if (v.isPotentialDependency()) {
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

    protected Job<?> submitJob(int transferGroupId) {
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
            slaveNames, this, predecessors, this.task.getSuccessors().size());
        // Remove predecessors from map for task dependency tracing
        if (Tracer.isActivated() && Tracer.isTracingTaskDependencies()) {
            Tracer.removePredecessor(this.task.getId());
        }
        this.currentJob = job;
        job.setTransferGroupId(transferGroupId);
        job.setHistory(JobHistory.NEW);

        return job;
    }

    @Override
    public void allInputDataOnWorker(Job<?> job) {
        allInputDataOnWorkerAt(job, System.currentTimeMillis());
    }

    @Override
    public void allInputDataOnWorkerAt(Job<?> job, long ts) {
        profile.setDataFetchingTime(ts);
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onDataReception();

    }

    @Override
    public void startingExecution(Job<?> job) {
        startingExecutionAt(job, System.currentTimeMillis());
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onExecutionStart();
    }

    @Override
    public void startingExecutionAt(Job<?> job, long ts) {
        profile.setExecutionStartTime(ts);
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onExecutionStartAt(ts);
    }

    @Override
    public void endedExecution(Job<?> job) {
        endedExecutionAt(job, System.currentTimeMillis());
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onExecutionEnd();
    }

    @Override
    public void endedExecutionAt(Job<?> job, long ts) {
        profile.setExecutionEndTime(ts);
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onExecutionEndAt(ts);
    }

    @Override
    public void endNotified(Job<?> job) {
        endNotifiedAt(job, System.currentTimeMillis());
    }

    @Override
    public void endNotifiedAt(Job<?> job, long ts) {
        profile.setEndNotificationTime(ts);
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
     * @param e COMPSsException raised by the job
     */
    @Override
    public final void jobException(Job<?> job, COMPSsException e) {
        this.profile.end(System.currentTimeMillis());
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
     * @param status Failure status
     */
    @Override
    public final void jobFailed(Job<?> job, JobEndStatus status) {
        this.profile.end(System.currentTimeMillis());

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
                    this.profile.setSubmissionTime(System.currentTimeMillis());
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
    @Override
    public final void jobCompleted(Job<?> job) {
        // End profile
        this.profile.end(System.currentTimeMillis());

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
        commitCommutativeAccesses(job);
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

    private void commitCommutativeAccesses(Job<?> job) {
        for (Parameter p : this.task.getParameters()) {
            if (p.isPotentialDependency()) {
                commitCommutativeAccesses((DependencyParameter) p);
            }
        }
    }

    private void commitCommutativeAccesses(DependencyParameter dp) {
        switch (dp.getType()) {
            case COLLECTION_T:
                CollectionParameter cp = (CollectionParameter) dp;
                for (Parameter elem : cp.getParameters()) {
                    if (elem.isPotentialDependency()) {
                        commitCommutativeAccesses((DependencyParameter) elem);
                    }
                }
                break;
            case DICT_COLLECTION_T:
                DictCollectionParameter dcp = (DictCollectionParameter) dp;
                for (Map.Entry<Parameter, Parameter> entry : dcp.getParameters().entrySet()) {
                    Parameter k = entry.getKey();
                    if (k.isPotentialDependency()) {
                        commitCommutativeAccesses((DependencyParameter) k);
                    }
                    Parameter v = entry.getValue();
                    if (v.isPotentialDependency()) {
                        commitCommutativeAccesses((DependencyParameter) v);
                    }
                }
                break;
            default:
                if (dp.getDirection() == Direction.COMMUTATIVE) {
                    DataAccessId placeHolder = dp.getDataAccessId();
                    CommutativeGroupTask cgt = this.getTask().getCommutativeGroup(placeHolder.getDataId());
                    DataAccessId performedAccess = cgt.nextAccess();
                    dp.setDataAccessId(performedAccess);
                }
        }

    }

    private void doMethodOutputTransfers(Job<?> job) {
        Worker<? extends WorkerResourceDescription> w = getAssignedResource().getResource();
        TaskMonitor monitor = this.task.getTaskMonitor();
        List<Parameter> params = job.getTaskParams().getParameters();
        for (int i = 0; i < params.size(); ++i) {
            Parameter p = params.get(i);
            String dataName = getOuputRename(p);
            if (dataName != null) {
                DependencyParameter dp = (DependencyParameter) p;
                storeOutputParameter(job, w, dataName, dp);
                TaskResult mp = buildMonitorParameter(p, dataName);
                monitor.valueGenerated(i, mp);
            }
        }
    }

    private TaskResult buildMonitorParameter(Parameter p, String dataName) {
        TaskResult result;
        String dataLocation = ((DependencyParameter) p).getDataTarget();
        if (p.getType() == DataType.COLLECTION_T) {
            List<Parameter> subParams = ((CollectionParameter) p).getParameters();
            TaskResult[] subResults = new TaskResult[subParams.size()];
            for (int i = 0; i < subParams.size(); i++) {
                subResults[i] = buildMonitorParameter(subParams.get(i), getOuputRename(subParams.get(i)));
            }
            result = new CollectionTaskResult(p.getType(), dataName, dataLocation, subResults);
        } else {
            result = new TaskResult(p.getType(), dataName, dataLocation);
        }

        return result;
    }

    private String getOuputRename(Parameter p) {
        String name = null;
        if (p.isPotentialDependency()) {
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

    private DataLocation storeOutputParameter(Job<?> job, Worker<? extends WorkerResourceDescription> w,
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

    private DataLocation storeHttpOutputParameter(String dataName, DependencyParameter p) {

        DataLocation outLoc = null;
        try {
            String dataTarget = p.getDataTarget();
            if (DEBUG) {
                JOB_LOGGER.debug("Proposed URI for storing HTTP output param: " + dataTarget);
            }
            SimpleURI resultURI = new SimpleURI(dataTarget);
            SimpleURI targetURI = new SimpleURI(resultURI.getSchema() + resultURI.getPath());
            outLoc = DataLocation.createLocation(Comm.getAppHost(), targetURI);
            // Data target has been stored as URI but final target data should be just the path
            p.setDataTarget(outLoc.getPath());
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + p.getDataTarget(), e);
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
            if (p.isPotentialDependency()) {
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
                    TaskResult mp = buildMonitorParameter(p, getOuputRename(p));
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
        Worker<? extends WorkerResourceDescription> w = getAssignedResource().getResource();

        List<Parameter> params = job.getTaskParams().getParameters();
        for (int i = 0; i < params.size(); ++i) {
            Parameter p = params.get(i);
            if (!(p.isPotentialDependency())) {
                continue;
            }

            DependencyParameter dp = (DependencyParameter) p;
            DataInstanceId dId = null;
            if (p.getDirection() == Direction.INOUT) {
                dId = ((RWAccessId) dp.getDataAccessId()).getWrittenDataInstance();
            } else {
                if (p.getDirection() == Direction.OUT) {
                    dId = ((WAccessId) dp.getDataAccessId()).getWrittenDataInstance();
                } else {
                    continue;
                }
            }

            String dataName = getOuputRename(p);
            DataLocation dl = storeHttpOutputParameter(dataName, dp);

            // Parameter found, store it
            // todo: fix this
            String name = dId.getRenaming();
            Object value;
            LogicalData ld;
            JsonObject retValue = (JsonObject) job.getReturnValue();
            if (dp.getType().equals(DataType.FILE_T)) {
                value = retValue.get(p.getName()).toString();
                try {
                    FileWriter file = new FileWriter(dp.getDataTarget());
                    // 0004 is the JSON package ID in Python binding
                    file.write("0004");
                    file.write(value.toString());
                    file.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                ld = Comm.registerLocation(name, dl);
                TaskResult mp = buildMonitorParameter(p, dataName);
                monitor.valueGenerated(i, mp);
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
                    case STRING_64_T:
                        String temp = gson.fromJson(primValue, String.class);
                        byte[] encoded = Base64.getEncoder().encode(temp.getBytes());
                        value = new String(encoded);
                        break;
                    case OBJECT_T:
                        if (dp.getContentType().equals("int")) {
                            value = gson.fromJson(primValue, int.class);
                        } else {
                            if (dp.getContentType().equals("long")) {
                                value = gson.fromJson(primValue, long.class);
                            } else {
                                if (dp.getContentType().equals("String")) {
                                    value = gson.fromJson(primValue, String.class);
                                } else {
                                    // todo: Strings fall here too.. why??
                                    value = gson.fromJson(primValue, Object.class);
                                }
                            }
                        }
                        break;
                    default:
                        value = null;
                        break;
                }
                ld = Comm.registerValue(name, value);
            }

            // Monitor one of its locations
            Set<DataLocation> locations = ld.getLocations();
            if (!locations.isEmpty()) {
                TaskResult mp = buildMonitorParameter(p, getOuputRename(p));
                for (DataLocation loc : ld.getLocations()) {
                    if (loc != null) {
                        monitor.valueGenerated(i, mp);
                    }
                }
            }
        }
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
    protected Collection<AllocatableAction> doException(COMPSsException e) {
        LinkedList<TaskGroup> taskGroups = this.task.getTaskGroupList();
        Collection<AllocatableAction> otherActionsFromGroups = new LinkedList<>();
        for (TaskGroup group : taskGroups) {
            if (!group.getName().equals("App" + this.task.getApplication().getId())) {
                group.setException((COMPSsException) e);
                group.addToCollectionExecutionForTasksOtherThan(otherActionsFromGroups, this.getTask());
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
        monitor.onException(e);

        // Decrease the execution counter and set the task as finished and notify the producer
        this.task.decreaseExecutionCount();
        this.task.setStatus(TaskState.FINISHED);
        this.ap.notifyTaskEnd(this.task);
        return otherActionsFromGroups;
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
    public final void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        // COMPUTE RESOURCE CANDIDATES
        List<ResourceScheduler<? extends WorkerResourceDescription>> candidates = new LinkedList<>();
        List<ResourceScheduler<? extends WorkerResourceDescription>> compatibleWorkers = this.getCompatibleWorkers();
        if (this.isTargetResourceEnforced()) {
            // The scheduling is forced to a given resource
            ResourceScheduler<? extends WorkerResourceDescription> target = this.getEnforcedTargetResource();
            if (compatibleWorkers.contains(target)) {
                candidates.add(target);
            } else {
                throw new UnassignedActionException();
            }
        } else if (isSchedulingConstrained()) {
            // The scheduling is constrained by dependencies
            for (AllocatableAction a : this.getConstrainingPredecessors()) {
                ResourceScheduler<? extends WorkerResourceDescription> target = a.getAssignedResource();
                if (compatibleWorkers.contains(target)) {
                    candidates.add(target);
                }
            }
        } else {
            // Free scheduling
            candidates = compatibleWorkers;
        }

        if (candidates.isEmpty()) {
            throw new BlockedActionException();
        }

        List prevExecutors = this.getExecutingResources();
        if (candidates.size() > prevExecutors.size() && prevExecutors.size() > 0) {
            candidates.removeAll(prevExecutors);
        }
        this.scheduleSecuredCandidates(actionScore, candidates);
    }

    @Override
    public void schedule(Collection<ResourceScheduler<? extends WorkerResourceDescription>> candidates,
        Score actionScore) throws UnassignedActionException {
        List<ResourceScheduler<? extends WorkerResourceDescription>> compatibleWorkers = this.getCompatibleWorkers();
        List<ResourceScheduler<? extends WorkerResourceDescription>> verifiedCandidates = new LinkedList<>();

        if (this.isTargetResourceEnforced()) {
            // The scheduling is forced to a given resource
            ResourceScheduler<? extends WorkerResourceDescription> target = this.getEnforcedTargetResource();
            if (candidates.contains(target) && compatibleWorkers.contains(target)) {
                verifiedCandidates.add(target);
            } else {
                throw new UnassignedActionException();
            }
        } else if (this.isSchedulingConstrained()) {
            // The scheduling is constrained by dependencies
            for (AllocatableAction a : this.getConstrainingPredecessors()) {
                ResourceScheduler<? extends WorkerResourceDescription> target = a.getAssignedResource();
                if (candidates.contains(target) && compatibleWorkers.contains(target)) {
                    verifiedCandidates.add(target);
                }
            }
            if (verifiedCandidates.isEmpty()) {
                throw new UnassignedActionException();
            }

        } else {
            for (ResourceScheduler<? extends WorkerResourceDescription> candidate : candidates) {
                if (compatibleWorkers.contains(candidate) && compatibleWorkers.contains(candidate)) {
                    verifiedCandidates.add(candidate);
                }
            }
            if (verifiedCandidates.isEmpty()) {
                throw new UnassignedActionException();
            }
        }
        scheduleSecuredCandidates(actionScore, verifiedCandidates);
    }

    @Override
    public final void schedule(ResourceScheduler<? extends WorkerResourceDescription> targetWorker, Score actionScore)
        throws UnassignedActionException {
        if (targetWorker == null || !validateWorker(targetWorker)) {
            throw new UnassignedActionException();
        }

        Implementation bestImpl = null;
        Score bestScore = null;
        Score resourceScore = targetWorker.generateResourceScore(this, this.task.getTaskDescription(), actionScore);
        if (resourceScore != null) {
            for (Implementation impl : getCompatibleImplementations(targetWorker)) {
                Score implScore =
                    targetWorker.generateImplementationScore(this, this.task.getTaskDescription(), impl, resourceScore);
                if (Score.isBetter(implScore, bestScore)) {
                    bestImpl = impl;
                    bestScore = implScore;
                }
            }
        }
        if (bestImpl == null) {
            throw new UnassignedActionException();
        }
        assignWorkerAndImpl(targetWorker, bestImpl);
    }

    @Override
    public final void schedule(ResourceScheduler<? extends WorkerResourceDescription> targetWorker, Implementation impl)
        throws UnassignedActionException {

        if (targetWorker == null || impl == null) {
            this.assignResource(null); // to remove previous allocation when re-scheduling the action
            throw new UnassignedActionException();
        }

        if (DEBUG) {
            LOGGER.debug("Scheduling " + this + " on worker " + targetWorker.getName() + " with implementation "
                + impl.getImplementationId());
        }

        if (!validateWorker(targetWorker)) {
            throw new UnassignedActionException();
        }

        if (!targetWorker.getResource().canRun(impl)) {
            LOGGER.warn("Worker " + targetWorker.getName() + " is not compatible with " + impl);
            throw new UnassignedActionException();
        }

        assignWorkerAndImpl(targetWorker, impl);
    }

    /**
     * Verifies that the passed-in worker is able to run the task given the task execution history.
     * 
     * @param targetCandidate Resource whose aptitude to run the task has to be evaluated
     * @return {@literal true}, if the worker passed in is compatible with the action; {@literal false}, otherwise.
     */
    private boolean validateWorker(ResourceScheduler targetCandidate) {
        if (this.isTargetResourceEnforced()) {
            // The scheduling is forced to a given resource
            ResourceScheduler<? extends WorkerResourceDescription> enforcedTarget = this.getEnforcedTargetResource();
            if (enforcedTarget != targetCandidate) {
                LOGGER.warn("Task " + this.getTask().getId() + " is enforced to run on " + enforcedTarget.getName());
                return false;
            }
        } else if (this.isSchedulingConstrained()) {
            boolean isPredecessor = false;
            // The scheduling is constrained by dependencies
            for (AllocatableAction a : this.getConstrainingPredecessors()) {
                ResourceScheduler<? extends WorkerResourceDescription> predecessorHost = a.getAssignedResource();
                if (targetCandidate == predecessorHost) {
                    isPredecessor = true;
                }
            }
            if (!isPredecessor) {
                LOGGER.warn(targetCandidate.getName() + " did not host the execution of any constraining predecessor");
                return false;
            }
        }

        if (this.getExecutingResources().contains(targetCandidate) && this.getCompatibleWorkers().size() > 1) {
            LOGGER.warn("Task " + this.getTask().getId() + " was already scheduled on " + targetCandidate.getName());
            return false;
        }
        return true;
    }

    /**
     * Selects the best resource-Implementation pair given for an action given a score and a list of workers that could
     * potentially host the execution of the action. All the workers within the list have been already checked to be
     * able to host the action execution.
     * 
     * @param actionScore Score given by the scheduler to the action
     * @param candidates list of compatible workers that could host the action execution
     * @throws UnassignedActionException the action could not be assigned to any of the given candidate resources
     */
    private void scheduleSecuredCandidates(Score actionScore,
        List<ResourceScheduler<? extends WorkerResourceDescription>> candidates) throws UnassignedActionException {
        // COMPUTE BEST WORKER AND IMPLEMENTATION
        StringBuilder debugString = new StringBuilder("Scheduling " + this + " execution:\n");
        ResourceScheduler<? extends WorkerResourceDescription> bestWorker = null;
        Implementation bestImpl = null;
        Score bestScore = null;
        for (ResourceScheduler<? extends WorkerResourceDescription> worker : candidates) {
            Score resourceScore = worker.generateResourceScore(this, this.task.getTaskDescription(), actionScore);
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
            throw new UnassignedActionException();
        }
        assignWorkerAndImpl(bestWorker, bestImpl);
    }

    /**
     * Assigns an implementation and a worker to the action and submits the action scheduler to the corresponding
     * ResourceScheduler. The method assumes that the worker has already been checked to be a proper candidate to host
     * the action and the selected implementation is a compatible pick given the action, its requirements and the
     * worker's capabilities.
     *
     * @param worker worker where to submit the action
     * @param impl implementation to request the execution
     */
    private void assignWorkerAndImpl(ResourceScheduler worker, Implementation impl) {
        LOGGER.info("Assigning action " + this + " to worker " + worker.getName() + " with implementation "
            + impl.getImplementationId());

        this.assignImplementation(impl);
        assignResource(worker);
        worker.scheduleAction(this);

        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onSchedule();
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
