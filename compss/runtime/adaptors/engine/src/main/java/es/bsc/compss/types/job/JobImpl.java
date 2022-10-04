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
package es.bsc.compss.types.job;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.parameter.CollectionParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.DictCollectionParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.util.JobDispatcher;
import es.bsc.compss.worker.COMPSsException;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Abstract representation of a job.
 *
 * @param <T> COMPSs Worker
 */
public abstract class JobImpl<T extends COMPSsWorker> implements Job<T> {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final Logger JOB_LOGGER = LogManager.getLogger(Loggers.JM_COMP);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();
    protected static final boolean JOB_DEBUG = JOB_LOGGER.isDebugEnabled();

    // Fault tolerance parameters
    private static final int TRANSFER_CHANCES = 2;

    // Job identifier management
    protected static final int FIRST_JOB_ID = 1;
    protected static int nextJobId = FIRST_JOB_ID;

    // Environment variables for job execution
    private static final String CLASSPATH_FROM_ENV = (System.getProperty(COMPSsConstants.WORKER_CP) != null
        && !System.getProperty(COMPSsConstants.WORKER_CP).equals("")) ? System.getProperty(COMPSsConstants.WORKER_CP)
            : "\"\"";
    private final String workerClasspath;

    private static final String PYTHONPATH_FROM_ENV = (System.getProperty(COMPSsConstants.WORKER_PP) != null
        && !System.getProperty(COMPSsConstants.WORKER_PP).equals("")) ? System.getProperty(COMPSsConstants.WORKER_PP)
            : "\"\"";
    private final String workerPythonpath;

    // Information of the job
    protected int jobId;

    protected final int taskId;
    protected final TaskDescription taskParams;
    protected final Implementation impl;
    protected final Resource worker;
    protected final JobListener listener;
    protected final List<Integer> predecessors;
    protected final Integer numSuccessors;

    protected JobHistory history;
    protected int transferId;
    private int transferErrors;
    private int executionErrors;


    /**
     * Creates a new job instance with the given parameters.
     *
     * @param taskId Task Identifier
     * @param task Task description
     * @param impl Task Implementation
     * @param res Assigned Resource
     * @param listener Listener to notify job events
     */
    public JobImpl(int taskId, TaskDescription task, Implementation impl, Resource res, JobListener listener,
        List<Integer> predecessors, Integer numSuccessors) {
        this.jobId = nextJobId++;
        this.taskId = taskId;
        this.history = JobHistory.NEW;
        this.transferErrors = 0;
        this.executionErrors = 0;
        this.taskParams = task;
        this.impl = impl;
        this.worker = res;
        this.listener = listener;
        this.predecessors = predecessors;
        this.numSuccessors = numSuccessors;
        /*
         * Setup job environment variables ****************************************
         * 
         * This variables are only used by GAT since NIO loads them from the worker rather than specific variables per
         * job
         */

        // Merge command classpath and worker defined classpath
        String classpathFromFile = getResourceNode().getClasspath();

        if (!classpathFromFile.equals("")) {
            if (!CLASSPATH_FROM_ENV.equals("")) {
                this.workerClasspath = CLASSPATH_FROM_ENV + ":" + classpathFromFile;
            } else {
                this.workerClasspath = classpathFromFile;
            }
        } else {
            this.workerClasspath = CLASSPATH_FROM_ENV;
        }

        // Merge command pythonpath and worker defined pythonpath
        String pythonpathFromFile = getResourceNode().getPythonpath();
        if (!pythonpathFromFile.equals("")) {
            if (!PYTHONPATH_FROM_ENV.equals("")) {
                this.workerPythonpath = PYTHONPATH_FROM_ENV + ":" + pythonpathFromFile;
            } else {
                this.workerPythonpath = pythonpathFromFile;
            }
        } else {
            this.workerPythonpath = PYTHONPATH_FROM_ENV;
        }
    }

    /**
     * Returns the language of the task.
     *
     * @return language of the task
     */
    public Lang getLang() {
        return this.taskParams.getLang();
    }

    /**
     * Returns the job id.
     *
     * @return
     */
    public int getJobId() {
        return this.jobId;
    }

    /**
     * Returns the id of the task executed by this job.
     *
     * @return
     */
    public int getTaskId() {
        return this.taskId;
    }

    /**
     * Returns the task parameters.
     *
     * @return
     */
    public TaskDescription getTaskParams() {
        return this.taskParams;
    }

    /**
     * Returns the job history.
     *
     * @return
     */
    public JobHistory getHistory() {
        return this.history;
    }

    /**
     * Sets a new job history.
     *
     * @param newHistoryState job history state
     */
    public void setHistory(JobHistory newHistoryState) {
        this.history = newHistoryState;
    }

    /**
     * Returns the resource assigned to the job execution.
     *
     * @return
     */
    public Resource getResource() {
        return this.worker;
    }

    /**
     * Returns the job classpath. .
     * 
     * @return
     */
    public String getClasspath() {
        return this.workerClasspath;
    }

    /**
     * Returns the job pythonpath.
     *
     * @return
     */
    public String getPythonpath() {
        return this.workerPythonpath;
    }

    /**
     * Returns the resource node assigned to the job.
     *
     * @return
     */
    @SuppressWarnings("unchecked")
    public T getResourceNode() {
        return (T) this.worker.getNode();
    }

    /**
     * Returns the core implementation.
     *
     * @return
     */
    public Implementation getImplementation() {
        return this.impl;
    }

    /**
     * Sets the transfer group id.
     *
     * @param transferId Transfer group Id
     */
    public void setTransferGroupId(int transferId) {
        this.transferId = transferId;
    }

    /**
     * Returns the transfer group id.
     *
     * @return
     */
    public int getTransferGroupId() {
        return this.transferId;
    }

    /**
     * Returns the return value of the job.
     *
     * @return
     */
    public Object getReturnValue() {
        return null;
    }

    /**
     * Returns the hostname.
     *
     * @return
     */
    public abstract String getHostName();

    /**
     * Returns the task type of the job.
     *
     * @return
     */
    public abstract TaskType getType();

    @Override
    public abstract String toString();

    /**
     * Returns the on-failure mechanisms.
     *
     * @return The on-failure mechanisms.
     */
    public OnFailure getOnFailure() {
        return this.taskParams.getOnFailure();
    }

    /**
     * Returns the time out of the task.
     *
     * @return time out of the task
     */
    public long getTimeOut() {
        return this.taskParams.getTimeOut();
    }

    /**
     * Returns the predecessors of a task.
     *
     * @return predecessors of the task
     */
    public List<Integer> getPredecessors() {
        return this.predecessors;
    }

    /**
     * Returns the number of successors of a task.
     *
     * @return number of successors of the task
     */
    public Integer getNumSuccessors() {
        return this.numSuccessors;
    }

    /*
     * -------------------------------------------------------------------------------------------------
     * ---------------------------------- LIFECYCLE MANAGEMENT -----------------------------------------
     * -------------------------------------------------------------------------------------------------
     */

    /**
     * Orders the copying of all the necessary input data.
     */
    @Override
    public void stageIn() {
        JobTransfersListener stageInListener = new JobTransfersListener() {

            @Override
            public void stageInCompleted() {
                JOB_LOGGER.debug(
                    "Received a notification for the transfers of task " + JobImpl.this.taskId + " with state DONE");
                JobImpl.this.listener.stageInCompleted();
            }

            @Override
            public void stageInFailed(int numErrors) {
                JOB_LOGGER.debug(
                    "Received a notification for the transfers for task " + JobImpl.this.taskId + " with state FAILED");
                JobImpl.this.removeTmpData();
                JobImpl.this.transferErrors++;
                if (JobImpl.this.transferErrors < TRANSFER_CHANCES
                    && JobImpl.this.taskParams.getOnFailure() == OnFailure.RETRY) {
                    JOB_LOGGER.debug("Resubmitting input files for task " + JobImpl.this.taskId + " to host "
                        + JobImpl.this.worker.getName() + " since " + numErrors + " transfers failed.");
                    JobImpl.this.stageIn();
                } else {
                    JobImpl.this.listener.stageInFailed(numErrors);
                }
            }

        };
        this.transferId = stageInListener.getId();
        transferInputData(stageInListener);
        stageInListener.enable();
    }

    private void transferInputData(JobTransfersListener listener) {
        for (Parameter p : this.taskParams.getParameters()) {
            if (DEBUG) {
                JOB_LOGGER.debug("    * " + p);
            }
            if (p.isPotentialDependency()) {
                DependencyParameter dp = (DependencyParameter) p;
                switch (this.taskParams.getType()) {
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
        DataAccessId access = param.getDataAccessId();
        if (access instanceof WAccessId) {
            String outRename = ((WAccessId) access).getWrittenDataInstance().getRenaming();
            String dataTarget = this.worker.getNode().getOutputDataTarget(outRename, param);
            param.setDataTarget(dataTarget);

        } else {
            if (access instanceof RAccessId) {
                // Read Access, transfer object
                listener.addOperation();

                LogicalData srcData = ((RAccessId) access).getReadDataInstance().getData();
                this.worker.getData(srcData, param, listener);
            } else {
                // ReadWrite Access, transfer object
                listener.addOperation();
                LogicalData srcData = ((RWAccessId) access).getReadDataInstance().getData();
                String tgtName = ((RWAccessId) access).getWrittenDataInstance().getRenaming();
                LogicalData tmpData = Comm.registerData("tmp" + tgtName);
                this.worker.getData(srcData, tgtName, tmpData, param, listener);
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
        if (DEBUG) {
            JOB_LOGGER
                .debug("Requesting stream transfer from " + source + " to " + target + " at " + this.worker.getName());
        }
        listener.addOperation();
        this.worker.getData(source, target, param, listener);
    }

    @Override
    public void removeTmpData() {
        for (Parameter p : this.taskParams.getParameters()) {
            if (DEBUG) {
                JOB_LOGGER.debug("    * " + p);
            }
            if (p.isPotentialDependency()) {
                DependencyParameter dp = (DependencyParameter) p;
                switch (this.taskParams.getType()) {
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

    @Override
    public final void submit() {
        this.listener.submitted(this);
        JobDispatcher.dispatch(this);
        JOB_LOGGER.info("Submitted Task: " + this.taskId + " Job: " + this.jobId + " Method: "
            + this.taskParams.getName() + " Resource: " + this.worker.getName());
    }

    /**
     * Submits the job.
     *
     * @throws Exception Error when submitting a job
     */
    public abstract void submitJob() throws Exception;

    /**
     * Stops the job.
     *
     * @throws Exception Error when stopping a job
     */
    public abstract void cancelJob() throws Exception;

    public void profileArrival() {
        this.listener.arrived(this);
    }

    public void profileArrivalAt(long ts) {
        this.listener.arrivedAt(this, ts);
    }

    public void fetchedAllInputData() {
        this.listener.allInputDataOnWorker(this);
    }

    public void fetchedAllInputDataAt(long ts) {
        this.listener.allInputDataOnWorkerAt(this, ts);
    }

    public void executionStarts() {
        this.listener.startingExecution(this);
    }

    public void executionStartedAt(long ts) {
        this.listener.startingExecutionAt(this, ts);
    }

    public void executionEnds() {
        this.listener.endedExecution(this);
    }

    public void executionEndsAt(long ts) {
        this.listener.endedExecutionAt(this, ts);
    }

    public void profileEndNotification() {
        this.listener.endNotified(this);
    }

    public void profileEndNotificationAt(long ts) {
        this.listener.endNotifiedAt(this, ts);
    }

    public void completed() {
        this.listener.jobCompleted(this);
    }

    public void failed(JobEndStatus status) {
        this.listener.jobFailed(this, status);
    }

    public void exception(COMPSsException exception) {
        this.listener.jobException(this, exception);
    }

}
