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
package es.bsc.compss.types.job;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.DataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.parameter.CollectiveParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.JobDispatcher;
import es.bsc.compss.worker.COMPSsException;

import java.io.File;
import java.util.List;

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
    private static final int SUBMISSION_CHANCES = 2;

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
    protected final TaskDescription<Parameter> taskParams;
    protected final Implementation impl;
    protected final Resource worker;
    private final JobListener listener;
    protected final List<Integer> predecessors;
    protected final Integer numSuccessors;

    private boolean cancelling;
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
        this.cancelling = false;
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
    @Override
    public int getJobId() {
        return this.jobId;
    }

    /**
     * Returns the id of the task executed by this job.
     *
     * @return
     */
    @Override
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
     * Returns the job history.
     *
     * @return
     */
    public JobHistory getHistory() {
        return this.history;
    }

    /**
     * Returns whether the job is being cancelled.
     * 
     * @return {@literal true} if the job is being cancelled but the cancellation has not been confirmed yet;
     *         {@literal false} otherwise.
     */
    public boolean isBeingCancelled() {
        return this.cancelling;
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
     * Returns the resource node assigned to the job.
     *
     * @return
     */
    @SuppressWarnings("unchecked")
    public T getResourceNode() {
        return (T) this.worker.getNode();
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
     * Returns the core implementation.
     *
     * @return
     */
    public Implementation getImplementation() {
        return this.impl;
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
     * Returns the task type of the job.
     *
     * @return
     */
    public abstract TaskType getType();

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

    @Override
    public abstract String toString();

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
        if (this.isBeingCancelled()) {
            cancelled();
        } else {
            JOB_LOGGER.info("Ordering transfers to " + this.worker.getName() + " to run task: " + this.taskId);
            JobTransfersListener stageInListener = new JobTransfersListener() {

                @Override
                public void stageInCompleted() {
                    if (JobImpl.this.isBeingCancelled()) {
                        JobImpl.this.cancelled();
                    } else {
                        JOB_LOGGER.debug("Received a notification for the transfers of task " + JobImpl.this.taskId
                            + " with state DONE");
                        JobImpl.this.listener.stageInCompleted();
                    }
                }

                @Override
                public void stageInFailed(int numErrors) {
                    if (JobImpl.this.isBeingCancelled()) {
                        JobImpl.this.cancelled();
                    } else {
                        JOB_LOGGER.debug("Received a notification for the transfers for task " + JobImpl.this.taskId
                            + " with state FAILED");
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
                }

            };
            this.transferId = stageInListener.getId();
            transferInputData(stageInListener);
            stageInListener.enable();
        }
    }

    private void transferInputData(JobTransfersListener listener) {
        for (Parameter p : this.taskParams.getParameters()) {
            if (DEBUG) {
                JOB_LOGGER.debug("    * " + p);
            }
            if (p.isPotentialDependency()) {
                DependencyParameter dp = (DependencyParameter) p;
                transferJobData(dp, listener);
            }
        }
    }

    // Private method that performs data transfers
    private void transferJobData(DependencyParameter param, JobTransfersListener listener) {
        switch (param.getType()) {
            case COLLECTION_T:
            case DICT_COLLECTION_T:
                CollectiveParameter<Parameter> cp = (CollectiveParameter) param;
                JOB_LOGGER.debug("Detected CollectiveParameter " + cp);
                // Recursively send all the collection parameters
                for (Parameter p : cp.getElements()) {
                    if (p.isPotentialDependency()) {
                        DependencyParameter dp = (DependencyParameter) p;
                        transferJobData(dp, listener);
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
        if (access != null) {
            if (!access.isRead()) {
                String outRename = ((WritingDataAccessId) access).getWrittenDataInstance().getRenaming();
                String dataTarget = this.worker.getNode().getOutputDataTarget(outRename, param);
                param.setDataTarget(dataTarget);

            } else {
                listener.addOperation();
                DataInstanceId dId = ((ReadingDataAccessId) access).getReadDataInstance();
                if (dId == null) {
                    ErrorManager.warn("Read Data Instance for Param: " + param.getName() + " Task: " + this.taskId
                        + " Job: " + this.jobId + " Method: " + this.taskParams.getName() + " is not defined (null)");
                    listener.notifyFailure(null,
                        new Exception(
                            "Read Data Instance for Param: " + param.getName() + " Task: " + this.taskId + " Job: "
                                + this.jobId + " Method: " + this.taskParams.getName() + " is not defined (null)"));
                }

                LogicalData srcData = dId.getData();
                if (access.isWrite()) {
                    String tgtName = ((WritingDataAccessId) access).getWrittenDataInstance().getRenaming();
                    LogicalData tmpData = Comm.registerData("tmp" + tgtName);
                    this.worker.getData(srcData, tgtName, tmpData, param, listener);
                } else {
                    this.worker.getData(srcData, param, listener);
                }
            }
        } else {
            listener.addOperation();
            ErrorManager.warn("Access for Param: " + param.getName() + " Task: " + this.taskId + " Job: " + this.jobId
                + " Method: " + this.taskParams.getName() + " is not defined (null)");
            listener.notifyFailure(null, new Exception("Access for Param: " + param.getName() + " Task: " + this.taskId
                + " Job: " + this.jobId + " Method: " + this.taskParams.getName() + " is not defined (null)"));

        }
    }

    private void transferStreamParameter(DependencyParameter param, JobTransfersListener listener) {
        DataAccessId access = param.getDataAccessId();
        LogicalData source;
        LogicalData target;
        if (access.isRead()) {
            source = ((ReadingDataAccessId) access).getReadDataInstance().getData();
            if (access.isWrite()) {
                target = ((WritingDataAccessId) access).getWrittenDataInstance().getData();
            } else {
                target = source;
            }
        } else {
            target = ((WritingDataAccessId) access).getWrittenDataInstance().getData();
            source = target;
        }

        // Ask for transfer
        if (DEBUG) {
            JOB_LOGGER
                .debug("Requesting stream transfer from " + source + " to " + target + " at " + this.worker.getName());
        }
        listener.addOperation();
        this.worker.getData(source, target, param, listener);
    }

    protected void removeTmpData() {
        for (Parameter p : this.taskParams.getParameters()) {
            if (DEBUG) {
                JOB_LOGGER.debug("    * " + p);
            }
            if (p.isPotentialDependency()) {
                DependencyParameter dp = (DependencyParameter) p;
                removeTmpData(dp);
            }
        }

    }

    private void removeTmpData(DependencyParameter param) {
        if (param.getType() != DataType.STREAM_T && param.getType() != DataType.EXTERNAL_STREAM_T) {
            if (param.isCollective()) {
                CollectiveParameter<Parameter> cp = (CollectiveParameter) param;
                JOB_LOGGER.debug("Detected CollectiveParameter " + cp);
                // Recursively send all the collection parameters
                for (Parameter p : cp.getElements()) {
                    if (p.isPotentialDependency()) {
                        DependencyParameter dp = (DependencyParameter) p;
                        removeTmpData(dp);
                    }
                }
            }
            DataAccessId access = param.getDataAccessId();
            if (access != null && access.isRead() && access.isWrite()) {
                String tgtName = "tmp" + ((WritingDataAccessId) access).getWrittenDataInstance().getRenaming();
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

    @Override
    public void cancel() throws Exception {
        this.cancelling = true;
        try {
            this.cancelJob();
            registerAllJobOutputsAsExpected();
        } catch (Exception e) {
            LOGGER.error("ERROR: Cancelling job. Continuing the cancellation. Results will be ignored.", e);
            registerAllJobOutputsAsExpected();
            cancelled();
        }
    }

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

    private void cancelled() {
        this.history = JobHistory.CANCELLED;
        this.cancelling = false;
        this.listener.jobCancelled(this);
    }

    /**
     * Actions to be done when the job execution ends.
     */
    public void completed() {
        JOB_LOGGER.info("Received a notification for job " + jobId + " with state OK");
        if (this.isBeingCancelled()) {
            cancelled();
        } else {
            this.listener.jobCompleted(this);
        }
    }

    /**
     * Actions to be done when the job execution fails.
     * 
     * @param status end status of the job
     */

    public void failed(JobEndStatus status) {
        JOB_LOGGER.error("Received a notification for job " + jobId + " with state FAILED");

        if (isBeingCancelled()) {
            cancelled();
        } else {
            final String errMsg = "Job " + this.jobId + ", running Task " + this.taskId + " on worker "
                + this.worker.getName() + ", has failed.";
            JOB_LOGGER.error(errMsg);
            ErrorManager.warn(errMsg);
            ++this.executionErrors;
            if (this.taskParams.getOnFailure() == OnFailure.RETRY
                && this.transferErrors + this.executionErrors < SUBMISSION_CHANCES) {
                final String resubmitMsg = "Resubmitting job to the same worker.";
                JOB_LOGGER.error(resubmitMsg);
                ErrorManager.warn(resubmitMsg);
                this.history = JobHistory.RESUBMITTED;
                submit();
            } else {
                switch (this.taskParams.getOnFailure()) {
                    case IGNORE:
                        ErrorManager.warn("Ignoring failure.");
                        break;
                    case CANCEL_SUCCESSORS:
                        ErrorManager.warn("Cancelling successors.");
                        break;
                    default:
                        // Do nothing before reporting the failure
                }
                this.listener.jobFailed(this, status);
            }
        }
    }

    /**
     * Actions to be done when the job execution raises an exception.
     * 
     * @param exception exception raised during the execution
     */
    public void exception(COMPSsException exception) {
        JOB_LOGGER.error("Received an exception notification for job " + this.jobId);

        if (this.isBeingCancelled()) {
            cancelled();
        } else {
            this.listener.jobException(this, exception);
        }
    }

    protected void registerAllJobOutputsAsExpected() {
        List<Parameter> params = this.taskParams.getParameters();
        for (Parameter p : params) {
            registerJobOutputAsExpected(p);
        }
    }

    private void registerJobOutputAsExpected(Parameter p) {
        if (!p.isPotentialDependency()) {
            return;
        }
        DependencyParameter dp = (DependencyParameter) p;
        String dataName = getOutputRename(p);
        if (dp.isCollective()) {
            CollectiveParameter<Parameter> cp = (CollectiveParameter) dp;
            for (Parameter elem : cp.getElements()) {
                if (elem.isPotentialDependency()) {
                    registerJobOutputAsExpected(elem);
                }
            }
        } else {
            if (dataName != null) {
                registerResultPrivateLocation(dp.getDataTarget(), dataName, this.worker);
            }
        }
        if (dataName != null) {
            notifyResultAvailability(dp, dataName);
        }
    }

    protected void notifyResultAvailability(DependencyParameter dp, String dataName) {
        this.listener.resultAvailable(dp, dataName);
        // Removing Temporary data
        DataAccessId access = dp.getDataAccessId();
        if (access.isRead() && access.isWrite()) {
            String tgtName = "tmp" + ((WritingDataAccessId) access).getWrittenDataInstance().getRenaming();
            Comm.removeDataKeepingValue(tgtName);
        }
    }

    protected String getOutputRename(Parameter p) {
        String name = null;
        if (p.isPotentialDependency()) {
            // Notify the FileTransferManager about the generated/updated OUT/INOUT datums
            DependencyParameter dp = (DependencyParameter) p;
            DataInstanceId dId = null;
            DataAccessId daId = dp.getDataAccessId();
            if (daId.isWrite()) {
                dId = ((WritingDataAccessId) dp.getDataAccessId()).getWrittenDataInstance();
            } else {
                return null;
            }

            // Retrieve parameter information
            name = dId.getRenaming();
        }
        return name;
    }

    protected DataLocation registerResultSharedLocation(String sharedDisk, String dataLocation, String dataName) {
        DataLocation outLoc = null;
        try {
            if (DEBUG) {
                JOB_LOGGER.debug("Proposed URI for storing output param: " + dataLocation + "@" + sharedDisk);
            }
            String sharedPath = ProtocolType.SHARED_URI.getSchema() + sharedDisk + File.separator + dataLocation;
            SimpleURI resultURI = new SimpleURI(sharedPath);
            outLoc = DataLocation.createLocation(null, resultURI);
            Comm.registerLocation(dataName, outLoc);
        } catch (Exception e) {
            e.printStackTrace(System.out);
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + dataLocation, e);
        }
        return null;
    }

    protected DataLocation registerResultPrivateLocation(String dataLocation, String dataName, Resource res) {
        // Request transfer
        DataLocation outLoc = null;
        try {
            if (DEBUG) {
                JOB_LOGGER.debug("Proposed URI for storing output param: " + dataLocation);
            }
            SimpleURI resultURI = new SimpleURI(dataLocation);
            SimpleURI targetURI = new SimpleURI(resultURI.getSchema() + resultURI.getPath());
            outLoc = DataLocation.createLocation(res, targetURI);
            Comm.registerLocation(dataName, outLoc);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + dataLocation, e);
        }

        // Return location
        return outLoc;
    }

}
