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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.parameter.CollectionParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.DictCollectionParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.JobDispatcher;
import es.bsc.compss.worker.COMPSsException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Base64;

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
    protected final TaskDescription taskParams;
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
        if (this.cancelling) {
            cancelled();
        } else {
            JOB_LOGGER.info("Ordering transfers to " + this.worker.getName() + " to run task: " + this.taskId);
            JobTransfersListener stageInListener = new JobTransfersListener() {

                @Override
                public void stageInCompleted() {
                    if (JobImpl.this.cancelling) {
                        JobImpl.this.cancelled();
                    } else {
                        JOB_LOGGER.debug("Received a notification for the transfers of task " + JobImpl.this.taskId
                            + " with state DONE");
                        JobImpl.this.listener.stageInCompleted();
                    }
                }

                @Override
                public void stageInFailed(int numErrors) {
                    if (JobImpl.this.cancelling) {
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

    private void removeTmpData() {
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

    @Override
    public void cancel() throws Exception {
        this.cancelling = true;
        this.cancelJob();
        registerJobOutputs();
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
        // Remove temporary data for INOUT params
        removeTmpData();

        if (this.history == JobHistory.CANCELLED) {
            JOB_LOGGER.error("Ignoring notification since the job was cancelled");
            return;
        }

        // Job finished, update info about the generated/updated data
        registerJobOutputs();
        if (this.cancelling) {
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
        // Remove temporary data for INOUT params
        removeTmpData();

        if (this.history == JobHistory.CANCELLED) {
            JOB_LOGGER.error("Ignoring notification since the job was cancelled");
            return;
        }
        if (cancelling) {
            registerJobOutputs();
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
                        registerJobOutputs();
                        break;
                    case CANCEL_SUCCESSORS:
                        ErrorManager.warn("Cancelling successors.");
                        registerJobOutputs();
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
        // Remove temporary data for INOUT
        removeTmpData();

        if (this.history == JobHistory.CANCELLED) {
            JOB_LOGGER.error("Ignoring notification since the job was cancelled");
            return;
        }

        // Job finished, update info about the generated/updated data
        registerJobOutputs();

        if (this.cancelling) {
            cancelled();
        } else {
            this.listener.jobException(this, exception);
        }
    }

    private void registerJobOutputs() {
        List<Parameter> params = this.taskParams.getParameters();
        int subParamIdx = 0;
        for (Parameter p : params) {
            registerParameterOutput(new int[] { subParamIdx }, p);
            subParamIdx++;
        }
    }

    private void registerParameterOutput(int[] idx, Parameter p) {
        if (!p.isPotentialDependency()) {
            return;
        }
        DependencyParameter dp = (DependencyParameter) p;
        String dataName = getOuputRename(p);
        switch (dp.getType()) {
            case COLLECTION_T: {
                CollectionParameter cp = (CollectionParameter) dp;
                int[] newIdx = new int[idx.length + 1];
                System.arraycopy(idx, 0, newIdx, 0, idx.length);
                newIdx[idx.length] = 0;
                for (Parameter elem : cp.getParameters()) {
                    if (elem.isPotentialDependency()) {
                        registerParameterOutput(newIdx, elem);
                        newIdx[idx.length]++;
                    }
                }
                break;
            }
            case DICT_COLLECTION_T: {
                DictCollectionParameter dcp = (DictCollectionParameter) dp;
                int[] newIdx = new int[idx.length + 1];
                for (Map.Entry<Parameter, Parameter> entry : dcp.getParameters().entrySet()) {
                    Parameter k = entry.getKey();
                    if (k.isPotentialDependency()) {
                        registerParameterOutput(newIdx, k);
                        newIdx[idx.length]++;
                    }
                    Parameter v = entry.getValue();
                    if (v.isPotentialDependency()) {
                        registerParameterOutput(newIdx, v);
                        newIdx[idx.length]++;
                    }
                }
                break;
            }
            default:
                if (dataName != null) {
                    switch (this.getType()) {
                        case METHOD:
                            registerResultLocation(dp, dataName, this.worker);
                            break;
                        case HTTP:
                            saveHTTPResult(dp, dataName);
                            break;
                    }
                }
        }
        if (dataName != null) {
            this.listener.resultAvailable(idx, p, dataName);
        }
    }

    private String getOuputRename(Parameter p) {
        String name = null;
        if (p.isPotentialDependency()) {
            // Notify the FileTransferManager about the generated/updated OUT/INOUT datums
            DependencyParameter dp = (DependencyParameter) p;
            DataInstanceId dId = null;
            switch (p.getDirection()) {
                case OUT:
                    dId = ((WAccessId) dp.getDataAccessId()).getWrittenDataInstance();
                    break;
                case COMMUTATIVE:
                    dId = ((RWAccessId) dp.getDataAccessId()).getWrittenDataInstance();
                    break;
                case INOUT:
                    dId = ((RWAccessId) dp.getDataAccessId()).getWrittenDataInstance();
                    break;
                case CONCURRENT:
                case IN_DELETE:
                case IN:
                default:
                    // FTM already knows about this datum
                    return null;
            }

            // Retrieve parameter information
            name = dId.getRenaming();
        }
        return name;
    }

    private DataLocation registerResultLocation(DependencyParameter dp, String dataName, Resource res) {
        // Request transfer
        DataLocation outLoc = null;
        try {
            String dataTarget = dp.getDataTarget();
            if (DEBUG) {
                JOB_LOGGER.debug("Proposed URI for storing output param: " + dataTarget);
            }
            SimpleURI resultURI = new SimpleURI(dataTarget);
            SimpleURI targetURI = new SimpleURI(resultURI.getSchema() + resultURI.getPath());
            outLoc = DataLocation.createLocation(res, targetURI);
            // Data target has been stored as URI but final target data should be just the path
            dp.setDataTarget(outLoc.getPath());
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + dp.getDataTarget(), e);
        }
        Comm.registerLocation(dataName, outLoc);

        // Return location
        return outLoc;
    }

    private void saveHTTPResult(DependencyParameter dp, String dataName) {
        JsonObject retValue = (JsonObject) this.getReturnValue();

        if (dp.getType().equals(DataType.FILE_T)) {
            Object value = retValue.get(dp.getName()).toString();
            try {
                FileWriter file = new FileWriter(dp.getDataTarget());
                // 0004 is the JSON package ID in Python binding
                file.write("0004");
                file.write(value.toString());
                file.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            registerResultLocation(dp, dataName, Comm.getAppHost());
        } else {
            // it's a Java HTTP task, can have only single value of a primitive type
            Gson gson = new Gson();
            JsonPrimitive primValue = retValue.getAsJsonPrimitive("$return_0");
            Object value;
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
                            if (dp.getContentType().equals("java.lang.String")) {
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
            LogicalData ld = Comm.registerValue(dataName, value);
            for (DataLocation dl : ld.getLocations()) {
                dp.setDataTarget(dl.getPath());
            }
        }
    }

}
