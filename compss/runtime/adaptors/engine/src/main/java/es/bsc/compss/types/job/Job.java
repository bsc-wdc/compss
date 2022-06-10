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
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.worker.COMPSsException;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Abstract representation of a job.
 *
 * @param <T> COMPSs Worker
 */
public abstract class Job<T extends COMPSsWorker> {

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

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

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


    /**
     * Creates a new job instance with the given parameters.
     *
     * @param taskId Task Identifier
     * @param task Task description
     * @param impl Task Implementation
     * @param res Assigned Resource
     * @param listener Listener to notify job events
     */
    public Job(int taskId, TaskDescription task, Implementation impl, Resource res, JobListener listener,
        List<Integer> predecessors, Integer numSuccessors) {
        this.jobId = nextJobId++;
        this.taskId = taskId;
        this.history = JobHistory.NEW;
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
     * Submits the job.
     *
     * @throws Exception Error when submitting a job
     */
    public abstract void submit() throws Exception;

    /**
     * Stops the job.
     *
     * @throws Exception Error when stopping a job
     */
    public abstract void cancelJob() throws Exception;

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
