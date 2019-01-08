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
package es.bsc.compss.types.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.Implementation.TaskType;
import es.bsc.compss.types.resources.Resource;


/**
 * Abstract representation of a job
 *
 * @param <T>
 */
public abstract class Job<T extends COMPSsWorker> {

    // Job identifier management
    protected static final int FIRST_JOB_ID = 1;
    protected static int nextJobId = FIRST_JOB_ID;

    // Environment variables for job execution
    private static final String classpathFromEnvironment = (System.getProperty(COMPSsConstants.WORKER_CP) != null
            && !System.getProperty(COMPSsConstants.WORKER_CP).equals("")) ? System.getProperty(COMPSsConstants.WORKER_CP) : "\"\"";
    private final String workerClasspath;

    private static final String pythonpathFromEnvironment = (System.getProperty(COMPSsConstants.WORKER_PP) != null
            && !System.getProperty(COMPSsConstants.WORKER_PP).equals("")) ? System.getProperty(COMPSsConstants.WORKER_PP) : "\"\"";
    private final String workerPythonpath;


    // Job history
    public enum JobHistory {
        NEW, // New job
        RESUBMITTED_FILES, // Resubmit transfers
        RESUBMITTED, // Resubmit job
        FAILED // Completely failed (can create new job for reschedule)
    }

    // Information of the job
    protected int jobId;

    protected final int taskId;
    protected final TaskDescription taskParams;
    protected final Implementation impl;
    protected final Resource worker;
    protected final JobListener listener;

    protected JobHistory history;
    protected int transferId;

    protected static final Logger logger = LogManager.getLogger(Loggers.COMM);
    protected static final boolean debug = logger.isDebugEnabled();

    /**
     * Creates a new job instance with the given parameters
     *
     * @param taskId
     * @param task
     * @param impl
     * @param res
     * @param listener
     */
    public Job(int taskId, TaskDescription task, Implementation impl, Resource res, JobListener listener) {
        jobId = nextJobId++;
        this.taskId = taskId;
        this.history = JobHistory.NEW;
        this.taskParams = task;
        this.impl = impl;
        this.worker = res;
        this.listener = listener;

        /*
         * Setup job environment variables ****************************************
         */
 /*
         * This variables are only used by GAT since NIO loads them from the worker rather than specific variables per
         * job
         */
        // Merge command classpath and worker defined classpath
        String classpathFromFile = getResourceNode().getClasspath();

        if (!classpathFromFile.equals("")) {
            if (!classpathFromEnvironment.equals("")) {
                this.workerClasspath = classpathFromEnvironment + ":" + classpathFromFile;
            } else {
                this.workerClasspath = classpathFromFile;
            }
        } else {
            this.workerClasspath = classpathFromEnvironment;
        }

        // Merge command pythonpath and worker defined pythonpath
        String pythonpathFromFile = getResourceNode().getPythonpath();
        if (!pythonpathFromFile.equals("")) {
            if (!pythonpathFromEnvironment.equals("")) {
                this.workerPythonpath = pythonpathFromEnvironment + ":" + pythonpathFromFile;
            } else {
                this.workerPythonpath = pythonpathFromFile;
            }
        } else {
            this.workerPythonpath = pythonpathFromEnvironment;
        }
    }

    /**
     * Returns the language of the task
     *
     * @return language of the task
     */
    public Lang getLang() {
        return this.taskParams.getLang();
    }

    /**
     * Returns the job id
     *
     * @return
     */
    public int getJobId() {
        return this.jobId;
    }

    /**
     * Returns the id of the task executed by this job
     *
     * @return
     */
    public int getTaskId() {
        return this.taskId;
    }

    /**
     * Returns the task params
     *
     * @return
     */
    public TaskDescription getTaskParams() {
        return this.taskParams;
    }

    /**
     * Returns the job history
     *
     * @return
     */
    public JobHistory getHistory() {
        return this.history;
    }

    /**
     * Sets a new job history
     *
     * @param newHistoryState
     */
    public void setHistory(JobHistory newHistoryState) {
        this.history = newHistoryState;
    }

    /**
     * Returns the resource assigned to the job execution
     *
     * @return
     */
    public Resource getResource() {
        return this.worker;
    }

    /**
     * Returns the job classpath
     *
     * @return
     */
    public String getClasspath() {
        return this.workerClasspath;
    }

    /**
     * Returns the job pythonpath
     *
     * @return
     */
    public String getPythonpath() {
        return this.workerPythonpath;
    }

    /**
     * Returns the resource node assigned to the job
     *
     * @return
     */
    @SuppressWarnings("unchecked")
    public T getResourceNode() {
        return (T) this.worker.getNode();
    }

    /**
     * Returns the job listener associated to the job
     *
     * @return
     */
    public JobListener getListener() {
        return this.listener;
    }

    /**
     * Returns the core implementation
     *
     * @return
     */
    public Implementation getImplementation() {
        return this.impl;
    }

    /**
     * Sets the transfer group id
     *
     * @param transferId
     */
    public void setTransferGroupId(int transferId) {
        this.transferId = transferId;
    }

    /**
     * Returns the transfer group id
     *
     * @return
     */
    public int getTransferGroupId() {
        return this.transferId;
    }

    /**
     * Returns the return value of the job
     *
     * @return
     */
    public Object getReturnValue() {
        return null;
    }

    /**
     * Actions to submit the job
     *
     * @throws Exception
     */
    public abstract void submit() throws Exception;

    /**
     * Actions to stop the job
     *
     * @throws Exception
     */
    public abstract void stop() throws Exception;

    /**
     * Returns the hostname
     *
     * @return
     */
    public abstract String getHostName();

    /**
     * Returns the task type of the job
     *
     * @return
     */
    public abstract TaskType getType();

    @Override
    public abstract String toString();

}
