package integratedtoolkit.types.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.ITConstants;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.Implementation.TaskType;
import integratedtoolkit.types.resources.Resource;

/**
 * Abstract representation of a job
 *
 * @param <T>
 */
public abstract class Job<T extends COMPSsWorker> {

    // Job identifier management
    protected static final int FIRST_JOB_ID = 1;
    protected static int nextJobId = FIRST_JOB_ID;
    
    // Language
    protected static final String LANG = System.getProperty(ITConstants.IT_LANG);

    // Environment variables for job execution
    private static final String classpathFromEnvironment = (System.getProperty(ITConstants.IT_WORKER_CP) != null
            && !System.getProperty(ITConstants.IT_WORKER_CP).equals("")) ? System.getProperty(ITConstants.IT_WORKER_CP) : "\"\"";
    private final String classpathFromFile;
    private final String workerClasspath;

    private static final String pythonpathFromEnvironment = (System.getProperty(ITConstants.IT_WORKER_PP) != null
            && !System.getProperty(ITConstants.IT_WORKER_PP).equals("")) ? System.getProperty(ITConstants.IT_WORKER_PP) : "\"\"";
    private final String pythonpathFromFile;
    private final String workerPythonpath;


    // Job history
    public enum JobHistory {
        NEW, 
        RESUBMITTED_FILES, 
        RESUBMITTED, 
        FAILED
    }


    // Information of the job
    protected int jobId;

    protected final int taskId;
    protected final TaskDescription taskParams;
    protected final Implementation<?> impl;
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
    public Job(int taskId, TaskDescription task, Implementation<?> impl, Resource res, JobListener listener) {
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
        classpathFromFile = getResourceNode().getClasspath();

        if (!classpathFromFile.equals("")) {
            if (!classpathFromEnvironment.equals("")) {
                workerClasspath = classpathFromEnvironment + ":" + classpathFromFile;
            } else {
                workerClasspath = classpathFromFile;
            }
        } else {
            workerClasspath = classpathFromEnvironment;
        }

        // Merge command pythonpath and worker defined pythonpath
        pythonpathFromFile = getResourceNode().getPythonpath();
        if (!pythonpathFromFile.equals("")) {
            if (!pythonpathFromEnvironment.equals("")) {
                workerPythonpath = pythonpathFromEnvironment + ":" + pythonpathFromFile;
            } else {
                workerPythonpath = pythonpathFromFile;
            }
        } else {
            workerPythonpath = pythonpathFromEnvironment;
        }
    }

    /**
     * Returns the job id
     * 
     * @return
     */
    public int getJobId() {
        return jobId;
    }

    /**
     * Returns the task params
     * @return
     */
    public TaskDescription getTaskParams() {
        return taskParams;
    }

    /**
     * Returns the job history
     * 
     * @return
     */
    public JobHistory getHistory() {
        return history;
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
        return listener;
    }

    /**
     * Returns the core implementation
     * 
     * @return
     */
    public Implementation<?> getImplementation() {
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
