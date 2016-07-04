package integratedtoolkit.types.job;

import org.apache.log4j.Logger;

import integratedtoolkit.ITConstants;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.types.resources.Resource;


public abstract class Job<T extends COMPSsWorker> {

    // Job identifier management
    protected static final int FIRST_JOB_ID = 1;
    protected static int nextJobId = FIRST_JOB_ID;
    // Language
    protected static final String lang = System.getProperty(ITConstants.IT_LANG);
    // Tracing
    protected static final boolean tracing = System.getProperty(ITConstants.IT_TRACING) != null
            && Integer.parseInt(System.getProperty(ITConstants.IT_TRACING)) > 0;
            
    // Environment variables for job execution
    private static final String classpathFromEnvironment = (System.getProperty(ITConstants.IT_WORKER_CP) != null
            && !System.getProperty(ITConstants.IT_WORKER_CP).equals(""))
            ? System.getProperty(ITConstants.IT_WORKER_CP) : "\"\"";
    private final String classpathFromFile;
    private final String workerClasspath;
            
    private static final String pythonpathFromEnvironment = (System.getProperty(ITConstants.IT_WORKER_PP) != null
            && !System.getProperty(ITConstants.IT_WORKER_PP).equals(""))
            ? System.getProperty(ITConstants.IT_WORKER_PP) : "\"\"";
    private final String pythonpathFromFile;
    private final String workerPythonpath;
       

    // Job history
    public enum JobHistory {
        NEW,
        RESUBMITTED_FILES,
        RESUBMITTED,
        FAILED;
    }

    // Job kind
    public enum JobKind {
        METHOD,
        SERVICE;
    }

    // Information of the job
    protected int jobId;

    protected final int taskId;
    protected final TaskParams taskParams;
    protected final Implementation<?> impl;
    protected final Resource worker;
    protected final JobListener listener;

    protected JobHistory history;
    protected int transferId;

    protected static final Logger logger = Logger.getLogger(Loggers.COMM);
    protected static final boolean debug = logger.isDebugEnabled();

    public Job(int taskId, TaskParams task, Implementation<?> impl, Resource res, JobListener listener) {
        jobId = nextJobId++;
        this.taskId = taskId;
        this.history = JobHistory.NEW;
        this.taskParams = task;
        this.impl = impl;
        this.worker = res;
        this.listener = listener;
        
        /* Setup job environment variables *****************************************/
        /* This variables are only used by GAT since NIO loads them from the worker 
         * rather than specific variables per job
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

    public int getJobId() {
        return jobId;
    }

    public TaskParams getTaskParams() {
        return taskParams;
    }

    public JobHistory getHistory() {
        return history;
    }

    public void setHistory(JobHistory newHistoryState) {
        this.history = newHistoryState;
    }

    public Resource getResource() {
        return this.worker;
    }
    
    public String getClasspath() {
    	return this.workerClasspath;
    }
    
    public String getPythonpath() {
    	return this.workerPythonpath;
    }

    @SuppressWarnings("unchecked")
    public T getResourceNode() {
        return (T) this.worker.getNode();
    }

    public JobListener getListener() {
        return listener;
    }

    public Implementation<?> getImplementation() {
        return this.impl;
    }

    public void setTransferGroupId(int transferId) {
        this.transferId = transferId;
    }

    public int getTransferGroupId() {
        return this.transferId;
    }

    public abstract String toString();

    public abstract void submit() throws Exception;

    public abstract void stop() throws Exception;

    public Object getReturnValue() {
        return null;
    }

    public abstract String getHostName();

    public abstract JobKind getKind();

    public static interface JobListener {

        enum JobEndStatus {
            OK,
            TO_RESCHEDULE,
            TRANSFERS_FAILED,
            SUBMISSION_FAILED,
            EXECUTION_FAILED;
        }

        void jobCompleted(Job<?> job);

        void jobFailed(Job<?> job, JobEndStatus endStatus);

    }
}
