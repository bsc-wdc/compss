package integratedtoolkit.types.job;

import org.apache.log4j.Logger;

import integratedtoolkit.ITConstants;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.COMPSsNode;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.types.resources.Resource;

public abstract class Job<T extends COMPSsNode> {

    // Job identifier management
    protected static final int FIRST_JOB_ID = 1;
    protected static int nextJobId = FIRST_JOB_ID;
    // Language
    protected static final String lang = System.getProperty(ITConstants.IT_LANG);
    // Tracing
    protected static final boolean tracing = System.getProperty(ITConstants.IT_TRACING) != null
            && Integer.parseInt(System.getProperty(ITConstants.IT_TRACING)) > 0;

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
