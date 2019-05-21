package es.bsc.compss.nio.listeners;

import es.bsc.compss.data.MultiOperationFetchListener;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.worker.NIOWorker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TaskFetchOperationsListener extends MultiOperationFetchListener {

    private static final Logger WORKER_LOGGER = LogManager.getLogger(Loggers.WORKER);

    private final NIOTask task;
    private final NIOWorker nw;


    /**
     * Creates a Task fetch operation listener.
     * 
     * @param task Task to fetch.
     * @param nw Associated NIOWorker.
     */
    public TaskFetchOperationsListener(NIOTask task, NIOWorker nw) {
        super();
        this.task = task;
        this.nw = nw;
    }

    /**
     * Returns the task to fetch.
     * 
     * @return The task to fetch.
     */
    public NIOTask getTask() {
        return this.task;
    }

    @Override
    public void doCompleted() {
        Long stTime = this.nw.getTimes(this.task.getJobId());
        if (stTime != null) {
            long duration = System.currentTimeMillis() - stTime;
            WORKER_LOGGER.info(" [Profile] Transfer: " + duration);
        }
        this.nw.executeTask(this.task);
    }

    @Override
    public void doFailure(String failedDataId, Exception cause) {
        // Create job*_[NEW|RESUBMITTED|RESCHEDULED].[out|err]
        // If we don't create this when the task fails to retrieve a value,
        // the master will try to get the out of this job, and it will get blocked.
        // Same for the worker when sending, throwing an error when trying
        // to read the job out, which wouldn't exist
        String baseJobPath = this.nw.getStandardStreamsPath(this.task);
        String errorMessage = "Worker closed because the data " + failedDataId + " couldn't be retrieved.";
        String taskFileOutName = baseJobPath + ".out";
        this.nw.checkStreamFileExistence(taskFileOutName, "out", errorMessage);
        String taskFileErrName = baseJobPath + ".err";
        this.nw.checkStreamFileExistence(taskFileErrName, "err", errorMessage);
        this.nw.sendTaskDone(task, false);
    }

}
