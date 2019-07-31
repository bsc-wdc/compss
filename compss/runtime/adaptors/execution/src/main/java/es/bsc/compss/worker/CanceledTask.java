package es.bsc.compss.worker;

import es.bsc.compss.log.Loggers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CanceledTask {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_INVOKER);

    int taskId;


    public CanceledTask(int taskId) {
        this.taskId = taskId;
    }

    /**
     * Sets the task to cancel.
     */
    public void cancel() {
        LOGGER.info("Task " + taskId + " has been canceled.");
        COMPSsWorker.setCancelled(taskId, 1);
    }
}
