package es.bsc.compss.worker;

import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.log.Loggers;

public class TimeOutTask extends TimerTask {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_INVOKER);

    int taskId;
    
    public TimeOutTask(int taskId) {
        this.taskId = taskId;
    }
    
    @Override
    public void run() {
        LOGGER.info("The task " + taskId + " timed out");
        COMPSsWorker.setCancelled(taskId);

    }

}

