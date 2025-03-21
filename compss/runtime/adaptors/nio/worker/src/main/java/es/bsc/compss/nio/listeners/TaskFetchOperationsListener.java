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
package es.bsc.compss.nio.listeners;

import es.bsc.compss.data.MultiOperationFetchListener;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.worker.NIOWorker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TaskFetchOperationsListener extends MultiOperationFetchListener {

    private static final Logger TIMER_LOGGER = LogManager.getLogger(Loggers.TIMER);

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
        // Get transfer times if needed
        if (NIOWorker.IS_TIMER_COMPSS_ENABLED) {
            final long transferStartTime = this.nw.getTransferStartTime(this.task.getJobId());
            final long transferEndTime = System.nanoTime();
            final float transferElapsedTime = (transferEndTime - transferStartTime) / (float) 1_000_000;
            TIMER_LOGGER
                .info("[TIMER] Transfers for task " + this.task.getJobId() + ": " + transferElapsedTime + " ms");
        }

        // Execute task
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
        String errorMessage = "Job failed because the data " + failedDataId + " couldn't be retrieved. "
            + "Check worker logs for more information about the error.";
        String taskFileOutName = baseJobPath + ".out";
        this.nw.checkStreamFileExistence(taskFileOutName, "out", errorMessage, cause);
        String taskFileErrName = baseJobPath + ".err";
        this.nw.checkStreamFileExistence(taskFileErrName, "err", errorMessage, cause);
        this.nw.sendTaskDone(task, false, cause);
    }

}
