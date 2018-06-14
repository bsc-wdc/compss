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
package es.bsc.compss.nio.worker.executors;

import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.exceptions.JobExecutionException;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.nio.worker.util.ExternalTaskStatus;
import es.bsc.compss.nio.worker.util.JobsThreadPool;
import es.bsc.compss.nio.worker.util.TaskResultReader;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.RequestQueue;

import java.io.FileOutputStream;
import java.util.concurrent.Semaphore;


public abstract class ExternalExecutor extends AbstractExternalExecutor {

    private static final String ERROR_PIPE_CLOSE = "Error on closing pipe ";
    private static final String ERROR_PIPE_QUIT = "Error sending quit to pipe ";

    private final String writePipe; // Pipe for sending executions
    private final TaskResultReader taskResultReader; // Process result reader (initialized by PoolManager,
                                                     // started/stopped by us)


    public ExternalExecutor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue, String writePipe,
            TaskResultReader resultReader) {

        super(nw, pool, queue);

        this.writePipe = writePipe;
        this.taskResultReader = resultReader;

        if (NIOTracer.isActivated()) {
            NIOTracer.disablePThreads();
        }
        // Start task Reader
        this.taskResultReader.start();

        if (NIOTracer.isActivated()) {
            NIOTracer.enablePThreads();
        }
    }

    @Override
    public void start() {
        // Nothing to do
        LOGGER.info("ExternalExecutor started");
    }

    @Override
    public void finish() {
        LOGGER.info("Finishing ExternalExecutor");

        // Send quit tag to pipe
        LOGGER.debug("Send quit tag to pipe " + writePipe);
        boolean done = false;
        int retries = 0;
        while (!done && retries < MAX_RETRIES) {
            FileOutputStream output = null;
            try {
                output = new FileOutputStream(writePipe, true);
                String quitCMD = QUIT_TAG + TOKEN_NEW_LINE;
                output.write(quitCMD.getBytes());
                output.flush();
            } catch (Exception e) {
                LOGGER.warn("Error on writing on pipe " + writePipe + ". Retrying " + retries + "/" + MAX_RETRIES);
                ++retries;
            } finally {
                if (output != null) {
                    try {
                        output.close();
                    } catch (Exception e) {
                        ErrorManager.error(ERROR_PIPE_CLOSE + writePipe, e);
                    }
                }
            }
            done = true;
        }
        if (!done) {
            ErrorManager.error(ERROR_PIPE_QUIT + writePipe);
        }

        // ------------------------------------------------------
        // Ask TaskResultReader to stop and wait for it to finish
        LOGGER.debug("Waiting for TaskResultReader");
        Semaphore sem = new Semaphore(0);
        taskResultReader.shutdown(sem);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        LOGGER.info("End Finishing ExternalExecutor");
    }

    @Override
    protected void executeExternal(int jobId, String command, NIOTask nt, NIOWorker nw) throws JobExecutionException {
        // Emit start task trace
        int taskType = nt.getTaskType() + 1; // +1 Because Task ID can't be 0 (0 signals end task)
        int taskId = nt.getTaskId();

        if (NIOTracer.isActivated()) {
            emitStartTask(taskId, taskType);
        }

        LOGGER.debug("Starting job process ...");
        // Send executeTask tag to pipe
        boolean done = false;
        int retries = 0;
        while (!done && retries < MAX_RETRIES) {
            // Send to pipe : task tID command(jobOut jobErr externalCMD) \n
            String taskCMD = EXECUTE_TASK_TAG + TOKEN_SEP + jobId + TOKEN_SEP + command + TOKEN_NEW_LINE;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("EXECUTOR COMMAND: " + taskCMD);
            }

            try (FileOutputStream output = new FileOutputStream(writePipe, true);) {
                output.write(taskCMD.getBytes());
                output.flush();
                output.close();
                done = true;
            } catch (Exception e) {
                LOGGER.debug("Error on pipe write. Retry");
                ++retries;
            }
        }

        if (!done) {
            if (NIOTracer.isActivated()) {
                emitEndTask();
            }
            LOGGER.error("ERROR: Could not execute job " + jobId + " because cannot write in pipe");
            throw new JobExecutionException("Job " + jobId + " has failed. Cannot write in pipe");
        }

        // Retrieving job result
        LOGGER.debug("Waiting for job " + jobId + " completion");
        Semaphore sem = new Semaphore(0);
        taskResultReader.askForTaskEnd(jobId, sem);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOGGER.debug("Job " + jobId + " completed. Retrieving task result");
        ExternalTaskStatus taskStatus = taskResultReader.getTaskStatus(jobId);

        // Check task exit value
        Integer exitValue = taskStatus.getExitValue();
        if (exitValue != 0) {
            if (NIOTracer.isActivated()) {
                emitEndTask();
            }
            throw new JobExecutionException("Job " + jobId + " has failed. Exit values is " + exitValue);
        }
        // Update parameters
        LOGGER.debug("Updating parameters for job " + jobId);
        for (int i = 0; i < taskStatus.getNumParameters(); ++i) {
            DataType paramType = taskStatus.getParameterType(i);
            if (paramType.equals(DataType.EXTERNAL_PSCO_T)) {
                String paramValue = taskStatus.getParameterValue(i);
                nt.getParams().get(i).setType(DataType.EXTERNAL_PSCO_T);
                nt.getParams().get(i).setValue(paramValue);
            }
        }

        // Emit end task trace
        if (NIOTracer.isActivated()) {
            emitEndTask();
        }
        LOGGER.debug("Job " + jobId + " has finished with exit value 0");
    }

}
