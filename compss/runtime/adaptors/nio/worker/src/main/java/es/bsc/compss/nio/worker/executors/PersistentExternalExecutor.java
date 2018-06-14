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
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.util.RequestQueue;


public abstract class PersistentExternalExecutor extends AbstractExternalExecutor {

    static {
        System.loadLibrary("bindings_common");
    }


    public PersistentExternalExecutor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue) {
        super(nw, pool, queue);
    }

    public static native String executeInBinding(String args);

    public static native void initThread();

    public static native void finishThread();

    @Override
    public void finish() {
        LOGGER.info("Finishing Persistent Executor");
        finishThread();
        LOGGER.info("End Finishing ExternalExecutor");
    }

    @Override
    public void start() {
        LOGGER.info("Starting Persistent Executor");
        initThread();
        LOGGER.info("Persistent Executor started");
    }

    @Override
    protected void executeExternal(int jobId, String command, NIOTask nt, NIOWorker nw) throws JobExecutionException {
        // Emit start task trace
        int taskType = nt.getTaskType() + 1; // +1 Because Task ID can't be 0 (0 signals end task)
        int taskId = nt.getTaskId();

        if (NIOTracer.isActivated()) {
            emitStartTask(taskId, taskType);
        }

        String taskCMD = EXECUTE_TASK_TAG + TOKEN_SEP + jobId + TOKEN_SEP + command + TOKEN_NEW_LINE;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Executing in binding: " + taskCMD);
        }

        String results = executeInBinding(taskCMD);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Result: " + results);
        }
        ExternalTaskStatus taskStatus = new ExternalTaskStatus(results.split(" "));

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
