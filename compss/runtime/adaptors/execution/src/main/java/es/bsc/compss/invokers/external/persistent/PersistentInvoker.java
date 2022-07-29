/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.invokers.external.persistent;

import es.bsc.compss.execution.types.ExecutorContext;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.executor.external.persistent.PersistentMirror;
import es.bsc.compss.executor.types.ExternalTaskStatus;
import es.bsc.compss.invokers.external.ExternalInvoker;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import java.io.File;


public abstract class PersistentInvoker extends ExternalInvoker {

    static {
        System.loadLibrary("bindings_common");
    }


    /**
     * Class Constructor.
     * 
     * @param context Invocation Constructor.
     * @param invocation Invocation description.
     * @param taskSandboxWorkingDir Sandbox working directory.
     * @param assignedResources Assigned resources to the invocation.
     * @throws JobExecutionException Exception building the invoker.
     */
    public PersistentInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir,
        InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, taskSandboxWorkingDir, assignedResources);
        super.appendOtherExecutionCommandArguments();
    }

    @Override
    protected void invokeExternalMethod() throws JobExecutionException {
        int jobId = invocation.getJobId();
        String taskCMD = command.getAsString();
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
            throw new JobExecutionException("Job " + jobId + " has failed. Exit values is " + exitValue);
        }

        // Update parameters
        LOGGER.debug("Updating parameters for job " + jobId);
        for (int i = 0; i < taskStatus.getNumParameters(); ++i) {
            DataType paramType = taskStatus.getParameterType(i);
            if (paramType.equals(DataType.EXTERNAL_PSCO_T)) {
                String paramValue = taskStatus.getParameterValue(i);
                invocation.getParams().get(i).setType(DataType.EXTERNAL_PSCO_T);
                invocation.getParams().get(i).setValue(paramValue);
            }
        }

        LOGGER.debug("Job " + jobId + " has finished with exit value 0");
    }

    public static native String executeInBinding(String args);

    public static native void initThread();

    public static native void finishThread();

    public static PersistentMirror getMirror(InvocationContext context, ExecutorContext platform) {
        int threads = platform.getSize();
        return new PersistentMirror(context, threads);
    }
}
