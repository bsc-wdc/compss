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
package es.bsc.compss.invokers.external.persistent;

import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.invokers.external.ExternalInvoker;
import es.bsc.compss.invokers.types.ExternalTaskStatus;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import java.io.File;
import java.util.LinkedList;
import java.util.List;


/**
 *
 * @author flordan
 */
public abstract class PersistentInvoker extends ExternalInvoker {

    static {
        System.loadLibrary("bindings_common");
    }

    public PersistentInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir, InvocationResources assignedResources) throws JobExecutionException {
        super(context, invocation, taskSandboxWorkingDir, assignedResources);
    }

    @Override
    protected void invokeMethod() throws JobExecutionException {
        int jobId = invocation.getJobId();
        String taskCMD = /*EXECUTE_TASK + TOKEN_SEP + jobId + TOKEN_SEP + command + TOKEN_NEW_LINE;*/ "";
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
        for (InvocationParam np : this.invocation.getResults()) {
            np.setValue(exitValue);
            np.setValueClass(exitValue.getClass());
        }
    }

    public static native String executeInBinding(String args);
}
