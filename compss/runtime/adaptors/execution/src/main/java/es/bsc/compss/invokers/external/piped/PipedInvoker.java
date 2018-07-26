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
package es.bsc.compss.invokers.external.piped;

import es.bsc.compss.executor.utils.PipedMirror.PipePair;
import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.invokers.external.ExternalCommand;
import es.bsc.compss.invokers.external.ExternalInvoker;
import es.bsc.compss.invokers.external.piped.PipeCommand.EndTaskPipeCommand;
import es.bsc.compss.invokers.external.piped.PipeCommand.ErrorTaskPipeCommand;
import es.bsc.compss.invokers.types.ExternalTaskStatus;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import java.io.File;
import java.util.LinkedList;


/**
 *
 * @author flordan
 */
public abstract class PipedInvoker extends ExternalInvoker {

    private final PipePair pipes;

    public PipedInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir, InvocationResources assignedResources, PipePair pipes)
            throws JobExecutionException {
        super(context, invocation, taskSandboxWorkingDir, assignedResources);
        this.pipes = pipes;
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        boolean send = pipes.send((PipeCommand) command);
        if (!send) {
            int jobId = invocation.getJobId();
            LOGGER.error("ERROR: Could not execute job " + jobId + " because cannot write in pipe");
            throw new JobExecutionException("Job " + jobId + " has failed. Cannot write in pipe");
        }

        ExternalTaskStatus taskStatus;
        // Process pipe while we are not asked to stop or there are waiting processes
        while (true) {
            PipeCommand rcvdCommand = pipes.read();
            if (rcvdCommand != null) {
                if (ExternalCommand.ERROR_TASK.equals(rcvdCommand.getType())) {
                    taskStatus = ((ErrorTaskPipeCommand) rcvdCommand).getTaskStatus();
                    Integer exitValue = taskStatus.getExitValue();
                    throw new JobExecutionException("Exit values is " + exitValue);
                }
                if (ExternalCommand.END_TASK.equals(rcvdCommand.getType())) {
                    taskStatus = ((EndTaskPipeCommand) rcvdCommand).getTaskStatus();
                    // Update parameters
                    LOGGER.debug("Updating parameters for job " + this.invocation.getJobId());
                    int parIdx = 0;
                    for (InvocationParam param : this.invocation.getParams()) {
                        updateParam(param, taskStatus, parIdx);
                        parIdx++;
                    }
                    InvocationParam target = this.invocation.getTarget();
                    if (target != null) {
                        updateParam(target, taskStatus, parIdx);
                        parIdx++;
                    }
                    for (InvocationParam param : this.invocation.getResults()) {
                        updateParam(param, taskStatus, parIdx);
                        parIdx++;
                    }
                    return;
                }
            }
        }
    }

    private void updateParam(InvocationParam param, ExternalTaskStatus taskStatus, int parIdx) {
        DataType paramType = taskStatus.getParameterType(parIdx);
        Object value;
        if (paramType.equals(DataType.EXTERNAL_PSCO_T)) {
            param.setType(paramType);
            value = taskStatus.getParameterValue(parIdx);
            param.setValue(value);
            if (value != null) {
                param.setValueClass(value.getClass());
            }
        }

    }
}
