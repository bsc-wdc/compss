/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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

package es.bsc.compss.execution.invokers.binding.piped;

import es.bsc.compss.execution.Invocation;
import es.bsc.compss.execution.InvocationContext;
import es.bsc.compss.execution.InvocationParam;
import es.bsc.compss.execution.exceptions.JobExecutionException;
import es.bsc.compss.execution.executor.ExecutionSandbox;
import es.bsc.compss.execution.executor.binding.commands.ExecuteTaskExternalCommand;
import es.bsc.compss.execution.executor.binding.piped.PipedExecutor;
import es.bsc.compss.execution.executor.binding.piped.commands.ExecuteTaskPipeCommand;
import es.bsc.compss.execution.platform.InvocationResources;
import java.util.List;

public class PythonInvoker extends PipedInvoker {

    public PythonInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandbox,
        InvocationResources assignedResources, PipedExecutor pExec) throws JobExecutionException {

        super(context, invocation, sandbox, assignedResources, pExec);
    }

    @Override
    protected ExecuteTaskExternalCommand getTaskExecutionCommand(InvocationContext context, Invocation invocation,
        String sandBox, InvocationResources assignedResources) {

        ExecuteTaskPipeCommand taskExecution = new ExecuteTaskPipeCommand(invocation.getJobId(), sandBox);
        return taskExecution;
    }

    // The logging is done in the worker.py Python worker
    protected void logProvenanceOfParameters(List<? extends InvocationParam> params, String direction) {
    }

}
