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

import es.bsc.compss.execution.executor.ExecutionSandbox;
import es.bsc.compss.execution.executor.ExecutorContext;
import es.bsc.compss.execution.executor.binding.commands.ExecuteTaskExternalCommand;
import es.bsc.compss.execution.executor.binding.piped.PipedExecutor;
import es.bsc.compss.execution.executor.binding.piped.commands.ExecuteTaskPipeCommand;
import es.bsc.compss.execution.invokers.util.CExecutionCommandGenerator;
import es.bsc.compss.execution.platform.InvocationResources;
import es.bsc.compss.execution.platform.mirrors.CMirror;
import es.bsc.compss.execution.platform.mirrors.ExecutorContextMirror;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import java.util.ArrayList;

public class CInvoker extends PipedInvoker {

    public CInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandbox,
        InvocationResources assignedResources, PipedExecutor executor) throws JobExecutionException {
        super(context, invocation, sandbox, assignedResources, executor);
    }

    @Override
    protected ExecuteTaskExternalCommand getTaskExecutionCommand(InvocationContext context, Invocation invocation,
        String sandBox, InvocationResources assignedResources) {
        ExecuteTaskPipeCommand taskExecution = new ExecuteTaskPipeCommand(invocation.getJobId(), sandBox);
        ArrayList<String> cCommand =
            CExecutionCommandGenerator.getTaskExecutionCommand(context, invocation, sandBox, assignedResources);
        taskExecution.appendAllArguments(cCommand);
        return taskExecution;
    }

    public static ExecutorContextMirror<?> getMirror(ExecutorContext platform) {
        return new CMirror(platform);
    }

}
