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
package es.bsc.compss.invokers.external.persistent;

import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.executor.external.commands.ExecuteTaskExternalCommand;
import es.bsc.compss.invokers.util.CExecutionCommandGenerator;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;


public class CPersistentInvoker extends PersistentInvoker {

    public CPersistentInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandbox,
        InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, sandbox, assignedResources);
    }

    @Override
    protected ExecuteTaskExternalCommand getTaskExecutionCommand(InvocationContext context, Invocation invocation,
        String sandBox, InvocationResources assignedResources) {

        ExecuteTaskExternalCommand command = new ExecuteTaskExternalCommand();
        command.appendAllArguments(
            CExecutionCommandGenerator.getTaskExecutionCommand(context, invocation, sandBox, assignedResources));
        return command;
    }

    @Override
    public void cancelMethod() {
        // TODO: Implement canceling running method
    }
}
