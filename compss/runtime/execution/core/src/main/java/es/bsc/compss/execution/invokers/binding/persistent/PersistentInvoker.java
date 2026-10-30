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
package es.bsc.compss.execution.invokers.binding.persistent;

import es.bsc.compss.execution.executor.ExecutionSandbox;
import es.bsc.compss.execution.executor.binding.persistent.PersistentCBinding;
import es.bsc.compss.execution.invokers.binding.BindingInvoker;
import es.bsc.compss.execution.invokers.binding.ExternalTaskStatus;
import es.bsc.compss.execution.platform.InvocationResources;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;

/**
 * Base class of the invokers that run tasks inside the persistent binding implemented in C: the task command is sent
 * through {@link PersistentCBinding} to the binding of the calling executor thread, and the reported task status drives
 * the shared result handling of the binding invokers.
 */
public abstract class PersistentInvoker extends BindingInvoker {

    /**
     * Class Constructor.
     * 
     * @param context Invocation Constructor.
     * @param invocation Invocation description.
     * @param sandbox Sandbox working directory.
     * @param assignedResources Assigned resources to the invocation.
     * @throws JobExecutionException Exception building the invoker.
     */
    public PersistentInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandbox,
        InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, sandbox, assignedResources);
    }

    @Override
    protected void invokeExternalMethod() throws JobExecutionException {
        String taskCMD = command.getAsString();
        if (logger.isDebugEnabled()) {
            logger.debug("Executing in binding: {}", taskCMD);
        }

        String result = PersistentCBinding.executeInBinding(taskCMD);
        if (logger.isDebugEnabled()) {
            logger.debug("Result: {}", result);
        }
        ExternalTaskStatus taskStatus = new ExternalTaskStatus(result.split(" "));

        // Check task exit value
        checkExitValue(taskStatus.getExitValue());

        // Update parameters
        applyTaskResults(taskStatus);

        logger.debug("Job {} has finished with exit value 0", invocation.getJobId());
    }

    /**
     * Cancellation of the running method. Persistent jobs run as synchronous native calls on the executor thread, so
     * there is nothing that can be canceled from the outside; the request is a no-op.
     */
    @Override
    public void cancelMethod() {
        // The job runs as a synchronous native call on the executor thread: can't be canceled from the outside
    }

}
