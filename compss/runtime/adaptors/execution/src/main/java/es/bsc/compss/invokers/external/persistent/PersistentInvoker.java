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

import es.bsc.compss.execution.types.ExecutorContext;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.executor.external.persistent.PersistentMirror;
import es.bsc.compss.executor.types.ExternalTaskStatus;
import es.bsc.compss.executor.types.ParameterResult;
import es.bsc.compss.invokers.external.ExternalInvoker;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamCollection;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import java.util.Collection;
import java.util.Iterator;


public abstract class PersistentInvoker extends ExternalInvoker {

    static {
        System.loadLibrary("bindings_common");
    }


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
        super.appendOtherExecutionCommandArguments();
    }

    @Override
    protected void invokeExternalMethod() throws JobExecutionException {
        int jobId = invocation.getJobId();
        String taskCMD = command.getAsString();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Executing in binding: " + taskCMD);
        }

        String result = executeInBinding(taskCMD);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Result: " + result);
        }
        ExternalTaskStatus taskStatus = new ExternalTaskStatus(result.split(" "));

        // Check task exit value
        Integer exitValue = taskStatus.getExitValue();
        if (exitValue != 0) {
            throw new JobExecutionException("Job " + jobId + " has failed. Exit values is " + exitValue);
        }

        // Update parameters
        LOGGER.debug("Updating parameters for job " + jobId);
        Collection<ParameterResult> results = taskStatus.getResults();

        Iterator<? extends InvocationParam> cip = invocation.getParams().iterator();
        Iterator<ParameterResult> cr = results.iterator();
        while (cr.hasNext()) {
            updateParam(cip.next(), cr.next());
        }

        LOGGER.debug("Job " + jobId + " has finished with exit value 0");
    }

    private void updateParam(InvocationParam param, ParameterResult result) {
        if (param.isCollective()) {
            Iterator<InvocationParam> cip = ((InvocationParamCollection) param).getCollectionParameters().iterator();
            Iterator<ParameterResult> cr = ((ParameterResult.CollectiveResult) param).getElements().iterator();
            while (cip.hasNext()) {
                updateParam(cip.next(), cr.next());
            }
        }

        DataType resultType = result.getType();
        if (resultType != null) {
            if (resultType == DataType.EXTERNAL_PSCO_T) {
                param.setType(resultType);
                param.setValue(((ParameterResult.SingleResult) result).getValue());
            }
        }

    }

    public static native String executeInBinding(String args);

    public static native void initThread();

    public static native void finishThread();

    public static PersistentMirror getMirror(InvocationContext context, ExecutorContext platform) {
        int threads = platform.getSize();
        return new PersistentMirror(context, threads);
    }
}
