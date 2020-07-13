/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.executor.external.ExternalExecutorException;
import es.bsc.compss.executor.external.piped.PipePair;
import es.bsc.compss.executor.external.piped.commands.CompssExceptionPipeCommand;
import es.bsc.compss.executor.external.piped.commands.EndTaskPipeCommand;
import es.bsc.compss.executor.external.piped.commands.PipeCommand;
import es.bsc.compss.executor.types.InvocationResources;
import es.bsc.compss.invokers.external.ExternalInvoker;
import es.bsc.compss.invokers.types.ExternalTaskStatus;
import es.bsc.compss.invokers.types.TypeValuePair;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamCollection;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.worker.COMPSsException;

import java.io.File;
import java.util.LinkedList;
import java.util.List;


public abstract class PipedInvoker extends ExternalInvoker {

    private final PipePair pipes;


    /**
     * Piped Invoker constructor.
     * 
     * @param context Task execution context
     * @param invocation Task execution description
     * @param taskSandboxWorkingDir Task execution sandbox directory
     * @param assignedResources Assigned resources
     * @param pipes In/Out pipe pair
     * @throws JobExecutionException Error creating the Piped invoker
     */
    public PipedInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir,
        InvocationResources assignedResources, PipePair pipes) throws JobExecutionException {

        super(context, invocation, taskSandboxWorkingDir, assignedResources);
        this.pipes = pipes;
    }

    @Override
    public void invokeMethod() throws JobExecutionException, COMPSsException {
        int jobId = invocation.getJobId();
        if (!pipes.sendCommand((PipeCommand) command)) {
            LOGGER.error("ERROR: Could not execute job " + jobId + " because cannot write in pipe");
            throw new JobExecutionException("Job " + jobId + " has failed. Cannot write in pipe");
        }

        ExternalTaskStatus taskStatus;
        // Process pipe while we are not asked to stop or there are waiting processes
        while (true) {
            try {
                PipeCommand rcvdCommand = pipes.readCommand();
                if (rcvdCommand != null) {
                    switch (rcvdCommand.getType()) {
                        case END_TASK:
                            taskStatus = ((EndTaskPipeCommand) rcvdCommand).getTaskStatus();
                            Integer exitValue = taskStatus.getExitValue();
                            if (exitValue != 0) {
                                throw new JobExecutionException("Job " + jobId + " exit with value " + exitValue);
                            }
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
                        case COMPSS_EXCEPTION:
                            throw new COMPSsException(((CompssExceptionPipeCommand) rcvdCommand).getMessage());
                        default:
                            LOGGER.warn("Unexpected tag on PipedInvoker: " + rcvdCommand + ". Skipping message");
                            break;
                    }
                }
            } catch (ExternalExecutorException e) {
                throw new JobExecutionException("Job " + jobId + "Notification pipe closed", e);
            }
        }
    }

    @SuppressWarnings({ "rawtypes" })
    private void updateParam(InvocationParam param, ExternalTaskStatus taskStatus, int parIdx) {
        DataType paramType = taskStatus.getParameterType(parIdx);
        Object value;
        if (paramType != null && paramType.equals(DataType.EXTERNAL_PSCO_T)) {
            param.setType(paramType);
            value = taskStatus.getParameterValue(parIdx);
            param.setValue(value);
            if (value != null) {
                param.setValueClass(value.getClass());
            }
        } else if (paramType != null && paramType.equals(DataType.COLLECTION_T)) {
            param.setType(paramType);
            InvocationParamCollection ipc = (InvocationParamCollection) param;
            LinkedList<Object> values = taskStatus.getParameterValues(parIdx);
            if (ipc.getCollectionParameters().size() == values.size()) {
                updateParamCollection(ipc, values);
            }
            // otherwise the collection contains null - it was in and has not been changed.
        }
    }

    @SuppressWarnings({ "unchecked",
        "rawtypes" })
    private void updateParamCollection(InvocationParamCollection ipc, LinkedList<Object> values) {
        List<Object> collectionParameters = ipc.getCollectionParameters();
        int position = 0;
        for (Object element : collectionParameters) {
            InvocationParam param = (InvocationParam) element;
            DataType elementType = param.getType();
            if (elementType.equals(DataType.COLLECTION_T)) {
                // The element is an inner collection. Resolve recursively.
                param.setType(elementType);
                updateParamCollection((InvocationParamCollection) param, (LinkedList<Object>) values.get(position));
            } else if (elementType.equals(DataType.EXTERNAL_PSCO_T)) {
                param.setType(elementType);
                TypeValuePair pair = (TypeValuePair) values.get(position);
                if (pair != null) {
                    Object value = pair.getUpdatedParameterValue();
                    param.setValue(value);
                    if (value != null) {
                        param.setValueClass(value.getClass());
                    }
                } else {
                    param.setValue(null);
                }
            } else if (elementType.equals(DataType.FILE_T)) {
                TypeValuePair pair = (TypeValuePair) values.get(position);
                if (pair != null && pair.getUpdatedParameterValue() != null) {
                    param.setType(pair.getUpdatedParameterType());
                    param.setValue(pair.getUpdatedParameterValue());
                    param.setValueClass(pair.getUpdatedParameterValue().getClass());
                }
            }
            position += 1;
        }
    }

    @Override
    public void cancelMethod() {
        this.pipes.getMirror().cancelJob(this.pipes);
    }

}
