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
package es.bsc.compss.invokers.external.piped;

import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.executor.external.ExternalExecutorException;
import es.bsc.compss.executor.external.piped.PipePair;
import es.bsc.compss.executor.external.piped.commands.AccessedFilePipeCommand;
import es.bsc.compss.executor.external.piped.commands.BarrierTaskGroupPipeCommand;
import es.bsc.compss.executor.external.piped.commands.CloseFilePipeCommand;
import es.bsc.compss.executor.external.piped.commands.CloseTaskGroupPipeCommand;
import es.bsc.compss.executor.external.piped.commands.CompssExceptionPipeCommand;
import es.bsc.compss.executor.external.piped.commands.DeleteFilePipeCommand;
import es.bsc.compss.executor.external.piped.commands.DeleteObjectPipeCommand;
import es.bsc.compss.executor.external.piped.commands.EndTaskPipeCommand;
import es.bsc.compss.executor.external.piped.commands.ExecuteNestedTaskPipeCommand;
import es.bsc.compss.executor.external.piped.commands.GetDirectoryPipeCommand;
import es.bsc.compss.executor.external.piped.commands.GetFilePipeCommand;
import es.bsc.compss.executor.external.piped.commands.GetObjectPipeCommand;
import es.bsc.compss.executor.external.piped.commands.NewBarrierPipeCommand;
import es.bsc.compss.executor.external.piped.commands.OpenFilePipeCommand;
import es.bsc.compss.executor.external.piped.commands.OpenTaskGroupPipeCommand;
import es.bsc.compss.executor.external.piped.commands.PipeCommand;
import es.bsc.compss.executor.external.piped.commands.RegisterCEPipeCommand;
import es.bsc.compss.executor.external.piped.commands.SynchPipeCommand;
import es.bsc.compss.executor.types.ExternalTaskStatus;
import es.bsc.compss.invokers.external.ExternalInvoker;
import es.bsc.compss.invokers.types.TypeValuePair;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
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
    // TODO: Possible bug. AppId has no value
    private Long appId;


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
        super.appendOtherExecutionCommandArguments();
        this.pipes = pipes;
    }

    @Override
    public void invokeExternalMethod() throws JobExecutionException, COMPSsException {
        try {
            this.appId = null;
            int jobId = this.invocation.getJobId();
            if (!this.pipes.sendCommand((PipeCommand) this.command)) {
                LOGGER.error("ERROR: Could not execute job " + jobId + " because cannot write in pipe");
                throw new JobExecutionException("Job " + jobId + " has failed. Cannot write in pipe");
            }

            ExternalTaskStatus taskStatus;
            // Process pipe while we are not asked to stop or there are waiting processes
            while (true) {
                try {
                    PipeCommand rcvdCommand = this.pipes.readCommand();
                    if (rcvdCommand != null) {
                        switch (rcvdCommand.getType()) {
                            case REGISTER_CE: {
                                RegisterCEPipeCommand rcpc = (RegisterCEPipeCommand) rcvdCommand;
                                String ceSignature = rcpc.getCESignature();
                                String implSignature = rcpc.getImplSignature();
                                String implConstraints = rcpc.getConstraints();
                                String implType = rcpc.getImplType();
                                String implLocal = rcpc.getImplLocal();
                                String implIO = rcpc.getImplIO();
                                String[] prolog = rcpc.getProlog();
                                String[] epilog = rcpc.getEpilog();
                                String[] implTypeArgs = rcpc.getTypeArgs();
                                this.context.getRuntimeAPI().registerCoreElement(ceSignature, implSignature,
                                    implConstraints, implType, implLocal, implIO, prolog, epilog, implTypeArgs);
                            }
                                break;
                            case EXECUTE_NESTED_TASK: {
                                ExecuteNestedTaskPipeCommand entpc = (ExecuteNestedTaskPipeCommand) rcvdCommand;
                                ExecuteNestedTaskPipeCommand.EntryPoint entryPoint = entpc.getEntryPoint();
                                String onFailure = entpc.getOnFailure();
                                int timeOut = entpc.getTimeOut();
                                boolean isPrioritary = entpc.getPrioritary();
                                boolean hasTarget = entpc.hasTarget();
                                int numReturns = entpc.getNumReturns();
                                int parameterCount = entpc.getParameterCount();
                                Object[] parameters = entpc.getParameters();
                                if (this.appId == null) {
                                    this.appId = this.context.getRuntimeAPI().registerApplication(null, this);
                                    LOGGER.info("Job " + this.invocation.getJobId() + " becomes app " + appId);
                                }
                                int numNodes = entpc.getNumNodes();
                                boolean isReduce = entpc.isReduce();
                                int reduceChunkSize = entpc.getReduceChunkSize();
                                boolean isReplicated = entpc.isReplicated();
                                boolean isDistributed = entpc.isDistributed();
                                if (entryPoint == ExecuteNestedTaskPipeCommand.EntryPoint.SIGNATURE) {
                                    String signature = entpc.getSignature();

                                    this.context.getRuntimeAPI().executeTask(this.appId, signature, onFailure, timeOut,
                                        isPrioritary, numNodes, isReduce, reduceChunkSize, isReplicated, isDistributed,
                                        hasTarget, numReturns, parameterCount, parameters);

                                } else {
                                    String methodClass = entpc.getMethodClass();
                                    String methodName = entpc.getMethodName();
                                    this.context.getRuntimeAPI().executeTask(this.appId, methodClass, onFailure,
                                        timeOut, methodName, isPrioritary, numNodes, isReduce, reduceChunkSize,
                                        isReplicated, isDistributed, hasTarget, numReturns, parameterCount, parameters);
                                }

                            }
                                break;
                            case ACCESSED_FILE: {
                                AccessedFilePipeCommand afpc = (AccessedFilePipeCommand) rcvdCommand;
                                String file = afpc.getFile();
                                if (this.appId == null) {
                                    this.pipes.sendCommand(new SynchPipeCommand("0"));
                                } else {
                                    boolean accessed = this.context.getRuntimeAPI().isFileAccessed(this.appId, file);
                                    this.pipes.sendCommand(new SynchPipeCommand(accessed ? "1" : "0"));
                                }
                            }
                                break;
                            case OPEN_FILE: {
                                OpenFilePipeCommand ofpc = (OpenFilePipeCommand) rcvdCommand;
                                String file = ofpc.getFile();
                                Direction dir = ofpc.getDirection();
                                if (this.appId == null) {
                                    this.pipes.sendCommand(new SynchPipeCommand(file));
                                } else {
                                    String finalLocation = this.context.getRuntimeAPI().openFile(this.appId, file, dir);
                                    this.pipes.sendCommand(new SynchPipeCommand(finalLocation));
                                }
                            }
                                break;
                            case CLOSE_FILE: {
                                CloseFilePipeCommand ofpc = (CloseFilePipeCommand) rcvdCommand;
                                String file = ofpc.getFile();
                                Direction dir = ofpc.getDirection();
                                if (this.appId != null) {
                                    this.context.getRuntimeAPI().closeFile(this.appId, file, dir);
                                }
                            }
                                break;
                            case DELETE_FILE: {
                                DeleteFilePipeCommand ofpc = (DeleteFilePipeCommand) rcvdCommand;
                                String file = ofpc.getFile();
                                boolean val = this.context.getRuntimeAPI().deleteFile(this.appId, file);
                                this.pipes.sendCommand(new SynchPipeCommand(val ? "1" : "0"));
                            }
                                break;
                            case GET_FILE: {
                                GetFilePipeCommand gfpc = (GetFilePipeCommand) rcvdCommand;
                                String file = gfpc.getFile();
                                if (this.appId != null) {
                                    this.context.getRuntimeAPI().getFile(this.appId, file);
                                }
                                this.pipes.sendCommand(new SynchPipeCommand());
                            }
                                break;
                            case GET_DIRECTORY: {
                                GetDirectoryPipeCommand gfpc = (GetDirectoryPipeCommand) rcvdCommand;
                                String file = gfpc.getDirectory();
                                if (this.appId != null) {
                                    context.getRuntimeAPI().getDirectory(this.appId, file);
                                }
                                pipes.sendCommand(new SynchPipeCommand());
                            }
                                break;
                            case GET_OBJECT: {
                                GetObjectPipeCommand gfpc = (GetObjectPipeCommand) rcvdCommand;
                                String id = gfpc.getObjectId();
                                if (this.appId != null) {
                                    this.context.getRuntimeAPI().getBindingObject(this.appId, id);
                                }
                                this.pipes.sendCommand(new SynchPipeCommand());
                            }
                                break;
                            case DELETE_OBJECT: {
                                DeleteObjectPipeCommand ofpc = (DeleteObjectPipeCommand) rcvdCommand;
                                String id = ofpc.getObjectId();
                                boolean val = this.context.getRuntimeAPI().deleteFile(this.appId, id);
                                this.pipes.sendCommand(new SynchPipeCommand(val ? "1" : "0"));
                            }
                                break;
                            case BARRIER: {
                                if (this.appId != null) {
                                    this.context.getRuntimeAPI().barrier(this.appId);
                                }
                                this.pipes.sendCommand(new SynchPipeCommand());
                            }
                                break;
                            case BARRIER_NEW: {
                                NewBarrierPipeCommand nbpc = (NewBarrierPipeCommand) rcvdCommand;
                                boolean noMoreTasks = nbpc.isNoMoreTasks();
                                if (this.appId != null) {
                                    this.context.getRuntimeAPI().barrier(this.appId, noMoreTasks);
                                }
                                this.pipes.sendCommand(new SynchPipeCommand());
                            }
                                break;
                            case BARRIER_GROUP: {
                                BarrierTaskGroupPipeCommand bgpc = (BarrierTaskGroupPipeCommand) rcvdCommand;
                                String groupName = bgpc.getGroupName();
                                boolean synch = true;
                                if (this.appId != null) {
                                    try {
                                        this.context.getRuntimeAPI().barrierGroup(appId, groupName);
                                    } catch (COMPSsException ce) {
                                        this.pipes.sendCommand(new CompssExceptionPipeCommand(null, ce.getMessage()));
                                        synch = false;
                                    }
                                }
                                if (synch) {
                                    this.pipes.sendCommand(new SynchPipeCommand());
                                }
                            }
                                break;
                            case OPEN_TASK_GROUP: {
                                OpenTaskGroupPipeCommand otgpc = (OpenTaskGroupPipeCommand) rcvdCommand;
                                String groupName = otgpc.getGroupName();
                                boolean barrier = otgpc.isImplicitBarrier();
                                if (this.appId == null) {
                                    this.appId = this.context.getRuntimeAPI().registerApplication(null, this);
                                    LOGGER.info("Job " + this.invocation.getJobId() + " becomes app " + appId);
                                }
                                this.context.getRuntimeAPI().openTaskGroup(groupName, barrier, this.appId);

                            }
                                break;
                            case CLOSE_TASK_GROUP: {
                                CloseTaskGroupPipeCommand otgpc = (CloseTaskGroupPipeCommand) rcvdCommand;
                                String groupName = otgpc.getGroupName();
                                if (this.appId != null) {
                                    this.context.getRuntimeAPI().closeTaskGroup(groupName, this.appId);
                                }
                            }
                                break;
                            case NO_MORE_TASKS: {
                                if (this.appId != null) {
                                    this.context.getRuntimeAPI().noMoreTasks(this.appId);
                                }
                                this.pipes.sendCommand(new SynchPipeCommand());
                            }
                                break;
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
        } catch (COMPSsException | JobExecutionException | RuntimeException e) {
            throw e;
        } finally {
            if (this.appId != null) {
                this.context.getRuntimeAPI().deregisterApplication(appId);
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
        } else {
            if (paramType != null && paramType.equals(DataType.COLLECTION_T)) {
                param.setType(paramType);
                InvocationParamCollection ipc = (InvocationParamCollection) param;
                LinkedList<Object> values = taskStatus.getParameterValues(parIdx);
                if (ipc.getCollectionParameters().size() == values.size()) {
                    updateParamCollection(ipc, values);
                }
                // otherwise the collection contains null - it was in and has not been changed.
            }
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
            } else {
                if (elementType.equals(DataType.EXTERNAL_PSCO_T)) {
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
                } else {
                    if (elementType.equals(DataType.FILE_T)) {
                        TypeValuePair pair = (TypeValuePair) values.get(position);
                        if (pair != null && pair.getUpdatedParameterValue() != null) {
                            param.setType(pair.getUpdatedParameterType());
                            param.setValue(pair.getUpdatedParameterValue());
                            param.setValueClass(pair.getUpdatedParameterValue().getClass());
                        }
                    }
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
