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

import es.bsc.compss.api.Workflow;
import es.bsc.compss.exceptions.WorkflowException;
import es.bsc.compss.execution.Invocation;
import es.bsc.compss.execution.InvocationContext;
import es.bsc.compss.execution.exceptions.JobExecutionException;
import es.bsc.compss.execution.executor.ExecutionSandbox;
import es.bsc.compss.execution.executor.binding.CommandChannelException;
import es.bsc.compss.execution.executor.binding.piped.PipePair;
import es.bsc.compss.execution.executor.binding.piped.PipedExecutor;
import es.bsc.compss.execution.executor.binding.piped.commands.AccessedFilePipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.BarrierTaskGroupPipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.CancelTaskGroupPipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.CloseFilePipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.CloseTaskGroupPipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.CompssExceptionPipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.DeleteFilePipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.DeleteObjectPipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.EndTaskPipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.ExecuteNestedTaskPipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.GetAvailableFilesPipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.GetDirectoryPipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.GetFilePipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.GetObjectPipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.NewBarrierPipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.OpenFilePipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.OpenTaskGroupPipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.PipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.RegisterCEPipeCommand;
import es.bsc.compss.execution.executor.binding.piped.commands.SynchPipeCommand;
import es.bsc.compss.execution.invokers.binding.BindingInvoker;
import es.bsc.compss.execution.invokers.binding.ExternalTaskStatus;
import es.bsc.compss.execution.platform.InvocationResources;

public abstract class PipedInvoker extends BindingInvoker {

    private final PipedExecutor executor;
    private final PipePair pipes;


    /**
     * Piped Invoker constructor.
     *
     * @param context Task execution context
     * @param invocation Task execution description
     * @param sandbox Task execution sandbox directory
     * @param assignedResources Assigned resources
     * @param executor Handle to the piped executor that runs the invocations
     * @throws JobExecutionException Error creating the Piped invoker
     */
    public PipedInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandbox,
        InvocationResources assignedResources, PipedExecutor executor) throws JobExecutionException {

        super(context, invocation, sandbox, assignedResources);
        this.executor = executor;
        this.pipes = executor.getPipePair();
    }

    @Override
    public void invokeExternalMethod() throws JobExecutionException, WorkflowException {
        Workflow wf = null;
        try {
            int jobId = this.invocation.getJobId();
            if (!this.pipes.sendCommand((PipeCommand) this.command)) {
                logger.error("ERROR: Could not execute job {} because cannot write in pipe", jobId);
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
                                String[] container = rcpc.getConatainer();
                                String[] implTypeArgs = rcpc.getTypeArgs();
                                this.context.getRuntimeAPI().registerCoreElement(ceSignature, implSignature,
                                    implConstraints, implType, implLocal, implIO, prolog, epilog, container,
                                    implTypeArgs);
                            }
                                break;
                            case EXECUTE_NESTED_TASK: {
                                ExecuteNestedTaskPipeCommand entpc = (ExecuteNestedTaskPipeCommand) rcvdCommand;
                                ExecuteNestedTaskPipeCommand.EntryPoint entryPoint = entpc.getEntryPoint();
                                byte onFailure = entpc.getOnFailure();
                                int timeOut = entpc.getTimeOut();
                                boolean isPrioritary = entpc.getPrioritary();
                                boolean hasTarget = entpc.hasTarget();
                                int numReturns = entpc.getNumReturns();
                                int parameterCount = entpc.getParameterCount();
                                Object[] parameters = entpc.getParameters();
                                if (wf == null) {
                                    wf = becomesNestedApplication(null);
                                }
                                int numNodes = entpc.getNumNodes();
                                boolean isReduce = entpc.isReduce();
                                int reduceChunkSize = entpc.getReduceChunkSize();
                                boolean isReplicated = entpc.isReplicated();
                                boolean isDistributed = entpc.isDistributed();
                                String signature = entpc.getSignature();

                                wf.executeTask(signature, onFailure, timeOut, isPrioritary, numNodes, isReduce,
                                    reduceChunkSize, isReplicated, isDistributed, hasTarget, numReturns, parameterCount,
                                    parameters);
                            }
                                break;
                            case ACCESSED_FILE: {
                                AccessedFilePipeCommand afpc = (AccessedFilePipeCommand) rcvdCommand;
                                String file = afpc.getFile();
                                if (wf == null) {
                                    this.pipes.sendCommand(new SynchPipeCommand("0"));
                                } else {
                                    boolean accessed = wf.isFileAccessed(file);
                                    this.pipes.sendCommand(new SynchPipeCommand(accessed ? "1" : "0"));
                                }
                            }
                                break;
                            case OPEN_FILE: {
                                OpenFilePipeCommand ofpc = (OpenFilePipeCommand) rcvdCommand;
                                String file = ofpc.getFile();
                                byte dir = ofpc.getAccessMode();
                                if (wf == null) {
                                    this.pipes.sendCommand(new SynchPipeCommand(file));
                                } else {
                                    String finalLocation = wf.openFile(file, dir);
                                    this.pipes.sendCommand(new SynchPipeCommand(finalLocation));
                                }
                            }
                                break;
                            case CLOSE_FILE: {
                                CloseFilePipeCommand ofpc = (CloseFilePipeCommand) rcvdCommand;
                                String file = ofpc.getFile();
                                byte dir = ofpc.getAccessMode();
                                if (wf != null) {
                                    wf.closeFile(file, dir);
                                }
                            }
                                break;
                            case DELETE_FILE: {
                                DeleteFilePipeCommand ofpc = (DeleteFilePipeCommand) rcvdCommand;
                                String file = ofpc.getFile();
                                boolean val = wf.deleteFile(file, true, true);
                                this.pipes.sendCommand(new SynchPipeCommand(val ? "1" : "0"));
                            }
                                break;
                            case GET_FILE: {
                                GetFilePipeCommand gfpc = (GetFilePipeCommand) rcvdCommand;
                                String file = gfpc.getFile();
                                if (wf != null) {
                                    wf.getFile(file);
                                }
                                this.pipes.sendCommand(new SynchPipeCommand());
                            }
                                break;
                            case GET_AVAILABLE_FILES: {
                                GetAvailableFilesPipeCommand gafpc = (GetAvailableFilesPipeCommand) rcvdCommand;
                                String[] files = gafpc.getFileNames();
                                int[] available;
                                if (wf != null) {
                                    available = wf.waitForAvailableFiles(files);
                                } else {
                                    available = new int[0];
                                }
                                StringBuilder sb = new StringBuilder();
                                for (int idx : available) {
                                    if (sb.length() > 0) {
                                        sb.append(" ");
                                    }
                                    sb.append(idx);
                                }
                                this.pipes.sendCommand(new SynchPipeCommand(sb.toString()));
                            }
                                break;
                            case GET_DIRECTORY: {
                                GetDirectoryPipeCommand gfpc = (GetDirectoryPipeCommand) rcvdCommand;
                                String file = gfpc.getDirectory();
                                if (wf != null) {
                                    wf.getDirectory(file);
                                }
                                pipes.sendCommand(new SynchPipeCommand());
                            }
                                break;
                            case GET_OBJECT: {
                                GetObjectPipeCommand gfpc = (GetObjectPipeCommand) rcvdCommand;
                                String id = gfpc.getObjectId();
                                if (wf != null) {
                                    wf.getBindingObject(id);
                                }
                                this.pipes.sendCommand(new SynchPipeCommand());
                            }
                                break;
                            case DELETE_OBJECT: {
                                DeleteObjectPipeCommand ofpc = (DeleteObjectPipeCommand) rcvdCommand;
                                String id = ofpc.getObjectId();
                                boolean val = wf.deleteFile(id, true, true);
                                this.pipes.sendCommand(new SynchPipeCommand(val ? "1" : "0"));
                            }
                                break;
                            case BARRIER: {
                                if (wf != null) {
                                    wf.barrier();
                                }
                                this.pipes.sendCommand(new SynchPipeCommand());
                            }
                                break;
                            case BARRIER_NEW: {
                                NewBarrierPipeCommand nbpc = (NewBarrierPipeCommand) rcvdCommand;
                                boolean noMoreTasks = nbpc.isNoMoreTasks();
                                if (wf != null) {
                                    wf.barrier(noMoreTasks);
                                }
                                this.pipes.sendCommand(new SynchPipeCommand());
                            }
                                break;
                            case BARRIER_GROUP: {
                                BarrierTaskGroupPipeCommand bgpc = (BarrierTaskGroupPipeCommand) rcvdCommand;
                                String groupName = bgpc.getGroupName();
                                boolean synch = true;
                                if (wf != null) {
                                    try {
                                        wf.barrierGroup(groupName);
                                    } catch (WorkflowException ce) {
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
                                if (wf == null) {
                                    wf = becomesNestedApplication(null);
                                    long appId = wf.getId();
                                    logger.info("Job {} becomes app {}", this.invocation.getJobId(), appId);
                                }
                                wf.openTaskGroup(groupName, barrier);

                            }
                                break;
                            case CLOSE_TASK_GROUP: {
                                CloseTaskGroupPipeCommand otgpc = (CloseTaskGroupPipeCommand) rcvdCommand;
                                String groupName = otgpc.getGroupName();
                                if (wf != null) {
                                    wf.closeTaskGroup(groupName);
                                }
                            }
                                break;
                            case CANCEL_TASK_GROUP: {
                                CancelTaskGroupPipeCommand otgpc = (CancelTaskGroupPipeCommand) rcvdCommand;
                                String groupName = otgpc.getGroupName();
                                if (wf != null) {
                                    wf.cancelTaskGroup(groupName);
                                }
                            }
                                break;
                            case NO_MORE_TASKS: {
                                if (wf != null) {
                                    wf.noMoreTasks();
                                }
                                this.pipes.sendCommand(new SynchPipeCommand());
                            }
                                break;
                            case END_TASK:
                                taskStatus = ((EndTaskPipeCommand) rcvdCommand).getTaskStatus();
                                checkExitValue(taskStatus.getExitValue());
                                applyTaskResults(taskStatus);
                                return;
                            case COMPSS_EXCEPTION:
                                throw new WorkflowException(((CompssExceptionPipeCommand) rcvdCommand).getMessage());
                            default:
                                logger.warn("Unexpected tag on PipedInvoker: {}. Skipping message", rcvdCommand);
                                break;
                        }
                    }
                } catch (CommandChannelException e) {
                    throw new JobExecutionException("Job " + jobId + "Notification pipe closed", e);
                }
            }
        } catch (WorkflowException | JobExecutionException | RuntimeException e) {
            throw e;
        } finally {
            if (wf != null) {
                completeNestedApplication(wf);
            }
        }

    }

    @Override
    public void cancelMethod() {
        this.executor.getMirror().cancelJob(this.executor);
    }

}
