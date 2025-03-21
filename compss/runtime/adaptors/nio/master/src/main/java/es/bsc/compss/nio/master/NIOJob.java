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
package es.bsc.compss.nio.master;

import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOResult;
import es.bsc.compss.nio.NIOResultCollection;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.NIOTaskResult;
import es.bsc.compss.nio.NIOUri;
import es.bsc.compss.nio.master.utils.NIOParamFactory;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.implementations.definition.MultiNodeDefinition;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.types.job.JobHistory;
import es.bsc.compss.types.job.JobImpl;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.CollectiveParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.worker.COMPSsException;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


public class NIOJob extends JobImpl<NIOWorkerNode> {

    protected final List<String> slaveWorkersNodeNames;


    /**
     * Creates a new NIOJob instance.
     *
     * @param taskId Associated task id.
     * @param taskParams Task parameters.
     * @param impl Task implementation.
     * @param res Resource
     * @param slaveWorkersNodeNames Associated slave nodes.
     * @param listener Listener.
     */
    public NIOJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res,
        List<String> slaveWorkersNodeNames, JobListener listener, List<Integer> predecessors, Integer numSuccessors) {

        super(taskId, taskParams, impl, res, listener, predecessors, numSuccessors);
        this.slaveWorkersNodeNames = slaveWorkersNodeNames;
    }

    @Override
    public TaskType getType() {
        return TaskType.METHOD;
    }

    @Override
    public void submitJob() throws Exception {
        // Prepare the job
        LOGGER.info("Submit NIOJob with ID " + jobId);
        NIOAdaptor.submitTask(this);
    }

    @Override
    public void cancelJob() throws Exception {
        // Prepare the job
        LOGGER.info("Stopping NIOJob with ID " + jobId);
        NIOAdaptor.cancelTask(this);
    }

    /**
     * Creates a new Task with the associated job parameters.
     *
     * @return The new task for the current job.
     */
    public NIOTask createNIOTask() {
        AbstractMethodImplementation absMethodImpl = (AbstractMethodImplementation) this.impl;

        // If it is a native method, check that methodname is defined (otherwise define it from job parameters)
        // This is a workaround for Python
        switch (absMethodImpl.getMethodType()) {
            case METHOD:
                MethodDefinition methodImpl = (MethodDefinition) absMethodImpl.getDescription().getDefinition();
                String methodName = methodImpl.getAlternativeMethodName();
                if (methodName == null || methodName.isEmpty()) {
                    methodImpl.setAlternativeMethodName(this.taskParams.getName());
                }
                break;
            case MULTI_NODE:
                MultiNodeDefinition multiNodeImpl =
                    (MultiNodeDefinition) absMethodImpl.getDescription().getDefinition();
                String multiNodeMethodName = multiNodeImpl.getMethodName();
                if (multiNodeMethodName == null || multiNodeMethodName.isEmpty()) {
                    multiNodeImpl.setMethodName(this.taskParams.getName());
                }
                break;
            default:
                // It is a non-native method, nothing to do
                break;
        }

        // Compute the task parameters
        LinkedList<NIOParam> params = addParams();
        String parallelismSource = this.taskParams.getParallelismSource();
        // Create NIOTask
        NIOTask nt = new NIOTask(this.getLang(), DEBUG, absMethodImpl, parallelismSource,
            this.taskParams.hasTargetObject(), this.taskParams.getNumReturns(), params, this.slaveWorkersNodeNames,
            this.taskId, this.jobId, this.history, this.transferId, this.getOnFailure(), this.getTimeOut(),
            this.getPredecessors(), this.getNumSuccessors());

        return nt;
    }

    private LinkedList<NIOParam> addParams() {
        LinkedList<NIOParam> params = new LinkedList<>();
        for (Parameter param : this.taskParams.getParameters()) {
            params.add(NIOParamFactory.fromParameter(param, this.getResourceNode(), this.taskParams.isReplicated()));
        }
        return params;
    }

    /**
     * Marks the task as finished with the given {@code successful} error code.
     *
     * @param successful {@code true} if the task has successfully finished, {@code false} otherwise.
     * @param ntr information referring the results of the task executed
     * @param e Exception arose during the task execution, {@literal null} if no exception was raised.
     */
    public void taskFinished(boolean successful, NIOTaskResult ntr, Exception e) {
        if (this.history == JobHistory.CANCELLED) {
            LOGGER.error("Ignoring notification since the job was cancelled");
            removeTmpData();
            return;
        }
        if (successful) {
            this.completed(ntr);
        } else {
            if (e instanceof COMPSsException) {
                this.exception(ntr, (COMPSsException) e);
            } else {
                this.failed(ntr, JobEndStatus.EXECUTION_FAILED);
            }
        }
    }

    private void completed(NIOTaskResult ntr) {
        registerAllJobOutputs(ntr);
        super.completed();
    }

    private void exception(NIOTaskResult ntr, COMPSsException e) {
        registerAllJobOutputs(ntr);
        super.exception(e);
    }

    private void failed(NIOTaskResult ntr, JobEndStatus status) {
        if (this.isBeingCancelled()) {
            registerAllJobOutputs(ntr);
        } else {
            switch (this.taskParams.getOnFailure()) {
                case IGNORE:
                case CANCEL_SUCCESSORS:
                    registerAllJobOutputs(ntr);
                    break;
                default:
                    // case RETRY:
                    // case FAIL:
                    removeTmpData();
            }
        }
        super.failed(status);
    }

    private void registerAllJobOutputs(NIOTaskResult ntr) {
        // Update information
        List<NIOResult> taskResults = ntr.getParamResults();
        List<Parameter> taskParams = getTaskParams().getParameters();
        Iterator<Parameter> taskParamsItr = taskParams.iterator();
        Iterator<NIOResult> taskResultItr = taskResults.iterator();

        while (taskParamsItr.hasNext()) {
            Parameter param = taskParamsItr.next();
            NIOResult result = taskResultItr.next();
            registerResult(param, result);
        }
    }

    protected void registerResult(Parameter param, NIOResult result) {
        if (!param.isPotentialDependency()) {
            return;
        }

        DependencyParameter dp = (DependencyParameter) param;
        String rename = getOutputRename(dp);
        if (dp.isCollective()) {
            CollectiveParameter colParam = (CollectiveParameter) param;
            NIOResultCollection colResult = (NIOResultCollection) result;

            List<NIOResult> taskResults = colResult.getElements();
            List<Parameter> taskParams = colParam.getElements();
            Iterator<Parameter> taskParamsItr = taskParams.iterator();
            Iterator<NIOResult> taskResultItr = taskResults.iterator();

            while (taskParamsItr.hasNext()) {
                Parameter elemParam = taskParamsItr.next();
                NIOResult elemResult = taskResultItr.next();
                registerResult(elemParam, elemResult);
            }
        }

        // applicable whene the results are NIOResults
        Collection<NIOUri> uris = result.getUris();
        if (!uris.isEmpty()) {
            if (rename != null) {
                for (NIOUri uri : uris) {
                    String loc = uri.getPath();
                    registerResultPrivateLocation(loc, rename, this.worker);
                }
                notifyResultAvailability(dp, rename);
            }
        }
    }

    @Override
    public String toString() {
        AbstractMethodImplementation method = (AbstractMethodImplementation) this.impl;
        String definition = method.getMethodDefinition();
        String methodName = this.taskParams.getName();
        return "NIOJob JobId" + this.jobId + " for method " + methodName + " with definition " + definition;
    }

}
