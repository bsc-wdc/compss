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
package es.bsc.compss.nio.master;

import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.master.utils.NIOParamFactory;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.implementations.definition.MultiNodeDefinition;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.worker.COMPSsException;

import java.util.LinkedList;
import java.util.List;


public class NIOJob extends Job<NIOWorkerNode> {

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
    public String getHostName() {
        return worker.getName();
    }

    @Override
    public void submit() throws Exception {
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
    public NIOTask prepareJob() {
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
        int numParams = params.size() - this.taskParams.getNumReturns();
        String parallelismSource = this.taskParams.getParallelismSource();
        // Create NIOTask
        NIOTask nt = new NIOTask(this.getLang(), DEBUG, absMethodImpl, parallelismSource,
            this.taskParams.hasTargetObject(), this.taskParams.getNumReturns(), params, numParams,
            absMethodImpl.getRequirements(), this.slaveWorkersNodeNames, this.taskId, this.impl.getTaskType(),
            this.jobId, this.history, this.transferId, this.getOnFailure(), this.getTimeOut(), this.getPredecessors(),
            this.getNumSuccessors());

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
     * @param e Exception rised during the task execution, {@literal null} if no exception was raised.
     */
    public void taskFinished(boolean successful, Exception e) {
        if (successful) {
            this.listener.jobCompleted(this);
        } else {
            if (e instanceof COMPSsException) {
                this.listener.jobException(this, (COMPSsException) e);
            } else {
                this.listener.jobFailed(this, JobEndStatus.EXECUTION_FAILED);
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
