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
package es.bsc.compss.nio.master;

import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.master.utils.NIOParamFactory;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.implementations.MultiNodeImplementation;
import es.bsc.compss.types.implementations.TaskType;
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
            List<String> slaveWorkersNodeNames, JobListener listener) {

        super(taskId, taskParams, impl, res, listener);
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
                MethodImplementation methodImpl = (MethodImplementation) absMethodImpl;
                String methodName = methodImpl.getAlternativeMethodName();
                if (methodName == null || methodName.isEmpty()) {
                    methodImpl.setAlternativeMethodName(this.taskParams.getName());
                }
                break;
            case MULTI_NODE:
                MultiNodeImplementation multiNodeImpl = (MultiNodeImplementation) absMethodImpl;
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
        int numParams = params.size() - taskParams.getNumReturns();

        // Create NIOTask
        NIOTask nt = new NIOTask(this.getLang(), DEBUG, absMethodImpl, this.taskParams.hasTargetObject(),
                this.taskParams.getNumReturns(), params, numParams, absMethodImpl.getRequirements(),
                this.slaveWorkersNodeNames, this.taskId, this.taskParams.getType(), this.jobId, this.history,
                this.transferId, this.getTimeOut());

        return nt;
    }

    private LinkedList<NIOParam> addParams() {
        LinkedList<NIOParam> params = new LinkedList<>();
        for (Parameter param : this.taskParams.getParameters()) {
            params.add(NIOParamFactory.fromParameter(param));
        }
        return params;
    }

    /**
     * Marks the task as finished with the given {@code successful} error code.
     *
     * @param successful {@code true} if the task has successfully finished, {@code false} otherwise.
     * @param e          Exception rised during the task execution, {@literal null} if no exception was rised.
     */
    public void taskFinished(boolean successful, Exception e) {
        if (successful) {
            listener.jobCompleted(this);
        } else {
            if (e instanceof COMPSsException) {
                listener.jobFailed(this, JobEndStatus.EXCEPTION, (COMPSsException) e);
            } else {
                listener.jobFailed(this, JobEndStatus.EXECUTION_FAILED, null);
            }
        }
    }

    @Override
    public void stop() throws Exception {
        // Do nothing
    }

    @Override
    public String toString() {
        AbstractMethodImplementation method = (AbstractMethodImplementation) this.impl;
        String definition = method.getMethodDefinition();
        String methodName = taskParams.getName();
        return "NIOJob JobId" + this.jobId + " for method " + methodName + " with definition " + definition;
    }

}
