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
package es.bsc.compss.local;

import es.bsc.compss.types.COMPSsMaster;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.types.job.JobHistory;
import es.bsc.compss.types.job.JobImpl;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.CollectiveParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.worker.COMPSsException;

import java.util.LinkedList;
import java.util.List;


public class LocalJob extends JobImpl<COMPSsMaster> implements Invocation {

    private boolean prepared;
    private final List<LocalParameter> arguments;
    private LocalParameter target;
    private final LinkedList<LocalParameter> results;
    private final MethodResourceDescription reqs;
    private final List<String> slaveWorkersNodeNames;
    private final TaskType taskType;


    /**
     * Creates a new LocalJob instance.
     *
     * @param taskId Associated task Id.
     * @param task Associated task description.
     * @param impl Task implementation.
     * @param res Resource.
     * @param slaveWorkersNodeNames List of slave nodes for multi-node tasks.
     * @param listener Task listener.
     */
    public LocalJob(int taskId, TaskDescription task, Implementation impl, Resource res,
        List<String> slaveWorkersNodeNames, JobListener listener, List<Integer> predecessors, Integer numSuccessors) {

        super(taskId, task, impl, res, listener, predecessors, numSuccessors);
        this.taskType = impl.getTaskType();
        this.arguments = new LinkedList<>();
        this.results = new LinkedList<>();
        this.prepared = false;
        this.slaveWorkersNodeNames = slaveWorkersNodeNames;

        AbstractMethodImplementation absMethodImpl = (AbstractMethodImplementation) this.impl;
        this.reqs = absMethodImpl.getRequirements();
    }

    private LocalParameter generateLocalParameter(Parameter p) {
        if (p.isCollective()) {
            CollectiveParameter<Parameter> cp = (CollectiveParameter) p;
            LocalParameterCollection lpc = new LocalParameterCollection(p);
            for (Parameter subParam : cp.getElements()) {
                lpc.addElement(generateLocalParameter(subParam));
            }
            return lpc;
        } else {
            return new LocalParameter(p);
        }
    }

    private void prepareParameters() {
        // Construct parameters
        final boolean hasTarget = this.taskParams.hasTargetObject();
        final int numReturns = this.taskParams.getNumReturns();

        List<Parameter> params = this.taskParams.getParameters();
        int paramsCount = params.size();
        for (int rIdx = 0; rIdx < numReturns; rIdx++) {
            int paramIdx = params.size() - numReturns + rIdx;
            Parameter p = params.get(paramIdx);
            this.results.add(generateLocalParameter(p));
        }

        paramsCount -= numReturns;
        if (hasTarget) {
            int paramIdx = params.size() - numReturns - 1;
            Parameter p = params.get(paramIdx);
            this.target = generateLocalParameter(p);
            paramsCount--;
        }

        for (int paramIdx = 0; paramIdx < paramsCount; paramIdx++) {
            Parameter p = params.get(paramIdx);
            this.arguments.add(generateLocalParameter(p));
        }
        this.prepared = true;
    }

    @Override
    public void submitJob() throws Exception {
        this.profileArrival();
        this.fetchedAllInputData();
        if (!prepared) {
            prepareParameters();
        }
        this.getResourceNode().runJob(this);
    }

    @Override
    public void cancelJob() throws Exception {
    }

    /**
     * A parameter of the job has been computed.
     *
     * @param lp Computed Parameter
     */
    public void notifyResultAvailable(LocalParameter lp) {
        DependencyParameter dp = (DependencyParameter) lp.getParam();
        String dataName = lp.getDataMgmtId();
        notifyResultAvailability(dp, dataName);
    }

    @Override
    public void completed() {
        if (this.history == JobHistory.CANCELLED) {
            LOGGER.error("Ignoring notification since the job was cancelled");
            removeTmpData();
            return;
        }

        super.completed();
    }

    @Override
    public void exception(COMPSsException e) {
        if (this.history == JobHistory.CANCELLED) {
            LOGGER.error("Ignoring notification since the job was cancelled");
            removeTmpData();
            return;
        }

        super.exception(e);
    }

    @Override
    public void failed(JobEndStatus status) {
        if (this.history == JobHistory.CANCELLED) {
            LOGGER.error("Ignoring notification since the job was cancelled");
            removeTmpData();
            return;
        }

        switch (this.taskParams.getOnFailure()) {
            case IGNORE:
            case CANCEL_SUCCESSORS:
                break;
            default:
                // case RETRY:
                // case FAIL:
                removeTmpData();
        }
        super.failed(status);
    }

    @Override
    public TaskType getType() {
        return TaskType.METHOD;
    }

    @Override
    public TaskType getTaskType() {
        return taskType;
    }

    @Override
    public String toString() {
        return "LocalJob JobId" + this.jobId + " for task " + this.impl.getSignature();
    }

    @Override
    public AbstractMethodImplementation getMethodImplementation() {
        return (AbstractMethodImplementation) this.impl;
    }

    @Override
    public boolean isDebugEnabled() {
        return DEBUG;
    }

    @Override
    public List<LocalParameter> getParams() {
        return this.arguments;
    }

    @Override
    public LocalParameter getTarget() {
        return this.target;
    }

    @Override
    public List<LocalParameter> getResults() {
        return this.results;
    }

    @Override
    public ResourceDescription getRequirements() {
        return this.reqs;
    }

    @Override
    public List<String> getSlaveNodesNames() {
        return this.slaveWorkersNodeNames;
    }

    @Override
    public boolean producesEmptyResultsOnFailure() {
        OnFailure failPolicy = this.getOnFailure();
        return failPolicy != OnFailure.RETRY && failPolicy != OnFailure.FAIL;
    }

    @Override
    public String getParallelismSource() {
        return this.taskParams.getParallelismSource();
    }

}
