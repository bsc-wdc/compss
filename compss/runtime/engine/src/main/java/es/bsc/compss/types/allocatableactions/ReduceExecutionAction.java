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
package es.bsc.compss.types.allocatableactions;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.types.ReduceTask;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.parameter.CollectionParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.FileParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


public class ReduceExecutionAction extends ExecutionAction {

    private TaskScheduler ts;
    private int reduceIndex = 0;
    // Initial collection of parameters to reduce
    private CollectionParameter initialCollection;
    // Map to store to know parameters generated per resource.
    private Map<Resource, List<Parameter>> parametersInResource;
    // List of intermediate reduce actions done.
    private List<AllocatableAction> alreadyDoneActions;
    // Map to know which reduce parameter has generated each intermediate reduce action
    private Map<AllocatableAction, Parameter> partialParameters;
    // Flag to indicate if the final task has been already executed
    private boolean finalTaskExecuted;
    // List of resources with a final
    private List<Resource> finalInResource;
    // Counter to indicate how many original parameters to reduce have been generated
    private int receivedOriginalParameters = 0;
    // List of resource used
    private List<Resource> usedResources;
    // Actions running in each resource
    private Map<Resource, List<AllocatableAction>> executingInResource;


    /**
     * Creates a new reduce execution action.
     *
     * @param schedulingInformation Associated scheduling information.
     * @param orchestrator Task orchestrator.
     * @param ap Access processor.
     * @param task Associated reduce task.
     */
    public ReduceExecutionAction(SchedulingInformation schedulingInformation, ActionOrchestrator orchestrator,
        AccessProcessor ap, ReduceTask task, TaskScheduler ts) {
        //
        super(schedulingInformation, orchestrator, ap, task);
        if (this.parametersInResource == null) {
            this.parametersInResource = new HashMap<>();
        }
        this.ts = ts;
        List<Parameter> finalParameters = this.task.getTaskDescription().getParameters();
        this.initialCollection = (CollectionParameter) finalParameters.get(0);
        this.partialParameters = new HashMap<>();
        this.finalTaskExecuted = false;
        this.usedResources = new ArrayList<>();
        LOGGER.debug("Creating new Reduce execution action");
        this.executingInResource = new HashMap<>();
        this.finalInResource = new ArrayList<>();

        if (this.alreadyDoneActions != null) {
            alreadyDonePredecessors();
        }

        List<Parameter> params = new ArrayList<>();
        for (Parameter p : task.getFreeParams()) {
            if (!(p instanceof CollectionParameter)
                && !(p instanceof FileParameter && ((FileParameter) p).getOriginalName().startsWith("reduce"))) {
                params.add(p);
            }
        }
        addNonDependentParam(params);
    }

    private void addNonDependentParam(List<Parameter> params) {
        for (Parameter p : params) {
            Resource r = null;
            if (p instanceof DependencyParameter) {
                r = getParameterLocation((DependencyParameter) p);
                if (r == null) {
                    r = assignResourceToFreeParam();
                }
            } else {
                r = assignResourceToFreeParam();
            }
            if (r != null) {
                this.parametersInResource.get(r).add(p);
                if (!(this.usedResources.contains(r))) {
                    this.usedResources.add(r);
                }
                this.receivedOriginalParameters++;
                addReduceTaskParameters(r);
            }
        }
    }

    private Resource assignResourceToFreeParam() {
        Resource r = null;
        Set<Resource> hosts = new HashSet<>();
        for (ResourceScheduler<?> rs : ts.getWorkers()) {
            hosts.add(rs.getResource());
        }
        r = getBestHost(r, hosts);
        return r;
    }

    private Resource getParameterLocation(DependencyParameter dp) {
        DataInstanceId dId = null;
        switch (dp.getDirection()) {
            case IN_DELETE:
            case IN:
            case CONCURRENT:
                RAccessId raId = (RAccessId) dp.getDataAccessId();
                dId = raId.getReadDataInstance();
                break;
            case COMMUTATIVE:
            case INOUT:
                RWAccessId rwaId = (RWAccessId) dp.getDataAccessId();
                dId = rwaId.getReadDataInstance();
                break;
            case OUT:
                // Cannot happen because of previous if
                break;
        }

        Resource maxResource = null;
        // Get hosts for resource score
        if (dId != null) {
            LogicalData dataLD = Comm.getData(dId.getRenaming());
            if (dataLD != null) {
                Set<Resource> hosts = dataLD.getAllHosts();
                maxResource = getBestHost(maxResource, hosts);
            }
        }
        return maxResource;
    }

    private Resource getBestHost(Resource maxResource, Set<Resource> hosts) {
        if (!hosts.isEmpty()) {
            maxResource = hosts.iterator().next();
            for (Resource host : hosts) {
                checkAndRegisterResource(host);
                if (this.parametersInResource.get(maxResource).size() < this.parametersInResource.get(host).size()) {
                    maxResource = host;
                }
            }

        }
        if (DEBUG) {
            LOGGER.debug("Chosen host to run the reduce: " + (maxResource != null ? maxResource.toString() : "null"));
        }

        return maxResource;
    }

    /**
     * Registers a predecessor that has finished before the creation of the reduce execution action.
     * 
     * @param finishedAction Action that has already finished.
     */
    @Override
    public void addAlreadyDoneAction(AllocatableAction finishedAction) {
        LOGGER.debug("Registering parameter of an already done action");
        if (this.alreadyDoneActions == null) {
            this.alreadyDoneActions = new ArrayList<>();
        }
        this.alreadyDoneActions.add(finishedAction);
    }

    private void alreadyDonePredecessors() {
        for (AllocatableAction a : this.alreadyDoneActions) {
            registerPredecessorDone(a);
        }
    }

    @Override
    /**
     * Updates the predecessors by removing the finished action {@code finishedAction}.
     *
     * @param finishedAction Finished Allocatable Action.
     */
    protected void dataPredecessorDone(AllocatableAction finishedAction) {

        registerPredecessorDone(finishedAction);

        super.dataPredecessorDone(finishedAction);
    }

    /**
     * Registers a predecessor of the reduce task.
     * 
     * @param finishedAction The action to register.
     */
    public void registerPredecessorDone(AllocatableAction finishedAction) {
        Resource resource = finishedAction.getAssignedResource().getResource();
        checkAndRegisterResource(resource);
        Task finishedTask = ((ExecutionAction) finishedAction).getTask();
        Parameter param;
        boolean partial = false;
        // Check if para
        if (!finishedTask.getTaskDescription().getName().equals(this.task.getTaskDescription().getName())) {
            param = this.getTask().getDependencyParameters(finishedTask);
            this.receivedOriginalParameters++;
        } else {
            partial = true;
            param = this.partialParameters.get(finishedAction);
            List<AllocatableAction> actions = this.executingInResource.get(resource);
            actions.remove(finishedAction);
        }

        this.parametersInResource.get(resource).add(param);
        if (!(this.usedResources.contains(resource))) {
            this.usedResources.add(resource);
        }
        if (partial) {
            this.partialParameters.remove(finishedAction);
        }

        addReduceTaskParameters(resource);
        ((ExecutionAction) finishedAction).getTask().releaseDataDependents();
    }

    private void checkAndRegisterResource(Resource resource) {
        if (!(this.parametersInResource.containsKey(resource))) {
            List<Parameter> parameters = new ArrayList<>();
            this.parametersInResource.put(resource, parameters);
        }
    }

    private void addReduceTaskParameters(Resource resource) {

        List<Parameter> params = this.parametersInResource.get(resource);
        if (!finalTaskExecuted) {
            if (params.size() >= getChunkSize()) {
                launchReduceAction(resource, params);
                reduceIndex++;
                this.parametersInResource.put(resource, new ArrayList<>());

            }
            if (noMoreOriginalReductionParamsPending()) {
                // Try to send final tasks
                List<AllocatableAction> actions = this.executingInResource.get(resource);
                if (actions == null || actions.isEmpty()) {
                    // Nothing else running in the resource send a final task in released resource
                    finalTaskInResource(resource);
                }
                if (mustLaunchFinalTasks()) {
                    launchFinalTasks();
                }
            }
        }
    }

    private void launchFinalTasks() {
        ArrayList<Parameter> finalParams = new ArrayList<>();
        this.finalTaskExecuted = true;

        for (Resource r : parametersInResource.keySet()) {
            if (!this.finalInResource.contains(r)) {
                finalTaskInResource(r);
            }
        }
        for (Entry<Resource, List<Parameter>> e : parametersInResource.entrySet()) {
            finalParams.addAll(e.getValue());
        }

        setFinalExecutionParameters(finalParams);
        finalTaskExecuted = true;

    }

    private int getChunkSize() {
        return ((ReduceTask) this.task).getChunkSize();
    }

    private boolean mustLaunchFinalTasks() {
        return this.partialParameters.size() == 0 && this.usedResources.size() > 1
            || (this.usedResources.size() == 1 && (this.partialParameters.size()
                + parametersInResource.get(usedResources.get(0)).size()) == getChunkSize());
    }

    private boolean noMoreOriginalReductionParamsPending() {
        return this.initialCollection != null
            && this.receivedOriginalParameters == this.initialCollection.getParameters().size();
    }

    private void finalTaskInResource(Resource resource) {
        List<Parameter> params = parametersInResource.get(resource);
        if (params.size() > 1) {
            launchReduceAction(resource, params);
            reduceIndex++;
            this.parametersInResource.put(resource, new ArrayList<>());
            finalInResource.add(resource);
        }
    }

    private void setFinalExecutionParameters(List<Parameter> params) {
        LOGGER.debug("Adding final reduce task for task " + this.task.toString());

        ReduceTask t = (ReduceTask) this.task;
        List<Parameter> partialsIn = new ArrayList<>(this.partialParameters.values());
        for (Parameter p : params) {
            partialsIn.add(p);
        }
        CollectionParameter cPartial = t.getFinalCollection();

        cPartial.setParameters(partialsIn);
        List<Parameter> finalParameters = this.task.getTaskDescription().getParameters();
        finalParameters.set(0, cPartial);
    }

    private void launchReduceAction(Resource resource, List<Parameter> params) {
        LOGGER.debug("Adding intermediate reduce task for task " + this.task.getId());
        ReduceTask t = (ReduceTask) this.task;
        CollectionParameter cp = t.getIntermediateCollections().get(reduceIndex);
        cp.setParameters(params);
        List<Parameter> taskP = new ArrayList<>();
        taskP.add(cp);
        Parameter result = t.getIntermediateOutParameters().get(0);
        t.setPartialOutUsed(result);
        Parameter partialIn = t.getIntermediateInParameters().get(0);
        t.setPartialInUsed(partialIn);
        taskP.add(result);
        TaskDescription td = this.task.getTaskDescription();
        Task partialTask = new Task(this.task.getApplication(), td.getLang(), td.getName(), td.hasPriority(),
            td.getNumNodes(), td.isReduction(), td.isReplicated(), td.isDistributed(), td.hasTargetObject(),
            td.getNumReturns(), taskP, this.task.getTaskMonitor(), td.getOnFailure(), td.getTimeOut());

        ResourceScheduler<? extends WorkerResourceDescription> partialReduceScheduler =
            ts.getWorkers().iterator().next();
        for (ResourceScheduler<? extends WorkerResourceDescription> rs : ts.getWorkers()) {
            if (rs.getResource() == resource) {
                partialReduceScheduler = rs;
                break;
            }
        }
        ExecutionAction partialReduceAction =
            new ExecutionAction(ts.generateSchedulingInformation(partialReduceScheduler, td.getParameters(),
                td.getCoreElement().getCoreId()), this.orchestrator, this.ap, partialTask);
        addDataPredecessor(partialReduceAction);
        partialParameters.put(partialReduceAction, partialIn);
        ts.newAllocatableAction(partialReduceAction);
        addExecutingToResource(partialReduceAction, resource);
    }

    private void addExecutingToResource(AllocatableAction partialReduceAction, Resource resource) {
        List<AllocatableAction> actions;
        if (!this.executingInResource.containsKey(resource)) {
            actions = new ArrayList<>();
            this.executingInResource.put(resource, actions);
        }
        actions = this.executingInResource.get(resource);
        actions.add(partialReduceAction);
    }

    /*
     * ***************************************************************************************************************
     * EXECUTION TRIGGERS
     * ***************************************************************************************************************
     */
    @Override
    protected void doCompleted() {
        List<Parameter> finalParameters = this.task.getTaskDescription().getParameters();
        finalParameters.set(0, initialCollection);
        ((ReduceTask) this.task).clearPartials();
        super.doCompleted();
    }

    @Override
    protected void doFailed() {
        List<Parameter> finalParameters = this.task.getTaskDescription().getParameters();
        finalParameters.set(0, initialCollection);

        super.doFailed();
    }

    @Override
    protected void doCanceled() {
        List<Parameter> finalParameters = this.task.getTaskDescription().getParameters();
        finalParameters.set(0, initialCollection);

        super.doCanceled();
    }

    @Override
    protected void doFailIgnored() {
        List<Parameter> finalParameters = this.task.getTaskDescription().getParameters();
        finalParameters.set(0, initialCollection);

        super.doFailIgnored();
    }

}
