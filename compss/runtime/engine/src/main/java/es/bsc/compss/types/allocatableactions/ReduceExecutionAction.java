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
package es.bsc.compss.types.allocatableactions;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.types.ReduceTask;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.parameter.impl.CollectiveParameter;
import es.bsc.compss.types.parameter.impl.DependencyParameter;
import es.bsc.compss.types.parameter.impl.FileParameter;
import es.bsc.compss.types.parameter.impl.Parameter;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.ErrorManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


public class ReduceExecutionAction extends ExecutionAction {

    private static final String LOG_PREFIX = "[Reduce-Execution-Action] ";
    private TaskScheduler ts;
    private int reduceIndex = 0;
    // Initial collection of parameters to reduce
    private CollectiveParameter initialCollection;
    // Other task parameters which are not part of the collection
    private Set<Parameter> extraParameters;
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
    private int colIndex;
    private Set<Integer> partialTaskIds;


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
        this.extraParameters = new HashSet<>();
        this.partialTaskIds = new HashSet<>();
        List<Parameter> finalParameters = this.task.getTaskDescription().getParameters();
        this.colIndex = task.getReduceCollectionIndex();
        for (int i = 0; i < finalParameters.size() - 1; i++) {
            if (i == this.colIndex) {
                this.initialCollection = (CollectiveParameter) finalParameters.get(colIndex);
            } else {
                this.extraParameters.add(finalParameters.get(i));
            }
        }
        this.partialParameters = new HashMap<>();
        this.finalTaskExecuted = false;
        this.usedResources = new ArrayList<>();
        LOGGER.debug(LOG_PREFIX + "Creating new Reduce execution action for Task " + this.task.getId());
        this.executingInResource = new HashMap<>();
        this.finalInResource = new ArrayList<>();

        if (this.alreadyDoneActions != null) {
            alreadyDonePredecessors();
        }
        // Checking parameters without data dependency (excluding the extra parameters)
        List<Parameter> params = new ArrayList<>();
        for (Parameter p : task.getFreeParams()) {
            if (!(p.isCollective())
                && !(p instanceof FileParameter && ((FileParameter) p).getOriginalName().startsWith("reduce"))
                && !extraParameters.contains(p)) {
                params.add(p);
            }
        }
        addNonDependentParam(params);
    }

    private void addNonDependentParam(List<Parameter> params) {
        for (Parameter p : params) {
            Resource r = null;
            if (p.isPotentialDependency()) {
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
        EngineDataInstanceId dId = null;
        EngineDataAccessId access = dp.getDataAccessId();
        if (access.isRead()) {
            ReadingDataAccessId raId = (ReadingDataAccessId) dp.getDataAccessId();
            dId = raId.getReadDataInstance();
        }

        Resource maxResource = null;
        // Get hosts for resource score
        if (dId != null) {
            LogicalData dataLD = dId.getData();
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
            LOGGER.debug(LOG_PREFIX + "Chosen host to run the reduce: "
                + (maxResource != null ? maxResource.toString() : "null"));
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
        LOGGER.debug(
            LOG_PREFIX + "Registering a dependenant action was already done action for Task " + this.getTask().getId());
        if (this.alreadyDoneActions == null) {
            this.alreadyDoneActions = new ArrayList<>();
        }
        this.alreadyDoneActions.add(finishedAction);
    }

    private void alreadyDonePredecessors() {
        LOGGER.debug(LOG_PREFIX + "Treating already done action actions for " + this.getTask().getId());
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
        LOGGER.debug(LOG_PREFIX + "Registering predecessor done for Task " + this.getTask().getId());
        Resource resource = finishedAction.getAssignedResource().getResource();
        checkAndRegisterResource(resource);
        Task finishedTask = ((ExecutionAction) finishedAction).getTask();

        Parameter param;
        boolean isReduceParam = false;
        if (!this.partialTaskIds.contains(finishedTask.getId())) {
            this.partialTaskIds.remove(finishedTask.getId());
            param = this.getTask().getDependencyParameters(finishedTask);
            if (!this.extraParameters.contains(param)) {
                isReduceParam = true;
                this.receivedOriginalParameters++;
            }

        } else {
            isReduceParam = true;
            param = this.partialParameters.remove(finishedAction);
            List<AllocatableAction> actions = this.executingInResource.get(resource);
            actions.remove(finishedAction);
        }
        if (isReduceParam) {
            this.parametersInResource.get(resource).add(param);
            if (!(this.usedResources.contains(resource))) {
                this.usedResources.add(resource);
            }
        }
        addReduceTaskParameters(resource);
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
            if (noMoreOriginalReductionParamsPending()) {
                // Try to send final tasks
                if (mustLaunchFinalTasks()) {
                    launchFinalTasks();
                } else {
                    List<AllocatableAction> actions = this.executingInResource.get(resource);
                    if (actions == null || actions.isEmpty()) {
                        LOGGER.debug(
                            "No partial reduces running in the resource. Trying to launch a final task in resource "
                                + resource.getName());
                        finalTaskInResource(resource);
                    } else if (params.size() >= getChunkSize()) {
                        // If running partials but chunk size reached -> Launch reduce action
                        launchReduceAction(resource, params);
                        reduceIndex++;
                        this.parametersInResource.put(resource, new ArrayList<>());

                    }
                }

            } else if (params.size() >= getChunkSize()) {
                launchReduceAction(resource, params);
                reduceIndex++;
                this.parametersInResource.put(resource, new ArrayList<>());

            }

        }
    }

    private void launchFinalTasks() {
        LOGGER.debug(LOG_PREFIX + "Launching final tasks for Task " + this.getTask().getId());
        ArrayList<Parameter> finalParams = new ArrayList<>();
        this.finalTaskExecuted = true;
        // If I have more than one node I try to run final tasks.
        // Otherwise we just prepare the collection with partial results for the original task
        if (usedResources.size() > 1) {
            for (Resource r : parametersInResource.keySet()) {
                if (!this.finalInResource.contains(r)) {
                    finalTaskInResource(r);
                }
            }
        }
        // Adding all the parameters which have not been added to the final tasks
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
        // No running partials pending and more than one resource used
        // or single used resource and expected parameters in resource is less than a chunk
        return (this.partialParameters.size() == 0 && this.usedResources.size() > 1)
            || (this.usedResources.size() == 1 && (this.partialParameters.size()
                + parametersInResource.get(usedResources.get(0)).size()) <= getChunkSize());
    }

    private boolean noMoreOriginalReductionParamsPending() {
        return this.initialCollection != null
            && this.receivedOriginalParameters == this.initialCollection.getElements().size();
    }

    private void finalTaskInResource(Resource resource) {
        LOGGER.debug(LOG_PREFIX + "Trying to generate final reduce for resource " + resource.getName() + " of Task "
            + this.task.getId());
        List<Parameter> params = parametersInResource.get(resource);
        if (params.size() > 1) {
            launchReduceAction(resource, params);
            reduceIndex++;
            this.parametersInResource.put(resource, new ArrayList<>());
            finalInResource.add(resource);
        }
    }

    private void setFinalExecutionParameters(List<Parameter> params) {
        LOGGER.debug(LOG_PREFIX + "Creating final reduce task for Task " + this.task.getId());

        ReduceTask t = (ReduceTask) this.task;
        List<Parameter> partialsIn = new ArrayList<>(this.partialParameters.values());
        for (Parameter p : params) {
            partialsIn.add(p);
        }
        CollectiveParameter cPartial = t.getFinalCollection();

        cPartial.setElements(partialsIn);
        List<Parameter> finalParameters = this.task.getTaskDescription().getParameters();
        finalParameters.set(this.colIndex, cPartial);
    }

    private void launchReduceAction(Resource resource, List<Parameter> params) {
        LOGGER.debug(LOG_PREFIX + "Creating partial reduce task for Task " + this.task.getId());
        ReduceTask t = (ReduceTask) this.task;

        // Check if number of reduces
        if (reduceIndex >= t.getIntermediateCollections().size()) {
            LOGGER.error("ERROR: Reduce Task " + this.task.getId() + " has exceed the number of partial reduces");
            ErrorManager.fatal("ERROR: Reduce task " + this.task.getId() + " has exceed the number of partial reduces");
        }
        CollectiveParameter cp = t.getIntermediateCollections().get(reduceIndex);
        cp.setElements(params);
        List<Parameter> taskP = new ArrayList<>();
        TaskDescription td = this.task.getTaskDescription();
        List<Parameter> oldParameters = td.getParameters();
        for (int i = 0; i < oldParameters.size() - 1; i++) {
            if (i == this.colIndex) {
                taskP.add(cp);
            } else {
                taskP.add(oldParameters.get(i));
            }
        }

        Parameter result = t.getIntermediateOutParameters().get(0);
        t.setPartialOutUsed(result);
        Parameter partialIn = t.getIntermediateInParameters().get(0);
        t.setPartialInUsed(partialIn);
        taskP.add(result);

        Task partialTask = new Task(this.task.getApplication(), td.getLang(), td.getName(), td.hasPriority(),
            td.getNumNodes(), td.isReduction(), td.isReplicated(), td.isDistributed(), td.hasTargetObject(),
            td.getNumReturns(), taskP, this.task.getTaskMonitor(), td.getOnFailure(), td.getTimeOut());
        this.partialTaskIds.add(partialTask.getId());
        LOGGER.debug(LOG_PREFIX + "Task " + partialTask.getId() + " is a partial reduce task for Task "
            + this.task.getId() + ". Adding as predecessor");
        ResourceScheduler<? extends WorkerResourceDescription> partialReduceScheduler =
            ts.getWorkers().iterator().next();
        for (ResourceScheduler<? extends WorkerResourceDescription> rs : ts.getWorkers()) {
            if (rs.getResource() == resource) {
                partialReduceScheduler = rs;
                break;
            }
        }
        ExecutionAction partialReduceAction = new ExecutionAction(
            ts.generateSchedulingInformation(partialReduceScheduler, taskP, td.getCoreElement().getCoreId()),
            this.orchestrator, this.ap, partialTask);
        int previous = getDataPredecessors().size();
        addDataPredecessor(partialReduceAction);
        LOGGER.debug("Current predec: " + getDataPredecessors().size() + " previous: " + previous);
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
        finalParameters.set(this.colIndex, initialCollection);
        ((ReduceTask) this.task).clearPartials();
        super.doCompleted();
    }

    @Override
    protected void doFailed() {
        List<Parameter> finalParameters = this.task.getTaskDescription().getParameters();
        finalParameters.set(this.colIndex, initialCollection);

        super.doFailed();
    }

    @Override
    protected boolean doCanceled() {
        List<Parameter> finalParameters = this.task.getTaskDescription().getParameters();
        finalParameters.set(this.colIndex, initialCollection);

        return super.doCanceled();
    }

    @Override
    protected void doFailIgnored() {
        List<Parameter> finalParameters = this.task.getTaskDescription().getParameters();
        finalParameters.set(this.colIndex, initialCollection);

        super.doFailIgnored();
    }

}
