package integratedtoolkit.types.request.td;

import java.util.Collection;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.components.impl.TaskProducer;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.Task.TaskState;
import integratedtoolkit.types.allocatableactions.ExecutionAction;
import integratedtoolkit.types.allocatableactions.MultiNodeExecutionAction;
import integratedtoolkit.types.allocatableactions.MultiNodeGroup;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.types.resources.WorkerResourceDescription;


/**
 * The ExecuteTasksRequest class represents the request to execute a task
 */
public class ExecuteTasksRequest<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends TDRequest<P, T, I> {

    private final TaskProducer producer;
    private final Task task;


    /**
     * Constructs a new ScheduleTasks Request
     *
     * @param producer
     *            taskProducer to be notified when the task ends
     * @param t
     *            Task to run
     */
    public ExecuteTasksRequest(TaskProducer producer, Task t) {
        this.producer = producer;
        this.task = t;
    }

    /**
     * Returns the task to execute
     *
     * @return task to execute
     */
    public Task getTask() {
        return task;
    }

    @Override
    public void process(TaskScheduler<P, T, I> ts) throws ShutdownException {
        int coreId = task.getTaskDescription().getId();
        if (debug) {
            logger.debug("Treating Scheduling request for task " + task.getId() + "(core " + coreId + ")");
        }

        task.setStatus(TaskState.TO_EXECUTE);
        int numNodes = task.getTaskDescription().getNumNodes();
        boolean isReplicated = task.getTaskDescription().isReplicated();
        boolean isDistributed = task.getTaskDescription().isDistributed();

        if (isReplicated) {
            // Method annotation forces to replicate task to all nodes
            if (debug) {
                logger.debug("Replicating task " + task.getId());
            }

            Collection<ResourceScheduler<P, T, I>> resources = ts.getWorkers();
            task.setExecutionCount(resources.size() * numNodes);
            for (ResourceScheduler<P, T, I> rs : resources) {
                submitTask(ts, numNodes, rs);
            }
        } else if (isDistributed) {
            // Method annotation forces RoundRobin among nodes
            // WARN: This code is proportional to the number of resources, can lead to some overhead
            if (debug) {
                logger.debug("Distributing task " + task.getId());
            }

            ResourceScheduler<P, T, I> selectedResource = null;
            int minNumTasksOfSameType = Integer.MAX_VALUE;
            Collection<ResourceScheduler<P, T, I>> resources = ts.getWorkers();
            for (ResourceScheduler<P, T, I> rs : resources) {
                // RS numTasks only considers MasterExecutionActions
                int numTasks = rs.getNumTasks(task.getTaskDescription().getId());
                if (numTasks < minNumTasksOfSameType) {
                    minNumTasksOfSameType = numTasks;
                    selectedResource = rs;
                }
            }

            task.setExecutionCount(numNodes);
            submitTask(ts, numNodes, selectedResource);
        } else {
            // Normal task
            if (debug) {
                logger.debug("Submitting task " + task.getId());
            }
            
            task.setExecutionCount(numNodes);
            submitTask(ts, numNodes, null);
        }

        if (debug) {
            logger.debug("Treated Scheduling request for task " + task.getId() + " (core " + coreId + ")");
        }
    }

    private void submitTask(TaskScheduler<P, T, I> ts, int numNodes, ResourceScheduler<P, T, I> specificResource) {
        // A task can use one or more resources
        if (numNodes == 1) {
            submitSingleTask(ts, specificResource);
        } else {
            submitMultiNodeTask(ts, numNodes, specificResource);
        }
    }

    private void submitSingleTask(TaskScheduler<P, T, I> ts, ResourceScheduler<P, T, I> specificResource) {
        logger.debug("Scheduling request for task " + task.getId() + " treated as singleTask");
        ExecutionAction<P, T, I> action = new ExecutionAction<>(ts.generateSchedulingInformation(), ts.getOrchestrator(), producer, task,
                specificResource);
        ts.newAllocatableAction(action);
    }

    private void submitMultiNodeTask(TaskScheduler<P, T, I> ts, int numNodes, ResourceScheduler<P, T, I> specificResource) {
        logger.debug("Scheduling request for task " + task.getId() + " treated as multiNodeTask");
        // Can use one or more resources depending on the computingNodes
        MultiNodeGroup<P, T, I> group = new MultiNodeGroup<>(numNodes);
        for (int i = 0; i < numNodes; ++i) {
            MultiNodeExecutionAction<P, T, I> action = new MultiNodeExecutionAction<>(ts.generateSchedulingInformation(),
                    ts.getOrchestrator(), producer, task, specificResource, group);
            ts.newAllocatableAction(action);
        }
    }

    @Override
    public TDRequestType getType() {
        return TDRequestType.EXECUTE_TASKS;
    }

}
