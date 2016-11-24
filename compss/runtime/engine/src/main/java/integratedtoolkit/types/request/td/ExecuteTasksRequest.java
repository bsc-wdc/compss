package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.TaskProducer;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.Task.TaskState;
import integratedtoolkit.types.allocatableactions.MasterExecutionAction;
import integratedtoolkit.types.allocatableactions.SlaveExecutionAction;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ResourceScheduler;


/**
 * The ExecuteTasksRequest class represents the request to execute a task
 */
public class ExecuteTasksRequest<P extends Profile, T extends WorkerResourceDescription> extends TDRequest<P, T> {

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
    public void process(TaskScheduler<P, T> ts) throws ShutdownException {
        int coreID = task.getTaskDescription().getId();
        if (debug) {
            logger.debug("Treating Scheduling request for task " + task.getId() + "(core " + coreID + ")");
        }
        
        task.setStatus(TaskState.TO_EXECUTE);
        int numNodes = task.getTaskDescription().getNumNodes();
        
        if (task.getTaskDescription().isReplicated()) {
            // Method annotation forces to replicate task to all nodes
            if (debug) {
                logger.debug("Replicating task " + task.getId());
            }
            ResourceScheduler<P, T>[] resources = ts.getWorkers();
            task.setExecutionCount(resources.length * numNodes);
            for (ResourceScheduler<P, T> rs : resources) {
                submitTask(ts, rs);
            }
        } else if (task.getTaskDescription().isDistributed()) {
            // Method annotation forces RoundRobin among nodes
            // WARN: This code is proportional to the number of resources, can lead to some overhead
            if (debug) {
                logger.debug("Distributing task " + task.getId());
            }
            
            task.setExecutionCount(numNodes);
            
            ResourceScheduler<P, T> selectedResource = null;
            int minNumTasksOfSameType = Integer.MAX_VALUE;
            ResourceScheduler<P, T>[] resources = ts.getWorkers();
            for (ResourceScheduler<P, T> rs : resources) {
                // RS numTasks only considers MasterExecutionActions
                int numTasks = rs.getNumTasks(task.getTaskDescription().getId());
                if (numTasks < minNumTasksOfSameType) {
                    minNumTasksOfSameType = numTasks;
                    selectedResource = rs;
                }
            }
            
            submitTask(ts, selectedResource);
        } else {
            // Normal task
            task.setExecutionCount(numNodes);
            submitTask(ts, null);
        }

        if (debug) {
            logger.debug("Treated Scheduling request for task " + task.getId() + "(core " + coreID + ")");
        }
    }
    
    private void submitTask(TaskScheduler<P, T> ts, ResourceScheduler<P, T> specificResource) {
        // Can use one or more resources depending on the computingNodes
        int numNodes = task.getTaskDescription().getNumNodes();
        int numSlaveNodes = numNodes - 1;

        // Prepare the master execution task
        MasterExecutionAction<P, T> masterExec = new MasterExecutionAction<>(ts.generateSchedulingInformation(), 
                                                                                producer, 
                                                                                task,
                                                                                numSlaveNodes,
                                                                                specificResource);
 
        // Launch slave tasks if needed (can go to any resource if it fits the requirements)
        if (debug && numSlaveNodes > 0) {
            logger.debug("MultiNode task " + task.getId() + ". Reserving slave nodes...");
        }
        for (int i = 0; i < numSlaveNodes; ++i) {
            SlaveExecutionAction<P, T> slaveExec = new SlaveExecutionAction<>(ts.generateSchedulingInformation(), 
                                                                                producer, 
                                                                                task,
                                                                                masterExec,
                                                                                null);
            ts.newAllocatableAction(slaveExec);
        }
        
        // Launch the master task
        ts.newAllocatableAction(masterExec);
    }

    @Override
    public TDRequestType getType() {
        return TDRequestType.EXECUTE_TASKS;
    }

}
