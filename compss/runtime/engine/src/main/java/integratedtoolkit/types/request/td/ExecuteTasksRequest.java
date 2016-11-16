package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.TaskDispatcher.TaskProducer;
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
        
        if (task.getTaskDescription().isReplicated()) {
            // Method annotation forces to replicate task to all nodes
            ResourceScheduler<P, T>[] resources = ts.getWorkers();
            task.setExecutionCount(resources.length);
            for (ResourceScheduler<P, T> rs : resources) {
                MasterExecutionAction<P, T> singleExec = new MasterExecutionAction<>(ts.generateSchedulingInformation(), producer, task, rs);
                ts.newAllocatableAction(singleExec); 
            }
        } else {
            // Normal task
            int numNodes = task.getTaskDescription().getNumNodes();
            task.setExecutionCount(numNodes);
            
            // Can use one or more resources depending on the computingNodes
            // Launch the master task and slaves if needed
            MasterExecutionAction<P, T> masterExec = new MasterExecutionAction<>(ts.generateSchedulingInformation(), producer, task, null);
            ts.newAllocatableAction(masterExec);
            
            int numSlaveNodes = numNodes - 1;
            for (int i = 0; i < numSlaveNodes; ++i) {
                SlaveExecutionAction<P, T> slaveExec = new SlaveExecutionAction<>(ts.generateSchedulingInformation(), producer, task, null);
                ts.newAllocatableAction(slaveExec);
            }
        }

        if (debug) {
            logger.debug("Treated Scheduling request for task " + task.getId() + "(core " + coreID + ")");
        }
    }

    @Override
    public TDRequestType getType() {
        return TDRequestType.EXECUTE_TASKS;
    }

}
