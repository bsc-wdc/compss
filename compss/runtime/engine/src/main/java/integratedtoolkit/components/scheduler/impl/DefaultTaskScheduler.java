package integratedtoolkit.components.scheduler.impl;

import integratedtoolkit.components.ResourceUser.WorkloadStatus;
import integratedtoolkit.components.impl.JobManager;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.components.scheduler.SchedulerPolicies;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.resources.Worker;

import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.TaskSets;
import integratedtoolkit.util.ResourceManager;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;


public class DefaultTaskScheduler extends TaskScheduler {

    // Max number of tasks to examine when scheduling
    private static final int MAX_TASK = 100;

    // Object that stores the information about the current project
    private final TaskSets taskSets;

    public DefaultTaskScheduler() {
        super();
        taskSets = new TaskSets();
        schedulerPolicies = new DefaultSchedulerPolicies();
        logger.info("Initialization finished");
    }

    public void resizeDataStructures() {
        super.resizeDataStructures();
        taskSets.resizeDataStructures();
    }

    /**
     ********************************************
     *
     * Pending Work Query
     *
     ********************************************
     */
    public boolean isPendingWork(Integer coreId) {
        return (taskSets.getToRescheduleCount(coreId) + taskSets.getNoResourceCount(coreId) + taskSets.getPriorityCount(coreId) + taskSets.getRegularCount(coreId)) != 0;
    }

    /**
     ********************************************
     *
     * Resource Management
     *
     ********************************************
     */
    @Override
    public void resourcesCreated(Worker<?> res) {
        super.resourcesCreated(res);
        logger.info("Resource " + res.getName() + " created");
        if (taskSets.getNoResourceCount() > 0) {
            int[] simTasks = res.getSimultaneousTasks();
            for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
                if (taskSets.getNoResourceCount(coreId) > 0 && simTasks[coreId] > 0) {
                    taskSets.resourceFound(coreId);
                }
            }
        }
        scheduleToResource(res);
    }

    @Override
    public void reduceResource(Worker<?> res) {

    }

    public void removeNode(Worker<?> res) {
        super.removeNode(res);
    }

    /**
     ********************************************
     *
     * Task Scheduling
     *
     ********************************************
     */
    public void scheduleTask(Task currentTask) {
        Worker<?> chosenResource = null;
        int coreId = currentTask.getTaskParams().getId();
        if (currentTask.isSchedulingForced()) {
            //Task is forced to run in a given resource
            chosenResource = JM.enfDataToService.get(currentTask.getEnforcingData().getDataId());
        }
        if (chosenResource != null) {
            logger.info("Task " + currentTask.getId() + " forced to run in " + chosenResource.getName());
            LinkedList<Implementation<?>> runnable = chosenResource.getRunnableImplementations(coreId);
            if (runnable.isEmpty()) {
                taskSets.newRegularTask(currentTask);
                logger.info("Pending: Task(" + currentTask.getId() + ", "
                        + currentTask.getTaskParams().getName() + ") "
                        + "Resource(" + chosenResource.getName() + ")");
            } else {
                LinkedList<Implementation<?>> run = schedulerPolicies.sortImplementationsForResource(runnable, chosenResource, profile);
                logger.info("Match: Task(" + currentTask.getId() + ", "
                        + currentTask.getTaskParams().getName() + ") "
                        + "Resource(" + chosenResource.getName() + ")");

                // Request the creation of a job for the task
                logger.info("Sending job " + currentTask + ", to res name " + chosenResource.getName() + ", resource " + chosenResource + ", with impl " + run.getFirst());
                sendJob(currentTask, chosenResource, run.getFirst());
            }
        } else {
            // Schedule task
            LinkedList<Worker<?>> validResources = ResourceManager.findCompatibleWorkers(coreId);
            if (validResources.isEmpty()) {
                //There's no point on getting scores, no existing machines can run this task <- score=0
                taskSets.waitWithoutNode(currentTask);
                logger.info("Blocked: Task(" + currentTask.getId() + ", " + currentTask.getTaskParams().getName() + ") ");
            } else {
                // Try to assign task to available resources
                HashMap<Worker<?>, LinkedList<Implementation<?>>> resourceToImpls = ResourceManager.findAvailableWorkers(validResources, coreId);
                PriorityQueue<SchedulerPolicies.ObjectValue<Worker<?>>> orderedResources = schedulerPolicies.sortResourcesForTask(currentTask, resourceToImpls.keySet(), profile);
                for (SchedulerPolicies.ObjectValue<Worker<?>> entry : orderedResources) {
                    chosenResource = (Worker<?>) entry.o;
                    LinkedList<Implementation<?>> orderedImpls = schedulerPolicies.sortImplementationsForResource(resourceToImpls.get(chosenResource), chosenResource, profile);
                    logger.info("Match: Task(" + currentTask.getId() + ", "
                            + currentTask.getTaskParams().getName() + ") "
                            + "Resource(" + chosenResource.getName() + ")");
                    if (sendJob(currentTask, chosenResource, orderedImpls.getFirst())) {
                        return;
                    }
                    else {
                    	//TODO: treat error
                    }
                }
                if (currentTask.getTaskParams().hasPriority()) {
                    taskSets.newPriorityTask(currentTask);
                } else {
                    taskSets.newRegularTask(currentTask);
                }
                logger.info("Pending: Task(" + currentTask.getId() + ", " + currentTask.getTaskParams().getName() + ")");
            }
        }
    }

    public boolean rescheduleTask(Task task, Worker<?> failedResource) {
        //Rescheduling the failed Task
        // Find available resources that match user constraints for this task
        int coreId = task.getTaskParams().getId();
        LinkedList<Worker<?>> validResources = ResourceManager.findCompatibleWorkers(coreId);
        if (!validResources.isEmpty()) {
            HashMap<Worker<?>, LinkedList<Implementation<?>>> resourceToImpls = ResourceManager.findAvailableWorkers(validResources, coreId);
            resourceToImpls.remove(failedResource);
            PriorityQueue<SchedulerPolicies.ObjectValue<Worker<?>>> orderedResources = schedulerPolicies.sortResourcesForTask(task, resourceToImpls.keySet(), profile);
            
            for (SchedulerPolicies.ObjectValue<Worker<?>> entry : orderedResources) {
                Worker<?> chosenResource = entry.o;
                // Request the creation of a job for the task
                LinkedList<Implementation<?>> orderedImpls = schedulerPolicies.sortImplementationsForResource(resourceToImpls.get(chosenResource), chosenResource, profile);
                logger.info("Match: Task(" + task.getId() + ", "
                        + task.getTaskParams().getName() + ") "
                        + "Resource(" + chosenResource.getName() + ")");
                if( sendJobRescheduled(task, chosenResource, orderedImpls.getFirst()) ) {
                	return true;
                }
            }

        	/* Arrived to this point, no valid worker has been found to reschedule, currently throwing error and shutting down COMPSs
            //Set for rescheduling again
            taskSets.newTaskToReschedule(task);
            logger.info("To Reschedule: Task(" + task.getId() + ", " + task.getTaskParams().getName() + ") ");   
        	 */
            
            if(resourceToImpls.isEmpty()) {
                //Rescheduling failed. No valid worker found

            	int jobId = task.getId();
            	JobManager.notifyJobErrorAndShutdown(task.getTaskParams().getName(), jobId);
            }
            
            return false;
            
        } else {
        	//TODO: add other resource creation checks
            taskSets.newTaskToReschedule(task);
            logger.info("To Reschedule: Task(" + task.getId() + ", " + task.getTaskParams().getName() + ") ");
        	return false;
        }
    }

    public boolean scheduleToResource(Worker<?> resource) {
        boolean assigned = false;
        LinkedList<Integer> compatibleCores = resource.getExecutableCores();
        LinkedList<Integer> executableCores = new LinkedList<Integer>();
        LinkedList<Implementation<?>>[] fittingImplementations = new LinkedList[CoreManager.getCoreCount()];
        LinkedList<Implementation<?>>[] executableCoreImpls = resource.getRunnableImplementations();

        for (int coreId = 0; coreId < executableCoreImpls.length; ++coreId) {
            fittingImplementations[coreId]
                    = schedulerPolicies.sortImplementationsForResource(resource.getRunnableImplementations(coreId), resource, profile);
        }

        // First check if there is some task to reschedule
        if (taskSets.areTasksToReschedule()) {
            for (Integer coreId : compatibleCores) {
                if (!fittingImplementations[coreId].isEmpty()) {
                    executableCores.add(coreId);
                }
            }
            LinkedList<Task>[] tasks = taskSets.getTasksToReschedule();

            assigned = assignTasks(tasks, executableCores, resource, fittingImplementations);
            executableCores.clear();
        }

        // Now assign, if possible, one of the pending tasks to the resource
        if (taskSets.arePriorityTasks()) {
            for (Integer coreId : compatibleCores) {
                if (!fittingImplementations[coreId].isEmpty()) {
                    executableCores.add(coreId);
                }
            }
            LinkedList<Task>[] tasks = new LinkedList[CoreManager.getCoreCount()];
            for (Integer coreId : executableCores) {
                tasks[coreId] = trimTaskList(taskSets.getPriorityTasks()[coreId]);
            }
            assigned = assigned || assignTasks(tasks, executableCores, resource, fittingImplementations);
            executableCores.clear();
        }

        if (taskSets.areRegularTasks()) {
            for (Integer coreId : compatibleCores) {
                if (!fittingImplementations[coreId].isEmpty()) {
                    executableCores.add(coreId);
                }
            }
            LinkedList<Task>[] tasks = new LinkedList[CoreManager.getCoreCount()];
            for (Integer coreId : executableCores) {
                tasks[coreId] = trimTaskList(taskSets.getRegularTasks()[coreId]);
            }
            assigned = assigned || assignTasks(tasks, executableCores, resource, fittingImplementations);
            executableCores.clear();
        }

        if (debug && !assigned) {
            logger.debug("Resource " + resource.getName() + " FREE");
        }
        return assigned;
    }

    private LinkedList<Task> trimTaskList(LinkedList<Task> original) {
        LinkedList<Task> result = new LinkedList<Task>();
        Iterator<Task> it = original.iterator();
        while (it.hasNext() && result.size() < MAX_TASK) {
            result.add(it.next());
        }
        return result;
    }

    private boolean assignTasks(LinkedList<Task>[] tasks, LinkedList<Integer> executableCores, Worker<?> resource, LinkedList<Implementation<?>>[] fittingImplementations) {
        boolean assigned = false;
        PriorityQueue<SchedulerPolicies.ObjectValue<Task>>[] sortedTasks = new PriorityQueue[CoreManager.getCoreCount()];
        LinkedList<Integer> unloadedCores = new LinkedList<Integer>();
        for (Integer coreId : executableCores) {
            if (tasks[coreId] == null || tasks[coreId].size() == 0) {
                unloadedCores.add(coreId);
            } else {
                sortedTasks[coreId] = schedulerPolicies.sortTasksForResource(resource, tasks[coreId], profile);
                if (sortedTasks[coreId].isEmpty()) {
                    unloadedCores.add(coreId);
                }
            }
        }
        for (Integer coreId : unloadedCores) {
            executableCores.remove(coreId);
        }

        while (!executableCores.isEmpty()) {
            Integer coreId = null;
            int maxValue = Integer.MIN_VALUE;
            for (Integer i : executableCores) {
                if (sortedTasks[i].peek().value > maxValue) {
                    maxValue = sortedTasks[i].peek().value;
                    coreId = i;
                }
            }
            SchedulerPolicies.ObjectValue<Task> ov = sortedTasks[coreId].poll();
            Task t = ov.o;
            if (t.getLastResource() != null) {
                if (t.getLastResource().compareTo(resource.getName()) == 0) {
                    if (sortedTasks[coreId].isEmpty()) {
                        executableCores.remove(coreId);
                    }
                    continue;
                }
                taskSets.rescheduledTask(t);
            } else {
                if (t.getTaskParams().hasPriority()) {
                    taskSets.priorityTaskScheduled(t);
                } else {
                    taskSets.regularTaskScheduled(t);
                }
            }

            if (sendJob(t, resource, fittingImplementations[coreId].get(0))) {
                assigned = true;
                if (sortedTasks[coreId].isEmpty()) {
                    executableCores.remove(coreId);
                }
            } else {
                sortedTasks[coreId].offer(ov);
            }
            unloadedCores.clear();
            for (Integer i : executableCores) {
                fittingImplementations[i] = resource.canRunNow(fittingImplementations[t.getTaskParams().getId()]);
                if (fittingImplementations[i].isEmpty()) {
                    unloadedCores.add(i);
                }
            }
            for (Integer i : unloadedCores) {
                executableCores.remove(i);
            }

        }

        return assigned;
    }

    /**
     ********************************************
     *
     * Scheduling state operations
     *
     ********************************************
     */
    @Override
    public void getWorkloadState(WorkloadStatus ss) {
        super.getWorkloadState(ss);
        try {
            taskSets.getWorkloadState(ss);
        } catch (Exception e) {
        	ErrorManager.fatal("Cannot get the current schedule", e);
        }
    }

    @Override
    public String getCoresMonitoringData(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(super.getCoresMonitoringData(prefix));
        sb.append(taskSets.getMonitoringInfo());
        return sb.toString();
    }

}
