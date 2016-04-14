package integratedtoolkit.components.impl;

import org.apache.log4j.Logger;

import integratedtoolkit.components.scheduler.SchedulerPolicies;
import integratedtoolkit.ITConstants;
import integratedtoolkit.components.ResourceUser.WorkloadStatus;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ErrorManager;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;


public abstract class TaskScheduler {

    // Components
    protected JobManager JM;
    protected SchedulerPolicies schedulerPolicies;

    // Logger
    protected static final Logger logger = Logger.getLogger(Loggers.TS_COMP);
    protected static final boolean debug = logger.isDebugEnabled();

    // Preschedule
    protected static final boolean presched = System.getProperty(ITConstants.IT_PRESCHED) != null
            && System.getProperty(ITConstants.IT_PRESCHED).equals("true")
            ? true : false;

    // Tasks running
    private HashMap<Worker<?>, List<Task>> nodeToRunningTasks;
    
    //Data to calculate mean time of running tasks per coreId
    //running tasks per core type
    private int[] runningCount;
    //accumulated mean start time per core type
    private long[] accumulatedInit;
    //Reference time to accumulate tasks init time
    private static final long referenceTime = System.currentTimeMillis();
	private static final int MAX_RETRIES = 3;
    
    //Task Stats
    protected ExecutionProfile[][] profile;
    

    public TaskScheduler() {
        if (nodeToRunningTasks == null) {
            nodeToRunningTasks = new HashMap<Worker<?>, List<Task>>();
        } else {
            nodeToRunningTasks.clear();
        }
        int coreCount = CoreManager.getCoreCount();
        runningCount = new int[coreCount];
        accumulatedInit = new long[coreCount];
        profile = new ExecutionProfile[coreCount][];
        //taskCountToEnd = new int[CoreManager.coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
        	runningCount[coreId] = 0; 
        	accumulatedInit[coreId] = 0l;
            int implCount = CoreManager.getCoreImplementations(coreId).length;
            profile[coreId] = new ExecutionProfile[implCount];
            for (int implId = 0; implId < implCount; implId++) {
                profile[coreId][implId] = new ExecutionProfile();
            }
        }
    }

    public void setCoWorkers(JobManager JM) {
        this.JM = JM;
        this.schedulerPolicies.JM = JM;
    }

    /**
     * Resizes all the internal data structures to enable them to manage a
     * different number of TaskParams Elements.
     */
    public void resizeDataStructures() {
        int coreCount = CoreManager.getCoreCount();
        ExecutionProfile[][] profileTmp = new ExecutionProfile[coreCount][];
        for (int coreId = 0; coreId < profile.length; coreId++) {
            int implCount = CoreManager.getCoreImplementations(coreId).length;
            profileTmp[coreId] = new ExecutionProfile[implCount];
            System.arraycopy(profile[coreId], 0, profileTmp[coreId], 0, profile[coreId].length);
            for (int implId = profile[coreId].length; implId < implCount; implId++) {
                profileTmp[coreId][implId] = new ExecutionProfile();
            }
        }
        for (int coreId = profile.length; coreId < coreCount; coreId++) {
            int implCount = CoreManager.getCoreImplementations(coreId).length;
            profileTmp[coreId] = new ExecutionProfile[implCount];
            for (int implId = profile[coreId].length; implId < implCount; implId++) {
                profileTmp[coreId][implId] = new ExecutionProfile();
            }
        }

        //int[] taskCountToEnd = new int[coreCount];
        //System.arraycopy(this.taskCountToEnd, 0, taskCountToEnd, 0, this.taskCountToEnd.length);
        profile = profileTmp;

    }

    /**
     ********************************************
     *
     * Pending Work Query
     *
     ********************************************
     */
    /**
     * Checks if there is pending tasks to be executed for a given core
     *
     * @param coreId Identifier of the core whose tasks presence has to be
     * tested
     * @return {@literal true} - if there are pending tasks to be executed
     */
    public abstract boolean isPendingWork(Integer coreId);

    /**
     ********************************************
     *
     * Resource Management
     *
     ********************************************
     */
    /**
     * Notifies the availability of new resources to the Task Scheduler.
     *
     * Adds or increases the capacity of a resource if the scheduling system.
     * Once the resource capacity has been added/increased, it tries to schedule
     * and submit pending tasks on the resource.
     *
     * @param res name of the augmented/created node
     */
    public void resourcesCreated(Worker<?> res) {
			nodeToRunningTasks.put(res, new LinkedList<Task>());
    	
    }

    /**
     * Notifies the unavailability of some existing resources to the Task
     * Scheduler.
     *
     * Removes from the scheduling system a set of capabilities of one resource.
     * So the removed resource are no longer considered in the scheduling
     * algorithm; and therefore, no more tasks are scheduled to them.
     *
     * @param res Description on the resource reduction
     */
    public abstract void reduceResource(Worker<?> res);

    /**
     * Removes a resource and all its slots from the scheduling system.
     *
     * The resource passed as a parameter and all bounded slots are completely
     * removed from the scheduling systes. Before calling this method, the user
     * must ensure that no tasks are running on the resource slots.
     *
     * @param resource Resource to be removed
     */
    public void removeNode(Worker<?> resource) {
    		nodeToRunningTasks.remove(resource);
    }

    /**
     * Tries to perform a pending modification on a resource.
     *
     * In the case that there exists a pending modification for the specified
     * resource, it checks if there are enough free slots to actually perform
     * the modification.
     *
     * When the amount of free slots is lower than the required by the
     * modification, it returns null and mantains all the modification request
     * in a pending state. Otherwise, if there are enough resources, the method
     * commits the modification
     *
     *
     * @param resource Name of the resource to be modified
     * @return {@literal true} if the pending modification has been commited
     */
    ///public abstract boolean performModification(String resource);
    /**
     * Checks the computing ability to compute tasks.
     *
     * @param resource Name of the resource whose computing ability has to be
     * tested.
     * @return {@literal true} - if the resource still has some slots to compute
     */
    //public abstract boolean canResourceCompute(String resource);
    /**
     ********************************************
     *
     * Task Scheduling
     *
     ********************************************
     */
    /**
     * Schedules a task execution on an available resource.
     *
     * Given a task passed as a parameter, it looks for an available resource
     * where to execute it. If there is no slot able to host the execution
     * (because of the task constraints or the slot occupation) the tasks is
     * stored to be executed later. Otherwise, if there is some resource that
     * fulfills the task constraints and one free slot, the task execution is
     * submitted to the resource via the Job Manager.
     *
     * @param task Task whose execution has to be scheduled
     */
    public abstract void scheduleTask(Task task);

    /**
     *
     * Reschedules a task execution on an available resource different from the
     * one where it already failed.
     *
     * Given a task passed as a parameter, it looks for an available resource
     * where to execute it. If there is no slot able to host the execution
     * (because of the task constraints or the slot occupation) the tasks is
     * stored to be executed later. Otherwise, if there is some resource that
     * fulfills the task constraints and one free slot, the task execution is
     * submitted to the resource via the Job Manager.
     *
     * The host where the task already run is ignored during the resource
     * selecting process.
     *
     * @param task Task whose execution has to be scheduled
     * @param failedResource Resource where the task execution failed
     */
    public abstract boolean rescheduleTask(Task task, Worker<?> failedResource);

    /**
     * Tries to find a pending task to run in a given resource and submits its
     * execution.
     *
     * It looks for a pending tasks that can be submitted to the resource passed
     * as a parameter. If there's no pending task that can run in the resource,
     * the method does nothing and returns false. Otherwise, if a pending task
     * whose constraints match the resource features is found, its execution is
     * submitted via the Job Manager.
     *
     * The method does not check the number of available slots on the resource.
     * It's responsibility of the user to check it.
     *
     * @param resource Resource where to run the chosen task.
     * @return {@literal true} if a pending task execution is submitted to the
     * resource
     */
    public abstract boolean scheduleToResource(Worker<?> resource);

    /**
     ********************************************
     *
     * Scheduling state operations
     *
     ********************************************
     */
    /**
     * Describes the current load of the scheduling system.
     *
     * @param state object describing the current state
     */
    public void getWorkloadState(WorkloadStatus state) {
        int coreCount = state.getCoreCount(); 
        for (int coreId = 0; coreId < coreCount; coreId++) {
        	long[] stats = getCoreStats(coreId, 100l);
            state.registerTimes(coreId, stats[1], stats[2], stats[3]);
            long currentMeanRunningTime = System.currentTimeMillis()-referenceTime;
            if(runningCount[coreId]>0){
            	currentMeanRunningTime = currentMeanRunningTime - (accumulatedInit[coreId]/runningCount[coreId]);
            }
            state.registerRunningTask(coreId, runningCount[coreId], currentMeanRunningTime);
        }
        
       
        
    }

    private long[] getCoreStats(int coreId, long defaultValue) {
        long[] result = new long[4];

        int counter = 0;
        long maxTime = Long.MIN_VALUE;
        long minTime = Long.MAX_VALUE;
        long avgTime = 0l;
        for (int implId = 0; implId < CoreManager.getCoreImplementations(coreId).length; implId++) {
            if (profile[coreId][implId].executionCount > 0) {//Implementation has been executed
                counter += profile[coreId][implId].executionCount;
                avgTime += profile[coreId][implId].executionCount * profile[coreId][implId].avgExecutionTime;
                if (profile[coreId][implId].maxExecutionTime > maxTime) {
                    maxTime = profile[coreId][implId].maxExecutionTime;
                }
                if (profile[coreId][implId].minExecutionTime < minTime) {
                    minTime = profile[coreId][implId].minExecutionTime;
                }
            }
        }
        if (counter > 0) {
            result[0] = counter;
            result[1] = minTime;
            result[2] = avgTime / counter;
            result[3] = maxTime;
        } else {
            Task earlier = null;

            for (int implId = 0; implId < CoreManager.getCoreImplementations(coreId).length; implId++) {
                if (profile[coreId][implId].firstExecution != null) {
                    if (earlier == null) {
                        earlier = profile[coreId][implId].firstExecution;
                    } else {
                        if (earlier.getInitialTimeStamp() > profile[coreId][implId].firstExecution.getInitialTimeStamp()) {
                            earlier = profile[coreId][implId].firstExecution;
                        }
                    }
                }
            }
            if (earlier == null) {
                result[0] = 0;
                result[1] = defaultValue;
                result[2] = defaultValue;
                result[3] = defaultValue;
            } else {
                result[0] = 0;
                long difference = System.currentTimeMillis() - earlier.getInitialTimeStamp();
                result[1] = difference;
                result[2] = difference;
                result[3] = difference;
            }
        }
        return result;
    }

    /**
     * Returns the current core status data
     *
     * @return Cores information in XML format
     */
    public String getCoresMonitoringData(String prefix) {
        StringBuilder sb = new StringBuilder(prefix + "<CoresInfo>" + "\n");
        for (java.util.Map.Entry<String, Integer> entry : CoreManager.SIGNATURE_TO_ID.entrySet()) {
            int core = entry.getValue();
            String signature = entry.getKey();
            sb.append(prefix + "\t").append("<Core id=\"").append(core).append("\" signature=\"" + signature + "\">").append("\n");
            long stats[] = getCoreStats(core, 0);
            sb.append(prefix + "\t\t").append("<MeanExecutionTime>").append(stats[2]).append("</MeanExecutionTime>\n");
            sb.append(prefix + "\t\t").append("<MinExecutionTime>").append(stats[1]).append("</MinExecutionTime>\n");
            sb.append(prefix + "\t\t").append("<MaxExecutionTime>").append(stats[3]).append("</MaxExecutionTime>\n");
            sb.append(prefix + "\t\t").append("<ExecutedCount>").append(stats[0]).append("</ExecutedCount>\n");
            sb.append(prefix + "\t").append("</Core>").append("\n");
        }
        sb.append("\t</CoresInfo>\n");
        return sb.toString();
    }

    /**
     * Returns the current tasks that are executed on the given resource
     *
     * @param worker Resource to monitor
     * @return Current tasks data executed on the given resource. Null if no
     * tasks are being executed
     */
    public String getRunningTasksMonitorData(Worker<?> worker) {
        List<Task> tasks = nodeToRunningTasks.get(worker);
        if (tasks != null) {
            StringBuilder sb = new StringBuilder("");
            for (Task t : tasks) {
                sb.append(t.getId()).append(" ");
            }
            return sb.toString();
        } else {
            return null;
        }
    }

    protected boolean sendJob(Task task, Worker resource, Implementation<?> impl) {
        // Request the creation of the job  
        if (resource.runTask(impl.getRequirements())) {
            try {
				startsExecution(task, resource, impl.getImplementationId());
			} catch (Exception e) {
				logger.error("Exception starting execution",e);
				return false;
			}
            JM.newJob(task, impl, resource);
            return true;
        } else {
            return false;
        }
    }

    protected boolean sendJobRescheduled(Task task, Worker resource, Implementation impl) {
        // Request the creation of the job
        if (resource.runTask(impl.getRequirements())) {
        	try {
				startsExecution(task, resource, impl.getImplementationId());
			} catch (Exception e) {
				logger.error("Exception starting execution",e);
				return false;
			}
            JM.jobRescheduled(task, impl, resource);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Releases the slot where a task was running.
     *
     * Releases the slot where the task was running so another task can be
     * assigned to it. In addition, if the task execution finished properly, the
     * slot is released and the Task Scheduler updates core monitoring data.
     *
     * @param task Task whose execution has ended
     * @param resource Name of the resource where the exeuction run
     * @param implementationId Id of the implementation executed
     */
    public void taskEnd(Task task, Worker<?> resource, int implementationId) {
        // Obtain freed resource
        switch (task.getStatus()) {
            case FINISHED:
                endsExecution(task, resource, true, implementationId);
                break;
            case TO_RESCHEDULE:
                endsExecution(task, resource, false, implementationId);
                break;
            case FAILED:
                endsExecution(task, resource, false, implementationId);
                break;
            default: //This Task should not be here
            	ErrorManager.fatal("INVALID KIND OF TASK ENDED: " + task.getStatus());
                break;
        }
    }

    private void startsExecution(Task t, Worker<?> resource, int implId) throws Exception {
    	int retries = 0;
    	boolean added = false;
    	List<Task> tasks = null; nodeToRunningTasks.get(resource);
        while (retries < MAX_RETRIES){
        	tasks = nodeToRunningTasks.get(resource);
        	if (tasks!=null){
        		tasks.add(t);
        		retries = MAX_RETRIES;
        	}else{
        		if (retries >= MAX_RETRIES){
        			throw new Exception("Resource "+ resource.getName()+ " still not created.");
        		}
        		try{
        			Thread.sleep(10);
        		}catch(Exception e){
        			
        		}
        		retries ++;
        	}
        }
        int coreId = t.getTaskParams().getId();
        long startTime = System.currentTimeMillis();
        runningCount[coreId]++;
        accumulatedInit[coreId]+= (startTime-referenceTime) ;
        t.setInitialTimeStamp(startTime);
             
        if (profile[coreId][implId].firstExecution == null) {
            profile[coreId][implId].firstExecution = t;
        }
    }

    private void endsExecution(Task task, Worker<?> resource, boolean success, int implId) {
        nodeToRunningTasks.get(resource).remove(task);
        int core = task.getTaskParams().getId();
        runningCount[core]--;
        accumulatedInit[core]-= (task.getInitialTimeStamp()-referenceTime) ;
        if (success) {
            profile[core][implId].executionEnded(task);
        } else {
            if (profile[core][implId].firstExecution == task) {

                long firstTime = Long.MAX_VALUE;
                Task firstTask = null;
                for (List<Task> tasks : nodeToRunningTasks.values()) {
                    for (Task running : tasks) {
                        if (running.getTaskParams().getId() == core) {
                            if (firstTime > running.getInitialTimeStamp()) {
                                firstTask = running;
                            }
                        }
                    }
                }
                profile[core][implId].firstExecution = firstTask;
            }
        }
    }

    public class ExecutionProfile {

        private Task firstExecution;
        private int executionCount;

        private long avgExecutionTime;
        private long maxExecutionTime;
        private long minExecutionTime;

        public ExecutionProfile() {
            executionCount = 0;
            firstExecution = null;
            avgExecutionTime = 0l;
            maxExecutionTime = 0l;
            minExecutionTime = Long.MAX_VALUE;
        }

        public void executionEnded(Task task) {
            long initialTime = task.getInitialTimeStamp();
            long duration = System.currentTimeMillis() - initialTime;
            Long mean = avgExecutionTime;
            if (mean == null) {
                mean = 0l;
            }
            if (maxExecutionTime < duration) {
                maxExecutionTime = duration;
            }
            if (minExecutionTime > duration) {
                minExecutionTime = duration;
            }
            avgExecutionTime = ((mean * executionCount) + duration) / (executionCount + 1);
            executionCount++;
        }

        public int getExecutionCount() {
            return executionCount;
        }

        public Long getMinExecutionTime(Long defaultValue) {
            if (executionCount > 0) {
                return minExecutionTime;
            } else {
                if (firstExecution == null) {
                    return defaultValue;
                } else {
                    return System.currentTimeMillis() - firstExecution.getInitialTimeStamp();
                }
            }
        }

        public Long getMaxExecutionTime(Long defaultValue) {
            if (executionCount > 0) {
                return maxExecutionTime;
            } else {
                if (firstExecution == null) {
                    return defaultValue;
                } else {
                    return System.currentTimeMillis() - firstExecution.getInitialTimeStamp();
                }
            }
        }

        public Long getAverageExecutionTime(Long defaultValue) {
            if (executionCount > 0) {
                return avgExecutionTime;
            } else {
                if (firstExecution == null) {
                    return defaultValue;
                } else {
                    return System.currentTimeMillis() - firstExecution.getInitialTimeStamp();
                }
            }
        }

    }
}
