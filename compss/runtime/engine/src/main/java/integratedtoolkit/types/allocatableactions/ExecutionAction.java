package integratedtoolkit.types.allocatableactions;

import integratedtoolkit.comm.Comm;
import integratedtoolkit.components.impl.TaskProducer;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Task;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.SchedulingInformation;
import integratedtoolkit.types.Score;
import integratedtoolkit.util.ResourceScheduler;
import integratedtoolkit.types.data.operation.JobTransfersListener;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.JobListener.JobEndStatus;
import integratedtoolkit.types.job.JobStatusListener;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.JobDispatcher;

import java.util.Iterator;
import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class ExecutionAction<P extends Profile, T extends WorkerResourceDescription> extends AllocatableAction<P, T> {

    // Fault tolerance parameters
    private static final int TRANSFER_CHANCES = 2;
    private static final int SUBMISSION_CHANCES = 2;
    private static final int SCHEDULING_CHANCES = 2;

    // LOGGER
    private static final Logger JOB_LOGGER = LogManager.getLogger(Loggers.JM_COMP);

    // Execution Info
    private final TaskProducer producer;
    protected final Task task;
    private int transferErrors = 0;
    private int executionErrors = 0;
    private LinkedList<Integer> jobs = new LinkedList<>();
    
    // Resource execution information
    private final ResourceScheduler<P,T> forcedResource;
    private T resourceConsumption;


    /**
     * Creates a new execution action
     * 
     * @param schedulingInformation
     * @param producer
     * @param task
     * @param forcedResource
     */
    @SuppressWarnings("unchecked")
    public ExecutionAction(SchedulingInformation<P, T> schedulingInformation, TaskProducer producer, Task task,
            ResourceScheduler<P,T> forcedResource) {

        super(schedulingInformation);

        this.producer = producer;
        this.task = task;
        this.forcedResource = forcedResource;
        task.addExecution(this);

        // Register data dependencies events
        for (Task predecessor : task.getPredecessors()) {
            for (ExecutionAction<?, ?> e : predecessor.getExecutions()) {
                if (e != null && e.isPending()) {
                    this.addDataPredecessor((ExecutionAction<P, T>) e);
                }
            }
        }

        // Scheduling constraints
        // Restricted resource
        Task resourceConstraintTask = task.getEnforcingTask();
        if (resourceConstraintTask != null) {
            for (ExecutionAction<?, ?> e : resourceConstraintTask.getExecutions()) {
                this.addResourceConstraint((ExecutionAction<P, T>) e);
            }
        }
    }

    /**
     * Returns the associated task
     * @return
     */
    public Task getTask() {
        return this.task;
    }
    
    /*
     * SCHEDULING OPERATIONS
     * 
     */
    @Override
    protected boolean areEnoughResources() {
        Worker<T> w = selectedResource.getResource();
        return w.canRunNow(selectedImpl.getRequirements());
    }

    @Override
    protected void reserveResources() {
        Worker<T> w = selectedResource.getResource();
        resourceConsumption = w.runTask(selectedImpl.getRequirements());
    }

    @Override
    protected void releaseResources() {
        Worker<T> w = selectedResource.getResource();
        w.endTask(resourceConsumption);
    }

    /*
     * EXECUTION OPERATIONS
     * 
     */
    @Override
    protected void doAction() {
        JOB_LOGGER.info("Ordering transfers to " + selectedResource + " to run task: " + task.getId());
        transferErrors = 0;
        executionErrors = 0;
        doInputTransfers();
    }

    private void doInputTransfers() {
        JobTransfersListener<P, T> listener = new JobTransfersListener<>(this);
        transferInputData(listener);
        listener.enable();
    }

    protected abstract void transferInputData(JobTransfersListener<P, T> listener);

    // EXECUTED BY SUPPORTING THREAD ON JOB_TRANSFERS_LISTENER
    /**
     * Code executed after some input transfers have failed
     * 
     * @param failedtransfers
     */
    public void failedTransfers(int failedtransfers) {
        JOB_LOGGER.debug("Received a notification for the transfers for task " + task.getId() + " with state FAILED");
        ++transferErrors;
        if (transferErrors < TRANSFER_CHANCES) {
            JOB_LOGGER.debug("Resubmitting input files for task " + task.getId() + " to host " + selectedResource.getName() + " since "
                    + failedtransfers + " transfers failed.");

            doInputTransfers();
        } else {
            ErrorManager
                    .warn("Transfers for running task " + task.getId() + " on worker " + selectedResource.getName() + " have failed.");
            this.notifyError();
        }
    }

    // EXECUTED BY SUPPORTING THREAD ON JOB_TRANSFERS_LISTENER
    /**
     * Code executed when all transfers have successed
     * 
     * @param transferGroupId
     */
    public void doSubmit(int transferGroupId) {
        JOB_LOGGER.debug("Received a notification for the transfers of task " + task.getId() + " with state DONE");
        JobStatusListener<P, T> listener = new JobStatusListener<>(this);
        Job<?> job = submitJob(transferGroupId, listener);

        // Register job
        jobs.add(job.getJobId());
        JOB_LOGGER.info(
                (this.executingResources.size() > 1 ? "Rescheduled" : "New") + " Job " + job.getJobId() + " (Task: " + task.getId() + ")");
        JOB_LOGGER.info("  * Method name: " + task.getTaskDescription().getName());
        JOB_LOGGER.info("  * Target host: " + selectedResource.getName());
        profile.start();
        JobDispatcher.dispatch(job);
    }

    protected abstract Job<?> submitJob(int transferGroupId, JobStatusListener<P, T> listener);

    // EXECUTED BY SUPPORTING THREAD ON JOB_STATUS_LISTENER
    /**
     * Code executed when the job execution has failed
     * 
     * @param job
     * @param endStatus
     */
    public void failedJob(Job<?> job, JobEndStatus endStatus) {
        profile.end();
        int jobId = job.getJobId();
        JOB_LOGGER.error("Received a notification for job " + jobId + " with state FAILED");
        ++executionErrors;
        if (transferErrors + executionErrors < SUBMISSION_CHANCES) {
            JOB_LOGGER.error("Job " + job.getJobId() + " for running task " + task.getId() + " on worker " + selectedResource.getName()
                    + " has failed; resubmitting task to the same worker.");
            ErrorManager.warn("Job " + job.getJobId() + " for running task " + task.getId() + " on worker " + selectedResource.getName()
                    + " has failed; resubmitting task to the same worker.");
            job.setHistory(Job.JobHistory.RESUBMITTED);
            profile.start();
            JobDispatcher.dispatch(job);
        } else {
            this.notifyError();
        }
    }

    // EXECUTED BY SUPPORTING THREAD ON JOB_STATUS_LISTENER
    /**
     * Code executed when the job execution has been completed
     * 
     * @param job
     */
    public void completedJob(Job<?> job) {
        // End profile
        profile.end();

        // Notify end
        int jobId = job.getJobId();
        JOB_LOGGER.info("Received a notification for job " + jobId + " with state OK");

        // Job finished, update info about the generated/updated data
        doOutputTransfers(job);

        // Notify completion
        this.notifyCompleted();
    }

    protected abstract void doOutputTransfers(Job<?> job);

    @Override
    protected void doCompleted() {
        selectedResource.profiledExecution(selectedImpl, profile);
        task.setStatus(Task.TaskState.FINISHED);
        producer.notifyTaskEnd(task);
    }

    @Override
    protected void doError() throws FailedActionException {
        if (this.executingResources.size() >= SCHEDULING_CHANCES) {
            logger.error("Task " + task.getId() + " has already been rescheduled; notifying task failure.");
            ErrorManager.warn("Task " + task.getId() + " has already been rescheduled; notifying task failure.");
            throw new FailedActionException();
        } else {
            ErrorManager.warn("Task " + task.getId() + " execution on worker " + selectedResource.getName()
                    + " has failed; rescheduling task execution. (changing worker)");
            logger.error("Task " + task.getId() + " execution on worker " + selectedResource.getName()
                    + " has failed; rescheduling task execution. (changing worker)");
        }
    }

    @Override
    protected void doFailed() {
        String taskName = task.getTaskDescription().getName();
        StringBuilder sb = new StringBuilder();
        sb.append("Task '").append(taskName).append("' TOTALLY FAILED.\n");
        sb.append("Possible causes:\n");
        sb.append("     -Exception thrown by task '").append(taskName).append("'.\n");
        sb.append("     -Expected output files not generated by task '").append(taskName).append("'.\n");
        sb.append("     -Could not provide nor retrieve needed data between master and worker.\n");
        sb.append("\n");
        sb.append("Check files '").append(Comm.getAppHost().getJobsDirPath()).append("job[");
        Iterator<Integer> j = jobs.iterator();
        while (j.hasNext()) {
            sb.append(j.next());
            if (!j.hasNext()) {
                break;
            }
            sb.append("|");
        }
        sb.append("'] to find out the error.\n");
        /*
         * sb.append("Task was scheduled on '"); Iterator<Worker> r = this.executingResources.iterator(); while (true) {
         * sb.append(r.next()); if (!r.hasNext()) { break; } sb.append(","); } sb.append(".\n");
         */
        sb.append(" \n");
        ErrorManager.warn(sb.toString());

        task.setStatus(Task.TaskState.FAILED);
        producer.notifyTaskEnd(task);
    }

    public static void shutdown() {
        // Cancel all submitted jobs
        JobDispatcher.stop();
    }

    @Override
    public LinkedList<ResourceScheduler<?, ?>> getCompatibleWorkers() {
        return getCoreElementExecutors(task.getTaskDescription().getId());
    }

    @Override
    public LinkedList<Implementation<T>> getCompatibleImplementations(ResourceScheduler<P, T> r) {
        return r.getExecutableImpls(task.getTaskDescription().getId());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Implementation<T>[] getImplementations() {
        return (Implementation<T>[]) CoreManager.getCoreImplementations(task.getTaskDescription().getId());
    }

    @Override
    public boolean isCompatible(Worker<T> r) {
        return r.canRun(task.getTaskDescription().getId());
    }

    @Override
    public Integer getCoreId() {
        return task.getTaskDescription().getId();
    }

    @Override
    public int getPriority() {
        return task.getTaskDescription().hasPriority() ? 1 : 0;
    }
    
    @Override
    public Score schedulingScore(ResourceScheduler<P, T> targetWorker, Score actionScore) {
        return targetWorker.getResourceScore(this, task.getTaskDescription(), actionScore);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        StringBuilder debugString = new StringBuilder("Scheduling " + this + " execution:\n");
        
        // COMPUTE RESOURCE CANDIDATES
        LinkedList<ResourceScheduler<?, ?>> candidates = new LinkedList<>();
        if (this.forcedResource != null) {
            // The scheduling is forced to a given resource
            candidates.add(this.forcedResource);
        } else if (isSchedulingConstrained()) {
            // The scheduling is constrained by dependencies
            for (AllocatableAction<P, T> a : this.getConstrainingPredecessors()) {
                candidates.add(a.getAssignedResource());
            }
        } else {
            // Free scheduling
            candidates = getCompatibleWorkers();
        }

        // COMPUTE BEST IMPLEMENTATIONS
        ResourceScheduler<P, T> bestWorker = null;
        Implementation<T> bestImpl = null;
        Score bestScore = null;
        int usefulResources = 0;
        for (ResourceScheduler<?, ?> w : candidates) {
            ResourceScheduler<P, T> worker = (ResourceScheduler<P, T>) w;
            if (executingResources.contains(w)) {
                continue;
            }
            Score resourceScore = worker.getResourceScore(this, task.getTaskDescription(), actionScore);
            usefulResources++;
            for (Implementation<T> impl : getCompatibleImplementations(worker)) {
                Score implScore = worker.getImplementationScore(this, task.getTaskDescription(), impl, resourceScore);
                debugString.append(" Resource ").append(w.getName()).append(" ").append(" Implementation ")
                        .append(impl.getImplementationId()).append(" ").append(" Score ").append(implScore).append("\n");
                if (Score.isBetter(implScore, bestScore)) {
                    bestWorker = worker;
                    bestImpl = impl;
                    bestScore = implScore;
                }
            }
        }
        
        // CHECK SCHEDULING RESULT
        if (bestWorker == null) {
            logger.debug(debugString.toString());
            if (usefulResources == 0) {
                logger.info("No worker can run " + this);
                throw new BlockedActionException();
            } else {
                logger.info("No worker has available resources to run " + this);
                throw new UnassignedActionException();
            }
        }

        this.assignImplementation(bestImpl);
        this.assignResources(bestWorker);
        logger.debug(debugString.toString());
        logger.info("Assigning action " + this + " to worker" + bestWorker + " with implementation " + bestImpl.getImplementationId());
        bestWorker.initialSchedule(this);
    }

    @Override
    public void schedule(ResourceScheduler<P, T> targetWorker, Score actionScore) throws BlockedActionException, UnassignedActionException {
        StringBuilder debugString = new StringBuilder("Scheduling " + this + " execution for worker " + targetWorker + ":\n");
        ResourceScheduler<P, T> bestWorker = null;
        Implementation<T> bestImpl = null;
        Score bestScore = null;

        if ( // Resource is not compatible with the Core
                !targetWorker.getResource().canRun(task.getTaskDescription().getId())
                // already ran on the resource
                || executingResources.contains(targetWorker)) {
            throw new UnassignedActionException();
        }
        Score resourceScore = targetWorker.getResourceScore(this, task.getTaskDescription(), actionScore);
        debugString.append("\t Resource ").append(targetWorker.getName()).append("\n");

        for (Implementation<T> impl : getCompatibleImplementations(targetWorker)) {
            Score implScore = targetWorker.getImplementationScore(this, task.getTaskDescription(), impl, resourceScore);
            debugString.append("\t\t Implementation ").append(impl.getImplementationId()).append(implScore).append("\n");
            if (Score.isBetter(implScore, bestScore)) {
                bestWorker = targetWorker;
                bestImpl = impl;
                bestScore = implScore;
            }
        }

        if (bestWorker == null) {
            logger.info("\tWorker " + targetWorker.getName() + "has available resources to run " + this);
            throw new UnassignedActionException();
        }

        this.assignImplementation(bestImpl);
        this.assignResources(bestWorker);
        logger.info("\t Worker" + bestWorker + " Implementation " + bestImpl.getImplementationId());
        logger.debug(debugString.toString());
        bestWorker.initialSchedule(this);
    }

    @Override
    public void schedule(ResourceScheduler<P, T> targetWorker, Implementation<T> impl)
            throws BlockedActionException, UnassignedActionException {
        
        StringBuilder debugString = new StringBuilder("Scheduling " + this + " execution for worker " + targetWorker + ":\n");

        if ( // Resource is not compatible with the implementation
                !targetWorker.getResource().canRun(impl)
                // already ran on the resource
                || executingResources.contains(targetWorker)) {
            throw new UnassignedActionException();
        }

        this.assignImplementation(impl);
        this.assignResources(targetWorker);
        logger.info("\t Worker" + targetWorker + " Implementation " + impl.getImplementationId());
        logger.debug(debugString.toString());
        targetWorker.initialSchedule(this);
    }

    @Override
    public String toString() {
        return "ExecutionAction ( Task " + task.getId() + ", CE name " + task.getTaskDescription().getName() + ")";
    }

}
