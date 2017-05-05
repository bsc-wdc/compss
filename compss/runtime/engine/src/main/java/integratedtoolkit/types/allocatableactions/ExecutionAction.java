package integratedtoolkit.types.allocatableactions;

import integratedtoolkit.comm.Comm;
import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.components.impl.TaskProducer;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.Task.TaskState;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.ActionOrchestrator;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.SchedulingInformation;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.data.DataAccessId;
import integratedtoolkit.types.data.DataInstanceId;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.operation.JobTransfersListener;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.Implementation.TaskType;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.JobListener.JobEndStatus;
import integratedtoolkit.types.parameter.DependencyParameter;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.job.JobStatusListener;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.types.uri.SimpleURI;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.JobDispatcher;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ExecutionAction<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends AllocatableAction<P, T, I> {

    // Fault tolerance parameters
    private static final int TRANSFER_CHANCES = 2;
    private static final int SUBMISSION_CHANCES = 2;
    private static final int SCHEDULING_CHANCES = 2;

    // LOGGER
    private static final Logger JOB_LOGGER = LogManager.getLogger(Loggers.JM_COMP);

    // Execution Info
    protected final TaskProducer producer;
    protected final Task task;
    private final LinkedList<Integer> jobs;
    private int transferErrors = 0;
    protected int executionErrors = 0;

    // Resource execution information
    private final ResourceScheduler<P, T, I> forcedResource;
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
    public ExecutionAction(SchedulingInformation<P, T, I> schedulingInformation, ActionOrchestrator<P, T, I> orchestrator,
            TaskProducer producer, Task task, ResourceScheduler<P, T, I> forcedResource) {

        super(schedulingInformation, orchestrator);

        this.producer = producer;
        this.task = task;
        this.jobs = new LinkedList<>();
        this.transferErrors = 0;
        this.executionErrors = 0;

        this.forcedResource = forcedResource;
        this.resourceConsumption = null;

        // Add execution to task
        this.task.addExecution(this);

        // Register data dependencies events
        for (Task predecessor : this.task.getPredecessors()) {
            for (ExecutionAction<?, ?, ?> e : predecessor.getExecutions()) {
                if (e != null && e.isPending()) {
                    addDataPredecessor((ExecutionAction<P, T, I>) e);
                }
            }
        }

        // Scheduling constraints
        // Restricted resource
        Task resourceConstraintTask = this.task.getEnforcingTask();
        if (resourceConstraintTask != null) {
            for (ExecutionAction<?, ?, ?> e : resourceConstraintTask.getExecutions()) {
                addResourceConstraint((ExecutionAction<P, T, I>) e);
            }
        }
    }

    /**
     * Returns the associated task
     * 
     * @return
     */
    public final Task getTask() {
        return this.task;
    }

    /*
     * ***************************************************************************************************************
     * EXECUTION AND LIFECYCLE MANAGEMENT
     * ***************************************************************************************************************
     */
    @Override
    public final boolean areEnoughResources() {
        Worker<T, I> w = selectedResource.getResource();
        return w.canRunNow(selectedImpl.getRequirements());
    }

    @Override
    protected final void reserveResources() {
        Worker<T, I> w = selectedResource.getResource();
        resourceConsumption = w.runTask(selectedImpl.getRequirements());
    }

    @Override
    protected final void releaseResources() {
        Worker<T, I> w = selectedResource.getResource();
        w.endTask(resourceConsumption);
    }

    @Override
    protected void doAction() {
        JOB_LOGGER.info("Ordering transfers to " + selectedResource + " to run task: " + task.getId());
        transferErrors = 0;
        executionErrors = 0;
        doInputTransfers();
    }

    private final void doInputTransfers() {
        JobTransfersListener<P, T, I> listener = new JobTransfersListener<>(this);
        transferInputData(listener);
        listener.enable();
    }

    private final void transferInputData(JobTransfersListener<P, T, I> listener) {
        TaskDescription taskDescription = task.getTaskDescription();
        for (Parameter p : taskDescription.getParameters()) {
            JOB_LOGGER.debug("    * " + p);
            if (p instanceof DependencyParameter) {
                DependencyParameter dp = (DependencyParameter) p;
                switch (taskDescription.getType()) {
                    case METHOD:
                        transferJobData(dp, listener);
                        break;
                    case SERVICE:
                        if (dp.getDirection() != Direction.INOUT) {
                            // For services we only transfer IN parameters because the only
                            // parameter that can be INOUT is the target
                            transferJobData(dp, listener);
                        }
                        break;
                }
            }
        }
    }

    // Private method that performs data transfers
    private final void transferJobData(DependencyParameter param, JobTransfersListener<P, T, I> listener) {
        Worker<T, I> w = selectedResource.getResource();
        DataAccessId access = param.getDataAccessId();
        if (access instanceof DataAccessId.WAccessId) {
            String tgtName = ((DataAccessId.WAccessId) access).getWrittenDataInstance().getRenaming();
            if (DEBUG) {
                JOB_LOGGER.debug("Setting data target job transfer: " + w.getCompleteRemotePath(param.getType(), tgtName));
            }
            param.setDataTarget(w.getCompleteRemotePath(param.getType(), tgtName).getPath());

            return;
        }

        listener.addOperation();
        if (access instanceof DataAccessId.RAccessId) {
            String srcName = ((DataAccessId.RAccessId) access).getReadDataInstance().getRenaming();
            w.getData(srcName, srcName, param, listener);
        } else {
            // Is RWAccess
            String srcName = ((DataAccessId.RWAccessId) access).getReadDataInstance().getRenaming();
            String tgtName = ((DataAccessId.RWAccessId) access).getWrittenDataInstance().getRenaming();
            w.getData(srcName, tgtName, (LogicalData) null, param, listener);
        }
    }

    /*
     * ***************************************************************************************************************
     * EXECUTED SUPPORTING THREAD ON JOB_TRANSFERS_LISTENER
     * ***************************************************************************************************************
     */
    /**
     * Code executed after some input transfers have failed
     * 
     * @param failedtransfers
     */
    public final void failedTransfers(int failedtransfers) {
        JOB_LOGGER.debug("Received a notification for the transfers for task " + task.getId() + " with state FAILED");
        ++transferErrors;
        if (transferErrors < TRANSFER_CHANCES) {
            JOB_LOGGER.debug("Resubmitting input files for task " + task.getId() + " to host " + selectedResource.getName() + " since "
                    + failedtransfers + " transfers failed.");

            doInputTransfers();
        } else {
            ErrorManager.warn("Transfers for running task " + task.getId() + " on worker " + selectedResource.getName() + " have failed.");
            this.notifyError();
        }
    }

    /**
     * Code executed when all transfers have successed
     * 
     * @param transferGroupId
     */
    public final void doSubmit(int transferGroupId) {
        JOB_LOGGER.debug("Received a notification for the transfers of task " + task.getId() + " with state DONE");
        JobStatusListener<P, T, I> listener = new JobStatusListener<>(this);
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

    protected Job<?> submitJob(int transferGroupId, JobStatusListener<P, T, I> listener) {
        // Create job
        if (DEBUG) {
            LOGGER.debug(this.toString() + " starts job creation");
        }
        Worker<T, I> w = selectedResource.getResource();
        List<String> slaveNames = new ArrayList<>(); // No salves
        Job<?> job = w.newJob(this.task.getId(), this.task.getTaskDescription(), this.selectedImpl, slaveNames, listener);
        job.setTransferGroupId(transferGroupId);
        job.setHistory(Job.JobHistory.NEW);

        return job;
    }

    /**
     * Code executed when the job execution has failed
     * 
     * @param job
     * @param endStatus
     */
    public final void failedJob(Job<?> job, JobEndStatus endStatus) {
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
            notifyError();
        }
    }

    /**
     * Code executed when the job execution has been completed
     * 
     * @param job
     */
    public final void completedJob(Job<?> job) {
        // End profile
        profile.end();

        // Notify end
        int jobId = job.getJobId();
        JOB_LOGGER.info("Received a notification for job " + jobId + " with state OK");

        // Job finished, update info about the generated/updated data
        doOutputTransfers(job);

        // Notify completion
        notifyCompleted();
    }

    private final void doOutputTransfers(Job<?> job) {
        // Job finished, update info about the generated/updated data
        Worker<T, I> w = selectedResource.getResource();

        for (Parameter p : job.getTaskParams().getParameters()) {
            if (p instanceof DependencyParameter) {
                // OUT or INOUT: we must tell the FTM about the
                // generated/updated datum
                DataInstanceId dId = null;
                DependencyParameter dp = (DependencyParameter) p;
                switch (p.getDirection()) {
                    case IN:
                        // FTM already knows about this datum
                        continue;
                    case OUT:
                        dId = ((DataAccessId.WAccessId) dp.getDataAccessId()).getWrittenDataInstance();
                        break;
                    case INOUT:
                        dId = ((DataAccessId.RWAccessId) dp.getDataAccessId()).getWrittenDataInstance();
                        if (job.getType() == TaskType.SERVICE) {
                            continue;
                        }
                        break;
                }

                String name = dId.getRenaming();
                if (job.getType() == TaskType.METHOD) {
                    String targetProtocol = null;
                    switch (dp.getType()) {
                        case FILE_T:
                            targetProtocol = DataLocation.Protocol.FILE_URI.getSchema();
                            break;
                        case OBJECT_T:
                            targetProtocol = DataLocation.Protocol.OBJECT_URI.getSchema();
                            break;
                        case PSCO_T:
                            targetProtocol = DataLocation.Protocol.PERSISTENT_URI.getSchema();
                            break;
                        case EXTERNAL_PSCO_T:
                            // External PSCOs are treated as objects within the runtime
                            // Its value is the PSCO Id
                            targetProtocol = DataLocation.Protocol.OBJECT_URI.getSchema();
                            break;
                        default:
                            // Should never reach this point because only
                            // DependencyParameter types are treated
                            // Ask for any_uri just in case
                            targetProtocol = DataLocation.Protocol.ANY_URI.getSchema();
                            break;
                    }

                    DataLocation outLoc = null;
                    try {
                        SimpleURI targetURI = new SimpleURI(targetProtocol + dp.getDataTarget());
                        outLoc = DataLocation.createLocation(w, targetURI);
                    } catch (Exception e) {
                        ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + dp.getDataTarget(), e);
                    }
                    Comm.registerLocation(name, outLoc);
                } else {
                    // Service
                    Object value = job.getReturnValue();
                    Comm.registerValue(name, value);
                }
            }
        }
    }

    /*
     * ***************************************************************************************************************
     * EXECUTION TRIGGERS
     * ***************************************************************************************************************
     */
    @Override
    protected void doCompleted() {
        // Profile the resource
        selectedResource.profiledExecution(selectedImpl, profile);

        // Decrease the execution counter and set the task as finished and notify the producer
        task.decreaseExecutionCount();
        task.setStatus(TaskState.FINISHED);
        producer.notifyTaskEnd(task);
    }

    @Override
    protected void doError() throws FailedActionException {
        if (this.executingResources.size() >= SCHEDULING_CHANCES) {
            LOGGER.warn("Task " + task.getId() + " has already been rescheduled; notifying task failure.");
            ErrorManager.warn("Task " + task.getId() + " has already been rescheduled; notifying task failure.");
            throw new FailedActionException();
        } else {
            ErrorManager.warn("Task " + task.getId() + " execution on worker " + selectedResource.getName()
                    + " has failed; rescheduling task execution. (changing worker)");
            LOGGER.warn("Task " + task.getId() + " execution on worker " + selectedResource.getName()
                    + " has failed; rescheduling task execution. (changing worker)");
        }
    }

    @Override
    protected void doFailed() {
        // Failed message
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
        sb.append(" \n");

        ErrorManager.warn(sb.toString());

        // Notify task failure
        task.decreaseExecutionCount();
        task.setStatus(TaskState.FAILED);
        producer.notifyTaskEnd(task);
    }

    /*
     * ***************************************************************************************************************
     * SCHEDULING MANAGEMENT
     * ***************************************************************************************************************
     */
    @Override
    public final LinkedList<ResourceScheduler<P, T, I>> getCompatibleWorkers() {
        return getCoreElementExecutors(task.getTaskDescription().getId());
    }

    @Override
    public final LinkedList<I> getCompatibleImplementations(ResourceScheduler<P, T, I> r) {
        return r.getExecutableImpls(task.getTaskDescription().getId());
    }

    @SuppressWarnings("unchecked")
    @Override
    public final I[] getImplementations() {
        return (I[]) CoreManager.getCoreImplementations(task.getTaskDescription().getId());
    }

    @Override
    public final boolean isCompatible(Worker<T, I> r) {
        return r.canRun(task.getTaskDescription().getId());
    }

    @Override
    public final Integer getCoreId() {
        return task.getTaskDescription().getId();
    }

    @Override
    public final int getPriority() {
        return task.getTaskDescription().hasPriority() ? 1 : 0;
    }

    @Override
    public final Score schedulingScore(ResourceScheduler<P, T, I> targetWorker, Score actionScore) {
        Score computedScore = targetWorker.generateResourceScore(this, task.getTaskDescription(), actionScore);
        // LOGGER.debug("Scheduling Score " + computedScore);
        return computedScore;
    }

    @Override
    public final void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        //if (DEBUG) {
        //    LOGGER.debug("Scheduling " + this + ". Computing best worker and best implementation");
        //}
        StringBuilder debugString = new StringBuilder("Scheduling " + this + " execution:\n");

        // COMPUTE RESOURCE CANDIDATES
        LinkedList<ResourceScheduler<P, T, I>> candidates = new LinkedList<>();
        if (this.forcedResource != null) {
            // The scheduling is forced to a given resource
            candidates.add(this.forcedResource);
        } else if (isSchedulingConstrained()) {
            // The scheduling is constrained by dependencies
            for (AllocatableAction<P, T, I> a : this.getConstrainingPredecessors()) {
                candidates.add(a.getAssignedResource());
            }
        } else {
            // Free scheduling
            candidates = getCompatibleWorkers();
        }

        // COMPUTE BEST WORKER AND IMPLEMENTATION
        ResourceScheduler<P, T, I> bestWorker = null;
        I bestImpl = null;
        Score bestScore = null;
        int usefulResources = 0;
        for (ResourceScheduler<P, T, I> worker : candidates) {
            if (executingResources.contains(worker)) {
                if (DEBUG) {
                    LOGGER.debug("Task already running on worker " + worker.getName());
                }
                continue;
            }
            Score resourceScore = worker.generateResourceScore(this, task.getTaskDescription(), actionScore);
            ++usefulResources;
            for (I impl : getCompatibleImplementations(worker)) {
                Score implScore = worker.generateImplementationScore(this, task.getTaskDescription(), impl, resourceScore);
                if (DEBUG) {
                    debugString.append(" Resource ").append(worker.getName()).append(" ").append(" Implementation ")
                            .append(impl.getImplementationId()).append(" ").append(" Score ").append(implScore).append("\n");
                }
                if (Score.isBetter(implScore, bestScore)) {
                    bestWorker = worker;
                    bestImpl = impl;
                    bestScore = implScore;
                }
            }
        }

        // CHECK SCHEDULING RESULT
        //if (DEBUG) {
        //    LOGGER.debug(debugString.toString());
        //}

        if (bestWorker == null && usefulResources == 0) {
            LOGGER.warn("No worker can run " + this);
            throw new BlockedActionException();
        }

        schedule(bestWorker, bestImpl);
    }

    @Override
    public final void schedule(ResourceScheduler<P, T, I> targetWorker, Score actionScore)
            throws BlockedActionException, UnassignedActionException {

        //if (DEBUG) {
        //    LOGGER.debug("Scheduling " + this + " on worker " + targetWorker.getName() + ". Computing best implementation");
        //}

        if (targetWorker == null
                // Resource is not compatible with the Core
                || !targetWorker.getResource().canRun(task.getTaskDescription().getId())
                // already ran on the resource
                || executingResources.contains(targetWorker)) {

            String message = "Worker " + (targetWorker == null ? "null" : targetWorker.getName()) + " has not available resources to run "
                    + this;
            LOGGER.warn(message);
            throw new UnassignedActionException();
        }

        StringBuilder debugString = new StringBuilder("Scheduling " + this + " execution for worker " + targetWorker + ":\n");
        if (DEBUG) {
            debugString.append("\t Resource ").append(targetWorker.getName()).append("\n");
        }

        I bestImpl = null;
        Score bestScore = null;
        Score resourceScore = targetWorker.generateResourceScore(this, task.getTaskDescription(), actionScore);
        for (I impl : getCompatibleImplementations(targetWorker)) {
            Score implScore = targetWorker.generateImplementationScore(this, task.getTaskDescription(), impl, resourceScore);
            if (DEBUG) {
                debugString.append("\t\t Implementation ").append(impl.getImplementationId()).append(implScore).append("\n");
            }
            if (Score.isBetter(implScore, bestScore)) {
                bestImpl = impl;
                bestScore = implScore;
            }
        }

        // CHECK SCHEDULING RESULT
        if (DEBUG) {
            LOGGER.debug(debugString.toString());
        }

        schedule(targetWorker, bestImpl);
    }

    @Override
    public final void schedule(ResourceScheduler<P, T, I> targetWorker, I impl) throws BlockedActionException, UnassignedActionException {
        if (targetWorker == null || impl == null) {
            /*if (targetWorker == null && impl == null) {
                LOGGER.debug("Not available resources to run " + this + " because both the target worker and the best implementation are null");
            } else if (targetWorker == null) {
                LOGGER.debug("Not available resources to run " + this + " because the target worker is null");
            } else if (impl == null) {
                LOGGER.debug("Not available resources to run " + this + " because the best implementation is null");
            }*/
            throw new UnassignedActionException();
        }

        //if (DEBUG) {
        //    LOGGER.debug(
        //            "Scheduling " + this + " on worker " + targetWorker.getName() + " with implementation " + impl.getImplementationId());
        //}

        if (// Resource is not compatible with the implementation
        !targetWorker.getResource().canRun(impl)
                // already ran on the resource
                || executingResources.contains(targetWorker)) {

            LOGGER.debug("Worker " + targetWorker.getName() + " has not available resources to run " + this);
            throw new UnassignedActionException();
        }

        LOGGER.info(
                "Assigning action " + this + " to worker " + targetWorker.getName() + " with implementation " + impl.getImplementationId());

        this.assignImplementation(impl);
        this.assignResources(targetWorker);
        targetWorker.scheduleAction(this);
    }

    /*
     * ***************************************************************************************************************
     * OTHER
     * ***************************************************************************************************************
     */
    @Override
    public String toString() {
        return "ExecutionAction ( Task " + task.getId() + ", CE name " + task.getTaskDescription().getName() + ")";
    }

}
