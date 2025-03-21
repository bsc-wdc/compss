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

import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.log.LoggerManager;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.ActionGroup.MutexGroup;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.CommutativeGroupTask;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.TaskGroup;
import es.bsc.compss.types.TaskState;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.impl.DependencyParameter;
import es.bsc.compss.types.parameter.impl.Parameter;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.worker.COMPSsException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


public class ExecutionAction extends AllocatableAction implements JobListener<Parameter> {

    // Fault tolerance parameters
    private static final int SCHEDULING_CHANCES = 2;

    // Execution Info
    protected final AccessProcessor ap;
    protected final Task task;
    private final LinkedList<Integer> jobs;
    protected Job<?> currentJob;
    boolean cancelledBeforeSubmit = false;
    boolean extraResubmit = false;


    /**
     * Creates a new execution action.
     *
     * @param schedulingInformation Associated scheduling information.
     * @param orchestrator Task orchestrator.
     * @param ap Access processor.
     * @param task Associated task.
     */
    public ExecutionAction(SchedulingInformation schedulingInformation, ActionOrchestrator orchestrator,
        AccessProcessor ap, Task task) {

        super(schedulingInformation, orchestrator);
        this.ap = ap;
        this.task = task;
        this.jobs = new LinkedList<>();

        // Add execution to task
        this.task.addExecution(this);

        synchronized (this.task) {
            // Register data dependencies
            registerDataDependencies();
            // Register stream producers
            registerStreamProducers();
            // Register mutex condition
            registerMutex();
        }

        // Scheduling constraints
        // Restricted resource
        Task resourceConstraintTask = this.task.getEnforcingTask();
        if (resourceConstraintTask != null) {
            for (AllocatableAction e : resourceConstraintTask.getExecutions()) {
                addResourceConstraint(e);
            }
        }
    }

    private void registerMutex() {
        for (CommutativeGroupTask group : this.task.getCommutativeGroupList()) {
            MutexGroup mGroup = group.getActions();
            this.addToMutexGroup(mGroup);
        }
    }

    private void registerStreamProducers() {
        for (AbstractTask predecessor : this.task.getStreamProducers()) {
            for (AllocatableAction e : ((Task) predecessor).getExecutions()) {
                if (e != null && e.isPending()) {
                    addStreamProducer(e);
                }
            }
        }
    }

    private void registerDataDependencies() {
        List<AbstractTask> predecessors = this.task.getPredecessors();
        for (AbstractTask predecessor : predecessors) {
            if (!(predecessor instanceof CommutativeGroupTask)) {
                treatStandardPredecessor(predecessor);
            } else {
                treatCommutativePredecessor((CommutativeGroupTask) predecessor);
            }
        }
    }

    private void treatCommutativePredecessor(CommutativeGroupTask predecessor) {
        if (DEBUG) {
            LOGGER.debug("Task has a commutative group as a predecessor");
        }
        for (Task t : predecessor.getCommutativeTasks()) {
            for (AllocatableAction com : t.getExecutions()) {
                if (!com.getDataPredecessors().contains(this)) {
                    this.addDataPredecessor(com);
                }
            }
        }
    }

    private void treatStandardPredecessor(AbstractTask predecessor) {
        for (AllocatableAction e : predecessor.getExecutions()) {
            if (e != null && e.isPending()) {
                addDataPredecessor(e);
            } else {
                addAlreadyDoneAction(e);
            }
        }
    }

    /**
     * Returns the associated task.
     *
     * @return The associated task.
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
    public boolean isToReserveResources() {
        return true;
    }

    @Override
    public boolean isToReleaseResources() {
        return true;
    }

    @Override
    public boolean isToStopResource() {
        return false;
    }

    @Override
    protected void doAction() {
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onSubmission();
        if (!this.cancelledBeforeSubmit) {
            this.currentJob = createJob();
            this.jobs.add(this.currentJob.getJobId());
            this.currentJob.stageIn();
            this.task.setSubmitted();
        } else {
            jobCancelled(null);
        }
    }

    private Job<?> createJob() {
        // Create job
        if (DEBUG) {
            LOGGER.debug(this.toString() + " starts job creation");
        }
        Worker<? extends WorkerResourceDescription> w = getAssignedResource().getResource();
        List<String> slaveNames = getSlaveNames();

        // Get predecessors for task dependency tracing
        List<Integer> predecessors = null;
        if (Tracer.isActivated() && Tracer.isTracingTaskDependencies()) {
            predecessors = Tracer.getPredecessors(this.task.getId());
        }
        Job<?> job = w.newJob(this.task.getId(), this.task.getTaskDescription(), this.getAssignedImplementation(),
            slaveNames, this, predecessors, this.task.getSuccessors().size());

        LOGGER.info((getExecutingResources().size() > 1 ? "Rescheduled" : "New") + " Job " + job.getJobId() + " (Task: "
            + this.task.getId() + ")");
        LOGGER.info("  * Method name: " + this.task.getTaskDescription().getName());
        LOGGER.info("  * Target host: " + this.getAssignedResource().getName());
        // Remove predecessors from map for task dependency tracing
        if (Tracer.isActivated() && Tracer.isTracingTaskDependencies()) {
            Tracer.removePredecessor(this.task.getId());
        }
        return job;
    }

    @Override
    public boolean checkIfCanceled(AllocatableAction aa) {
        return (aa instanceof ExecutionAction) && (((ExecutionAction) aa).getTask().getStatus() == TaskState.CANCELED);
    }

    /*
     * ***************************************************************************************************************
     * EXECUTED SUPPORTING THREAD ON JOB_TRANSFERS_LISTENER
     * ***************************************************************************************************************
     */

    @Override
    public final void stageInFailed(int failedtransfers) {
        int taskId = this.task.getId();
        String workerName = getAssignedResource().getName();
        ErrorManager.warn("Transfers for running task " + taskId + " on worker " + workerName + " have failed.");
        this.notifyError();
    }

    @Override
    public final void stageInCompleted() {
        this.currentJob.submit();
    }

    @Override
    public void submitted(Job<?> job) {
        submittedAt(job, System.currentTimeMillis());
    }

    @Override
    public void submittedAt(Job<?> job, long ts) {
        this.profile.setSubmissionTime(ts);
    }

    @Override
    public void arrived(Job<?> job) {
        arrivedAt(job, System.currentTimeMillis());
    }

    @Override
    public void arrivedAt(Job<?> job, long ts) {
        this.profile.setArrivalTime(ts);
    }

    protected List<String> getSlaveNames() {
        return new ArrayList<>(); // No slaves
    }

    @Override
    public void allInputDataOnWorker(Job<?> job) {
        allInputDataOnWorkerAt(job, System.currentTimeMillis());
    }

    @Override
    public void allInputDataOnWorkerAt(Job<?> job, long ts) {
        profile.setDataFetchingTime(ts);
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onDataReception();

    }

    @Override
    public void startingExecution(Job<?> job) {
        startingExecutionAt(job, System.currentTimeMillis());
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onExecutionStart();
    }

    @Override
    public void startingExecutionAt(Job<?> job, long ts) {
        profile.setExecutionStartTime(ts);
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onExecutionStartAt(ts);
    }

    @Override
    public void endedExecution(Job<?> job) {
        endedExecutionAt(job, System.currentTimeMillis());
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onExecutionEnd();
    }

    @Override
    public void endedExecutionAt(Job<?> job, long ts) {
        profile.setExecutionEndTime(ts);
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onExecutionEndAt(ts);
    }

    @Override
    public void endNotified(Job<?> job) {
        endNotifiedAt(job, System.currentTimeMillis());
    }

    @Override
    public void endNotifiedAt(Job<?> job, long ts) {
        profile.setEndNotificationTime(ts);
    }

    /**
     * Code executed to cancel a running execution.
     *
     * @throws Exception Unstarted node exception.
     */
    @Override
    protected void stopAction() throws Exception {
        // Submit stop petition
        if (DEBUG) {
            LOGGER.debug("Task " + this.task.getId() + " starts cancelling running job");
        }
        if (this.currentJob != null) {
            this.currentJob.cancel();
        } else {
            this.cancelledBeforeSubmit = true;
        }
    }

    @Override
    public final void jobCancelled(Job<?> job) {
        notifyError();
    }

    /**
     * Code executed when an exception has occurred on the job.
     *
     * @param job Job of exception.
     * @param e COMPSsException raised by the job
     */
    @Override
    public final void jobException(Job<?> job, COMPSsException e) {
        LOGGER.debug("Job " + job.getJobId() + " raised an Exception");
        this.profile.end(System.currentTimeMillis());

        if (e instanceof COMPSsException && this.task.hasTaskGroups()) {
            for (TaskGroup t : this.task.getTaskGroupList()) {
                t.setException((COMPSsException) e);
            }
        }

        notifyException(e);

    }

    /**
     * Code executed when the job execution has failed.
     *
     * @param job Failed job.
     * @param status Failure status
     */
    @Override
    public final void jobFailed(Job<?> job, JobEndStatus status) {
        LOGGER.debug("Job " + job.getJobId() + " failed");
        this.profile.end(System.currentTimeMillis());

        notifyError();
    }

    /**
     * Code executed when the job execution has been completed.
     *
     * @param job Completed job.
     */
    @Override
    public final void jobCompleted(Job<?> job) {
        LOGGER.debug("Job " + job.getJobId() + " completed");
        // End profile
        this.profile.end(System.currentTimeMillis());

        // Notify completion
        notifyCompleted();
    }

    @Override
    public void resultAvailable(Parameter p, String dataName) {
        if (p.isPotentialDependency()) {
            DependencyParameter dp = (DependencyParameter) p;
            if (dp.getDirection() == Direction.COMMUTATIVE) {
                EngineDataAccessId placeHolder = dp.getDataAccessId();
                CommutativeGroupTask cgt = this.getTask().getCommutativeGroup(placeHolder.getDataId());
                EngineDataAccessId performedAccess = cgt.nextAccess();
                dp.setDataAccessId(performedAccess);
            }

            p.getMonitor().onCreation(p.getType(), dataName);
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
        this.getAssignedResource().profiledExecution(this.getAssignedImplementation(), profile);

        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onSuccesfulExecution();

        // Decrease the execution counter and set the task as finished and notify the producer
        this.task.decreaseExecutionCount();
        this.task.setStatus(TaskState.FINISHED);
        this.ap.notifyTaskEnd(this.task);
    }

    @Override
    protected void doError() throws FailedActionException {
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onErrorExecution();

        if (this.task.getOnFailure() == OnFailure.RETRY) {
            if (this.getExecutingResources().size() >= SCHEDULING_CHANCES) {
                LOGGER.warn("Task " + this.task.getId() + " has already been rescheduled; notifying task failure.");
                ErrorManager
                    .warn("Task " + this.task.getId() + " has already been rescheduled; notifying task failure.");
                throw new FailedActionException();
            } else {
                ErrorManager
                    .warn("Task " + this.task.getId() + " execution on worker " + this.getAssignedResource().getName()
                        + " has failed; rescheduling task execution. (changing worker)");
                LOGGER.warn("Task " + this.task.getId() + " execution on worker " + this.getAssignedResource().getName()
                    + " has failed; rescheduling task execution. (changing worker)");
            }
        } else {
            LOGGER.warn("Notifying task " + this.task.getId() + " failure");
            ErrorManager.warn("Notifying task " + this.task.getId() + " failure");
            throw new FailedActionException();
        }
    }

    @Override
    protected void doAbort() {
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onAbortedExecution();
    }

    @Override
    protected void doFailed() {
        // Failed log message
        String taskName = this.task.getTaskDescription().getName();
        StringBuilder sb = new StringBuilder();
        sb.append("Task '").append(taskName).append("' TOTALLY FAILED.\n");
        sb.append("Possible causes:\n");
        sb.append("     -Exception thrown by task '").append(taskName).append("'.\n");
        sb.append("     -Expected output files not generated by task '").append(taskName).append("'.\n");
        sb.append("     -Could not provide nor retrieve needed data between master and worker.\n");
        sb.append("\n");
        sb.append("Check files '").append(LoggerManager.getJobsLogDir()).append("job[");
        Iterator<Integer> j = this.jobs.iterator();
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
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onFailedExecution();

        // Notify task failure
        this.task.decreaseExecutionCount();
        this.task.setStatus(TaskState.FAILED);
        this.ap.notifyTaskEnd(this.task);
    }

    @Override
    protected Collection<AllocatableAction> doException(COMPSsException e) {
        LinkedList<TaskGroup> taskGroups = this.task.getTaskGroupList();
        Collection<AllocatableAction> otherActionsFromGroups = new LinkedList<>();
        for (TaskGroup group : taskGroups) {
            if (!group.getName().equals("App" + this.task.getApplication().getId())) {
                group.setException((COMPSsException) e);
                group.addToCollectionExecutionForTasksOtherThan(otherActionsFromGroups, this.getTask());
            }
        }

        // Failed log message
        String taskName = this.task.getTaskDescription().getName();
        StringBuilder sb = new StringBuilder();
        sb.append("COMPSs Exception raised : Task " + this.task.getId() + " (").append(taskName)
            .append(") has raised an exception with message ").append(e.getMessage())
            .append(". Members of the containing groups will be cancelled.\n");
        sb.append("\n");
        ErrorManager.warn(sb.toString());

        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onException(e);

        // Decrease the execution counter and set the task as finished and notify the producer
        this.task.decreaseExecutionCount();
        this.task.setStatus(TaskState.FINISHED);
        this.ap.notifyTaskEnd(this.task);
        return otherActionsFromGroups;
    }

    @Override
    protected boolean doCanceled() {
        // Cancelled log message
        String taskName = this.task.getTaskDescription().getName();
        ErrorManager.warn("Task " + this.task.getId() + "(Action: " + this.getId() + ") with name " + taskName
            + " has been cancelled.");

        // Notify task cancellation
        this.task.decreaseExecutionCount();
        this.task.setStatus(TaskState.CANCELED);
        this.ap.notifyTaskEnd(this.task);
        if (this.task.getOnFailure().equals(OnFailure.IGNORE)) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    protected void doFailIgnored() {
        // Failed log message
        String taskName = this.task.getTaskDescription().getName();
        StringBuilder sb = new StringBuilder();
        sb.append("Task failure: Task " + this.task.getId() + " (").append(taskName)
            .append(") has failed. Successors keep running.\n");
        sb.append("\n");
        ErrorManager.warn(sb.toString());

        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onFailedExecution();

        // Notify task completion despite the failure
        this.task.decreaseExecutionCount();
        this.task.setStatus(TaskState.FINISHED);
        this.ap.notifyTaskEnd(this.task);
    }

    /*
     * ***************************************************************************************************************
     * SCHEDULING MANAGEMENT
     * ***************************************************************************************************************
     */
    @Override
    public final List<ResourceScheduler<? extends WorkerResourceDescription>> getCompatibleWorkers() {
        return getCoreElementExecutors(this.task.getTaskDescription().getCoreElement().getCoreId());
    }

    @Override
    public final Implementation[] getImplementations() {
        CoreElement ce = this.task.getTaskDescription().getCoreElement();
        List<Implementation> coreImpls = ce.getImplementations();

        int coreImplsSize = coreImpls.size();
        Implementation[] impls = new Implementation[coreImplsSize];
        for (int i = 0; i < coreImplsSize; ++i) {
            impls[i] = coreImpls.get(i);
        }
        return impls;
    }

    @Override
    public <W extends WorkerResourceDescription> boolean isCompatible(Worker<W> r) {
        return r.canRun(this.task.getTaskDescription().getCoreElement().getCoreId());
    }

    @Override
    public final <T extends WorkerResourceDescription> List<Implementation>
        getCompatibleImplementations(ResourceScheduler<T> r) {
        return r.getExecutableImpls(this.task.getTaskDescription().getCoreElement().getCoreId());
    }

    @Override
    public final Integer getCoreId() {
        return this.task.getTaskDescription().getCoreElement().getCoreId();
    }

    @Override
    public final int getPriority() {
        return this.task.getTaskDescription().hasPriority() ? 1 : 0;
    }

    @Override
    public long getGroupPriority() {
        return ACTION_SINGLE;
    }

    @Override
    public OnFailure getOnFailure() {
        return this.task.getOnFailure();
    }

    @Override
    public final <T extends WorkerResourceDescription> Score schedulingScore(ResourceScheduler<T> targetWorker,
        Score actionScore) {
        return targetWorker.generateResourceScore(this, this.task.getTaskDescription(), actionScore);
    }

    @SuppressWarnings("unchecked")
    @Override
    public final void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        // COMPUTE RESOURCE CANDIDATES
        List<ResourceScheduler<? extends WorkerResourceDescription>> candidates = new LinkedList<>();
        List<ResourceScheduler<? extends WorkerResourceDescription>> compatibleWorkers = this.getCompatibleWorkers();
        if (this.isTargetResourceEnforced()) {
            // The scheduling is forced to a given resource
            ResourceScheduler<? extends WorkerResourceDescription> target = this.getEnforcedTargetResource();
            if (compatibleWorkers.contains(target)) {
                candidates.add(target);
            } else {
                throw new UnassignedActionException();
            }
        } else if (isSchedulingConstrained()) {
            // The scheduling is constrained by dependencies
            for (AllocatableAction a : this.getConstrainingPredecessors()) {
                ResourceScheduler<? extends WorkerResourceDescription> target = a.getAssignedResource();
                if (compatibleWorkers.contains(target)) {
                    candidates.add(target);
                }
            }
        } else {
            // Free scheduling
            candidates = compatibleWorkers;
        }

        if (candidates.isEmpty()) {
            throw new BlockedActionException();
        }

        List prevExecutors = this.getExecutingResources();
        if (candidates.size() > prevExecutors.size() && prevExecutors.size() > 0) {
            candidates.removeAll(prevExecutors);
        }
        this.scheduleSecuredCandidates(actionScore, candidates);
    }

    @Override
    public void schedule(Collection<ResourceScheduler<? extends WorkerResourceDescription>> candidates,
        Score actionScore) throws UnassignedActionException {
        List<ResourceScheduler<? extends WorkerResourceDescription>> compatibleWorkers = this.getCompatibleWorkers();
        List<ResourceScheduler<? extends WorkerResourceDescription>> verifiedCandidates = new LinkedList<>();

        if (this.isTargetResourceEnforced()) {
            // The scheduling is forced to a given resource
            ResourceScheduler<? extends WorkerResourceDescription> target = this.getEnforcedTargetResource();
            if (candidates.contains(target) && compatibleWorkers.contains(target)) {
                verifiedCandidates.add(target);
            } else {
                throw new UnassignedActionException();
            }
        } else if (this.isSchedulingConstrained()) {
            // The scheduling is constrained by dependencies
            for (AllocatableAction a : this.getConstrainingPredecessors()) {
                ResourceScheduler<? extends WorkerResourceDescription> target = a.getAssignedResource();
                if (candidates.contains(target) && compatibleWorkers.contains(target)) {
                    verifiedCandidates.add(target);
                }
            }
            if (verifiedCandidates.isEmpty()) {
                throw new UnassignedActionException();
            }

        } else {
            for (ResourceScheduler<? extends WorkerResourceDescription> candidate : candidates) {
                if (compatibleWorkers.contains(candidate) && compatibleWorkers.contains(candidate)) {
                    verifiedCandidates.add(candidate);
                }
            }
            if (verifiedCandidates.isEmpty()) {
                throw new UnassignedActionException();
            }
        }
        scheduleSecuredCandidates(actionScore, verifiedCandidates);
    }

    @Override
    public final void schedule(ResourceScheduler<? extends WorkerResourceDescription> targetWorker, Score actionScore)
        throws UnassignedActionException {
        if (targetWorker == null || !validateWorker(targetWorker)) {
            throw new UnassignedActionException();
        }

        Implementation bestImpl = null;
        Score bestScore = null;
        Score resourceScore = targetWorker.generateResourceScore(this, this.task.getTaskDescription(), actionScore);
        if (resourceScore != null) {
            for (Implementation impl : getCompatibleImplementations(targetWorker)) {
                Score implScore =
                    targetWorker.generateImplementationScore(this, this.task.getTaskDescription(), impl, resourceScore);
                if (Score.isBetter(implScore, bestScore)) {
                    bestImpl = impl;
                    bestScore = implScore;
                }
            }
        }
        if (bestImpl == null) {
            throw new UnassignedActionException();
        }
        assignWorkerAndImpl(targetWorker, bestImpl);
    }

    @Override
    public final void schedule(ResourceScheduler<? extends WorkerResourceDescription> targetWorker, Implementation impl)
        throws UnassignedActionException {

        if (targetWorker == null || impl == null) {
            this.assignResource(null); // to remove previous allocation when re-scheduling the action
            throw new UnassignedActionException();
        }

        if (DEBUG) {
            LOGGER.debug("Scheduling " + this + " on worker " + targetWorker.getName() + " with implementation "
                + impl.getImplementationId());
        }

        if (!validateWorker(targetWorker)) {
            throw new UnassignedActionException();
        }

        if (!targetWorker.getResource().canRun(impl)) {
            LOGGER.warn("Worker " + targetWorker.getName() + " is not compatible with " + impl);
            throw new UnassignedActionException();
        }

        assignWorkerAndImpl(targetWorker, impl);
    }

    /**
     * Verifies that the passed-in worker is able to run the task given the task execution history.
     * 
     * @param targetCandidate Resource whose aptitude to run the task has to be evaluated
     * @return {@literal true}, if the worker passed in is compatible with the action; {@literal false}, otherwise.
     */
    private boolean validateWorker(ResourceScheduler targetCandidate) {
        if (this.isTargetResourceEnforced()) {
            // The scheduling is forced to a given resource
            ResourceScheduler<? extends WorkerResourceDescription> enforcedTarget = this.getEnforcedTargetResource();
            if (enforcedTarget != targetCandidate) {
                LOGGER.warn("Task " + this.getTask().getId() + " is enforced to run on " + enforcedTarget.getName());
                return false;
            }
        } else if (this.isSchedulingConstrained()) {
            boolean isPredecessor = false;
            // The scheduling is constrained by dependencies
            for (AllocatableAction a : this.getConstrainingPredecessors()) {
                ResourceScheduler<? extends WorkerResourceDescription> predecessorHost = a.getAssignedResource();
                if (targetCandidate == predecessorHost) {
                    isPredecessor = true;
                }
            }
            if (!isPredecessor) {
                LOGGER.warn(targetCandidate.getName() + " did not host the execution of any constraining predecessor");
                return false;
            }
        }

        if (this.getExecutingResources().contains(targetCandidate) && this.getCompatibleWorkers().size() > 1) {
            LOGGER.warn("Task " + this.getTask().getId() + " was already scheduled on " + targetCandidate.getName());
            return false;
        }
        return true;
    }

    /**
     * Selects the best resource-Implementation pair given for an action given a score and a list of workers that could
     * potentially host the execution of the action. All the workers within the list have been already checked to be
     * able to host the action execution.
     * 
     * @param actionScore Score given by the scheduler to the action
     * @param candidates list of compatible workers that could host the action execution
     * @throws UnassignedActionException the action could not be assigned to any of the given candidate resources
     */
    private void scheduleSecuredCandidates(Score actionScore,
        List<ResourceScheduler<? extends WorkerResourceDescription>> candidates) throws UnassignedActionException {
        // COMPUTE BEST WORKER AND IMPLEMENTATION
        StringBuilder debugString = new StringBuilder("Scheduling " + this + " execution:\n");
        ResourceScheduler<? extends WorkerResourceDescription> bestWorker = null;
        Implementation bestImpl = null;
        Score bestScore = null;
        for (ResourceScheduler<? extends WorkerResourceDescription> worker : candidates) {
            Score resourceScore = worker.generateResourceScore(this, this.task.getTaskDescription(), actionScore);
            if (resourceScore != null) {
                for (Implementation impl : getCompatibleImplementations(worker)) {
                    Score implScore =
                        worker.generateImplementationScore(this, this.task.getTaskDescription(), impl, resourceScore);
                    if (DEBUG) {
                        debugString.append("[Task ").append(this.task.getId()).append("] Resource ")
                            .append(worker.getName()).append(" ").append(" Implementation ")
                            .append(impl.getImplementationId()).append(" ").append(" Score ").append(implScore)
                            .append("\n");
                    }
                    if (Score.isBetter(implScore, bestScore)) {
                        bestWorker = worker;
                        bestImpl = impl;
                        bestScore = implScore;
                    }
                }
            }
        }

        // CHECK SCHEDULING RESULT
        if (DEBUG) {
            LOGGER.debug(debugString.toString());
        }

        if (bestWorker == null) {
            throw new UnassignedActionException();
        }
        assignWorkerAndImpl(bestWorker, bestImpl);
    }

    /**
     * Assigns an implementation and a worker to the action and submits the action scheduler to the corresponding
     * ResourceScheduler. The method assumes that the worker has already been checked to be a proper candidate to host
     * the action and the selected implementation is a compatible pick given the action, its requirements and the
     * worker's capabilities.
     *
     * @param worker worker where to submit the action
     * @param impl implementation to request the execution
     */
    private void assignWorkerAndImpl(ResourceScheduler worker, Implementation impl) {
        LOGGER.info("Assigning action " + this + " to worker " + worker.getName() + " with implementation "
            + impl.getImplementationId());

        this.assignImplementation(impl);
        assignResource(worker);
        worker.scheduleAction(this);

        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onSchedule();
    }

    /*
     * ***************************************************************************************************************
     * OTHER
     * ***************************************************************************************************************
     */
    @Override
    public String toString() {
        String ceName = this.task.getTaskDescription().getName();
        return "ExecutionAction (Task " + this.task.getId() + ", CE name " + ceName + ")";
    }

}
