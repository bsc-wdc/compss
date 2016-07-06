package integratedtoolkit.types.allocatableactions;

import integratedtoolkit.ITConstants;
import integratedtoolkit.api.COMPSsRuntime.DataDirection;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.components.impl.TaskDispatcher.TaskProducer;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.SchedulingInformation;
import integratedtoolkit.util.ResourceScheduler;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.data.DataAccessId;
import integratedtoolkit.types.data.DataInstanceId;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.operation.JobTransfersListener;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.JobStatusListener;
import integratedtoolkit.types.parameter.DependencyParameter;
import integratedtoolkit.types.parameter.PSCOId;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.parameter.SCOParameter;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.JobDispatcher;

import java.util.Iterator;
import java.util.LinkedList;

import org.apache.log4j.Logger;

public class SingleExecution<P extends Profile, T extends WorkerResourceDescription> extends AllocatableAction<P, T> {

    private static final int TRANSFER_CHANCES = 2;
    private static final int SUBMISSION_CHANCES = 2;
    private static final int SCHEDULING_CHANCES = 2;

    public static String executionType = System.getProperty(ITConstants.IT_TASK_EXECUTION);

    //LOGGER
    private static final Logger jobLogger = Logger.getLogger(Loggers.JM_COMP);

    //Execution Info
    private final TaskProducer producer;
    private final Task task;
    private int transferErrors = 0;
    private int executionErrors = 0;
    private LinkedList<Integer> jobs = new LinkedList<Integer>();

    private WorkerResourceDescription resourceConsumption;

    public SingleExecution(SchedulingInformation<P, T> schedulingInformation, TaskProducer producer, Task task) {
        super(schedulingInformation);
        this.producer = producer;
        this.task = task;
        task.setExecution(this);
        //Register data dependencies events
        for (Task predecessor : task.getPredecessors()) {
            SingleExecution<P, T> e = (SingleExecution<P, T>) predecessor.getExecution();
            if (e != null && e.isPending()) {
                this.addDataPredecessor(e);
            }
        }
        //Scheduling constraints
        //Restricted resource
        Task resourceConstraintTask = task.getEnforcingTask();
        if (resourceConstraintTask != null) {
            SingleExecution<P, T> e = (SingleExecution<P, T>) resourceConstraintTask.getExecution();
            this.setResourceConstraint(e);
        }

    }

    public Task getTask() {
        return this.task;
    }

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
        Worker w = selectedResource.getResource();
        w.endTask(resourceConsumption);
    }

    @Override
    protected void doAction() {
        jobLogger.info("Ordering transfers to " + selectedResource + " to run task: " + task.getId());
        transferErrors = 0;
        executionErrors = 0;
        transferInputData();
    }

    private void transferInputData() {
        TaskParams taskParams = task.getTaskParams();
        JobTransfersListener listener = new JobTransfersListener(this);
        for (Parameter p : taskParams.getParameters()) {
            jobLogger.debug("    * " + p);
            if (p instanceof DependencyParameter) {
                DependencyParameter dp = (DependencyParameter) p;
                if (taskParams.getType() != TaskParams.Type.SERVICE || dp.getDirection() != DataDirection.INOUT) {
                    transferJobData(dp, listener);
                }
            }
        }
        listener.enable();
    }

    // Private method that performs data transfers
    private void transferJobData(DependencyParameter param, JobTransfersListener listener) {
        Worker<?> w = selectedResource.getResource();
        DataAccessId access = param.getDataAccessId();
        if (access instanceof DataAccessId.WAccessId) {
            String tgtName = ((DataAccessId.WAccessId) access).getWrittenDataInstance().getRenaming();
            if (debug) {
                jobLogger.debug("Setting data target job transfer: " + w.getCompleteRemotePath(param.getType(), tgtName));
            }
            param.setDataTarget(w.getCompleteRemotePath(param.getType(), tgtName));
            return;
        }

        listener.addOperation();
        if (access instanceof DataAccessId.RAccessId) {
            String srcName = ((DataAccessId.RAccessId) access).getReadDataInstance().getRenaming();
            w.getData(srcName, srcName, param, listener);
        } else {
            //Is RWAccess
            String srcName = ((DataAccessId.RWAccessId) access).getReadDataInstance().getRenaming();
            String tgtName = ((DataAccessId.RWAccessId) access).getWrittenDataInstance().getRenaming();
            w.getData(srcName, tgtName, (LogicalData) null, param, listener);
        }
    }

    //EXECUTED BY SUPPORTING THREAD
    public void failedTransfers(int failedtransfers) {
        jobLogger.debug("Received a notification for the transfers for task " + task.getId() + " with state FAILED");
        transferErrors++;
        if (transferErrors < TRANSFER_CHANCES) {
            jobLogger.debug("Resubmitting input files for task " + task.getId() + " to host "
                    + selectedResource.getName() + " since " + failedtransfers + " transfers failed.");

            transferInputData();
        } else {
            ErrorManager.warn("Transfers for running task " + task.getId() + " on worker " + selectedResource.getName() + " have failed.");
            this.notifyError();
        }
    }

    //EXECUTED BY SUPPORTING THREAD
    public void submitJob(int transferGroupId) {
        Worker<?> w = selectedResource.getResource();
        jobLogger.debug("Received a notification for the transfers of task " + task.getId() + " with state DONE");
        JobStatusListener listener = new JobStatusListener(this);
        Job<?> job = w.newJob(task.getId(), task.getTaskParams(), selectedImpl, listener);
        jobs.add(job.getJobId());
        job.setTransferGroupId(transferGroupId);
        job.setHistory(Job.JobHistory.NEW);

        jobLogger.info((this.executingResources.size() > 1 ? "Rescheduled" : "New") + " Job " + job.getJobId() + " (Task: " + task.getId() + ")");
        jobLogger.info("  * Method name: " + task.getTaskParams().getName());
        jobLogger.info("  * Target host: " + selectedResource.getName());
        profile.start();
        JobDispatcher.dispatch(job);
    }

    //EXECUTED BY SUPPORTING THREAD
    public void failedJob(Job<?> job, Job.JobListener.JobEndStatus endStatus) {
        profile.end();
        int jobId = job.getJobId();
        jobLogger.error("Received a notification for job " + jobId + " with state FAILED");
        executionErrors++;
        if ((transferErrors + executionErrors) < SUBMISSION_CHANCES) {
            jobLogger.error("Job " + job.getJobId() + " for running task " + task.getId() + " on worker " + selectedResource.getName() + " has failed; resubmitting task to the same worker.");
            ErrorManager.warn("Job " + job.getJobId() + " for running task " + task.getId() + " on worker " + selectedResource.getName() + " has failed; resubmitting task to the same worker.");
            job.setHistory(Job.JobHistory.RESUBMITTED);
            profile.start();
            JobDispatcher.dispatch(job);
        } else {
            this.notifyError();
        }
    }

    //EXECUTED BY SUPPORTING THREAD
    public void completedJob(Job<?> job) {
        profile.end();
        int jobId = job.getJobId();
        Worker<?> w = selectedResource.getResource();
        jobLogger.info("Received a notification for job " + jobId + " with state OK");
        // Job finished, update info about the generated/updated data
        for (Parameter p : job.getTaskParams().getParameters()) {
            if (p instanceof DependencyParameter) {
                // OUT or INOUT: we must tell the FTM about the generated/updated datum
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
                        if (job.getKind() == Job.JobKind.SERVICE) {
                            continue;
                        }
                        break;
                }

                if (executionType.compareTo(ITConstants.COMPSs) != 0) {
                    if (p instanceof SCOParameter) {
                        SCOParameter scop = (SCOParameter) p;
                        int id = scop.getCode();
                        Object value = scop.getValue();
                        if (value instanceof PSCOId) {
                            PSCOId pscoId = (PSCOId) value;
                            Comm.registerPSCOId(id, pscoId);
                        }
                    }
                }

                String name = dId.getRenaming();

                if (job.getKind() == Job.JobKind.METHOD) {
                    DataLocation outLoc = DataLocation.getLocation(w, dp.getDataTarget());
                    Comm.registerLocation(name, outLoc);
                } else {
                    Object value = job.getReturnValue();
                    Comm.registerValue(name, value);
                }
            }
        }
        task.setStatus(Task.TaskState.FINISHED);
        this.notifyCompleted();
    }

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
            ErrorManager.warn("Task " + task.getId() + " execution on worker " + selectedResource.getName() + " has failed; rescheduling task execution. (changing worker)");
            logger.error("Task " + task.getId() + " execution on worker " + selectedResource.getName() + " has failed; rescheduling task execution. (changing worker)");
        }
    }

    @Override
    protected void doFailed() {
        String taskName = task.getTaskParams().getName();
        StringBuilder sb = new StringBuilder();
        sb.append("Task '").append(taskName).append("' TOTALLY FAILED.\n");
        sb.append("Possible causes:\n");
        sb.append("     -Exception thrown by task '").append(taskName).append("'.\n");
        sb.append("     -Expected output files not generated by task '").append(taskName).append("'.\n");
        sb.append("     -Could not provide nor retrieve needed data between master and worker.\n");
        sb.append("\n");
        sb.append("Check files '").append(Comm.appHost.getJobsDirPath()).append("job[");
        Iterator<Integer> j = jobs.iterator();
        while (j.hasNext()) {
            sb.append(j.next());
            if (!j.hasNext()) {
                break;
            }
            sb.append("|");
        }
        sb.append("'] to find out the error.\n");
        /*sb.append("Task was scheduled on '");
         Iterator<Worker> r = this.executingResources.iterator();
         while (true) {
         sb.append(r.next());
         if (!r.hasNext()) {
         break;
         }
         sb.append(",");
         }
         sb.append(".\n");
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
        return getCoreElementExecutors(task.getTaskParams().getId());
    }

    @Override
    public LinkedList<Implementation<T>> getCompatibleImplementations(ResourceScheduler<P, T> r) {
        return r.getExecutableImpls(task.getTaskParams().getId());
    }

    @Override
    public Implementation<T>[] getImplementations() {
        return (Implementation<T>[]) CoreManager.getCoreImplementations(task.getTaskParams().getId());
    }

    @Override
    public boolean isCompatible(Worker<T> r) {
        return r.canRun(task.getTaskParams().getId());
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        StringBuilder debugString = new StringBuilder("Scheduling " + this + " execution:\n");
        ResourceScheduler<P, T> bestWorker = null;
        Implementation<T> bestImpl = null;
        Score bestScore = null;
        LinkedList<ResourceScheduler<?, ?>> candidates;
        if (isSchedulingConstrained()) {
            candidates = new LinkedList<ResourceScheduler<?, ?>>();
            candidates.add(this.getConstrainingPredecessor().getAssignedResource());
        } else {
            candidates = getCompatibleWorkers();
        }
        int usefulResources = 0;
        for (ResourceScheduler<?, ?> w : candidates) {
            ResourceScheduler<P, T> worker = (ResourceScheduler<P, T>) w;
            if (executingResources.contains(w)) {
                continue;
            }
            Score resourceScore = worker.getResourceScore(this, task.getTaskParams(), actionScore);
            usefulResources++;
            for (Implementation<T> impl : getCompatibleImplementations(worker)) {
                Score implScore = worker.getImplementationScore(this, task.getTaskParams(), impl, resourceScore);
                debugString
                        .append(" Resource ").append(w.getName()).append(" ")
                        .append(" Implementation ").append(impl.getImplementationId()).append(" ")
                        .append(" Score ").append(implScore).append("\n");
                if (Score.isBetter(implScore, bestScore)) {
                    bestWorker = worker;
                    bestImpl = impl;
                    bestScore = implScore;
                }
            }
        }
        if (bestWorker == null) {
            if (usefulResources == 0) {
                logger.debug(debugString.toString());
                logger.info("No worker can run " + this + "\n");
                throw new BlockedActionException();
            } else {
                logger.debug(debugString.toString());
                logger.info("No worker has available resources to run " + this + "\n");
                throw new UnassignedActionException();
            }
        }

        this.assignImplementation(bestImpl);
        this.assignResource(bestWorker);
        logger.debug(debugString.toString());
        logger.info("Assigning action " + this + " to worker" + bestWorker + " with implementation " + bestImpl.getImplementationId() + "\n");
        bestWorker.initialSchedule(this);
    }

    @Override
    public Score schedulingScore(ResourceScheduler<P, T> targetWorker, Score actionScore) {
        return targetWorker.getResourceScore(this, task.getTaskParams(), actionScore);
    }

    @Override
    public void schedule(ResourceScheduler<P, T> targetWorker, Score actionScore) throws BlockedActionException, UnassignedActionException {
        StringBuilder debugString = new StringBuilder("Scheduling " + this + " execution for worker " + targetWorker + ":\n");
        ResourceScheduler<P, T> bestWorker = null;
        Implementation<T> bestImpl = null;
        Score bestScore = null;

        if ( //Resource is not compatible with the Core
                !targetWorker.getResource().canRun(task.getTaskParams().getId())
                // already ran on the resource
                || executingResources.contains(targetWorker)) {
            throw new UnassignedActionException();
        }
        Score resourceScore = targetWorker.getResourceScore(this, task.getTaskParams(), actionScore);
        debugString.append("\t Resource ").append(targetWorker.getName()).append("\n");

        for (Implementation<T> impl : getCompatibleImplementations(targetWorker)) {
            Score implScore = targetWorker.getImplementationScore(this, task.getTaskParams(), impl, resourceScore);
            debugString.append("\t\t Implementation ").append(impl.getImplementationId()).append(implScore).append("\n");
            if (Score.isBetter(implScore, bestScore)) {
                bestWorker = targetWorker;
                bestImpl = impl;
                bestScore = implScore;
            }
        }

        if (bestWorker == null) {
            logger.info("\tWorker " + targetWorker.getName() + "has available resources to run " + this + "\n");
            throw new UnassignedActionException();
        }

        this.assignImplementation(bestImpl);
        this.assignResource(bestWorker);
        logger.info("\t Worker" + bestWorker + " Implementation " + bestImpl.getImplementationId() + "\n");
        logger.debug(debugString.toString());
        bestWorker.initialSchedule(this);
    }

    @Override
    public void schedule(ResourceScheduler<P, T> targetWorker, Implementation impl) throws BlockedActionException, UnassignedActionException {
        StringBuilder debugString = new StringBuilder("Scheduling " + this + " execution for worker " + targetWorker + ":\n");

        if ( //Resource is not compatible with the implementation
                !targetWorker.getResource().canRun(impl)
                // already ran on the resource
                || executingResources.contains(targetWorker)) {
            throw new UnassignedActionException();
        }

        this.assignImplementation(impl);
        this.assignResource(targetWorker);
        logger.info("\t Worker" + targetWorker + " Implementation " + impl.getImplementationId() + "\n");
        logger.debug(debugString.toString());
        targetWorker.initialSchedule(this);
    }

    public String toString() {
        return "SingleExecution ( Task " + task.getId() + ", CE name " + task.getTaskParams().getName() + ")";
    }

    @Override
    public Integer getCoreId() {
        return task.getTaskParams().getId();
    }

    @Override
    public int getPriority() {
        return task.getTaskParams().hasPriority() ? 1 : 0;
    }
}
