package integratedtoolkit.types.allocatableactions;

import integratedtoolkit.api.COMPSsRuntime.DataDirection;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.components.impl.TaskDispatcher.TaskProducer;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Implementation.Type;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.SchedulingInformation;
import integratedtoolkit.util.ResourceScheduler;
import integratedtoolkit.types.data.DataAccessId;
import integratedtoolkit.types.data.DataInstanceId;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.operation.JobTransfersListener;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.JobStatusListener;
import integratedtoolkit.types.parameter.DependencyParameter;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.types.uri.SimpleURI;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.JobDispatcher;

import java.util.Iterator;
import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class ExecutionAction<P extends Profile, T extends WorkerResourceDescription> extends AllocatableAction<P, T> {

	private static final int TRANSFER_CHANCES = 2;
	private static final int SUBMISSION_CHANCES = 2;
	private static final int SCHEDULING_CHANCES = 2;

	// LOGGER
	private static final Logger jobLogger = LogManager.getLogger(Loggers.JM_COMP);

	// Execution Info
	private final TaskProducer producer;
	protected final Task task;
	private int transferErrors = 0;
	private int executionErrors = 0;
	private LinkedList<Integer> jobs = new LinkedList<Integer>();


	public ExecutionAction(SchedulingInformation<P, T> schedulingInformation, TaskProducer producer, Task task) {
		super(schedulingInformation);

		this.producer = producer;
		this.task = task;
		task.setExecution(this);

		// Register data dependencies events
		for (Task predecessor : task.getPredecessors()) {
			ExecutionAction<P, T> e = (ExecutionAction<P, T>) predecessor.getExecution();
			if (e != null && e.isPending()) {
				this.addDataPredecessor(e);
			}
		}

		// Scheduling constraints
		// Restricted resource
		Task resourceConstraintTask = task.getEnforcingTask();
		if (resourceConstraintTask != null) {
			ExecutionAction<P, T> e = (ExecutionAction<P, T>) resourceConstraintTask.getExecution();
			this.setResourceConstraint(e);
		}
	}

	public Task getTask() {
		return this.task;
	}

	@Override
	protected void doAction() {
		jobLogger.info("Ordering transfers to " + selectedMainResource + " to run task: " + task.getId());
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
				if (taskParams.getType() != Type.SERVICE || dp.getDirection() != DataDirection.INOUT) {
					transferJobData(dp, listener);
				}
			}
		}
		listener.enable();
	}

	// Private method that performs data transfers
	private void transferJobData(DependencyParameter param, JobTransfersListener listener) {
		Worker<?> w = selectedMainResource.getResource();
		DataAccessId access = param.getDataAccessId();
		if (access instanceof DataAccessId.WAccessId) {
			String tgtName = ((DataAccessId.WAccessId) access).getWrittenDataInstance().getRenaming();
			if (debug) {
				jobLogger.debug("Setting data target job transfer: " + w.getCompleteRemotePath(param.getType(), tgtName));
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

	// EXECUTED BY SUPPORTING THREAD
	public void failedTransfers(int failedtransfers) {
		jobLogger.debug("Received a notification for the transfers for task " + task.getId() + " with state FAILED");
		transferErrors++;
		if (transferErrors < TRANSFER_CHANCES) {
			jobLogger.debug("Resubmitting input files for task " + task.getId() + " to host " + selectedMainResource.getName() + " since "
					+ failedtransfers + " transfers failed.");

			transferInputData();
		} else {
			ErrorManager.warn("Transfers for running task " + task.getId() + " on worker " + selectedMainResource.getName()
					+ " have failed.");
			this.notifyError();
		}
	}

	// EXECUTED BY SUPPORTING THREAD
	public void submitJob(int transferGroupId) {
		Worker<?> w = selectedMainResource.getResource();
		jobLogger.debug("Received a notification for the transfers of task " + task.getId() + " with state DONE");
		JobStatusListener listener = new JobStatusListener(this);
		Job<?> job = w.newJob(task.getId(), task.getTaskParams(), selectedImpl, listener);
		jobs.add(job.getJobId());
		job.setTransferGroupId(transferGroupId);
		job.setHistory(Job.JobHistory.NEW);

		jobLogger.info((this.executingResources.size() > 1 ? "Rescheduled" : "New") + " Job " + job.getJobId() + " (Task: " + task.getId()
				+ ")");
		jobLogger.info("  * Method name: " + task.getTaskParams().getName());
		jobLogger.info("  * Target host: " + selectedMainResource.getName());
		profile.start();
		JobDispatcher.dispatch(job);
	}

	// EXECUTED BY SUPPORTING THREAD
	public void failedJob(Job<?> job, Job.JobListener.JobEndStatus endStatus) {
		profile.end();
		int jobId = job.getJobId();
		jobLogger.error("Received a notification for job " + jobId + " with state FAILED");
		executionErrors++;
		if ((transferErrors + executionErrors) < SUBMISSION_CHANCES) {
			jobLogger.error("Job " + job.getJobId() + " for running task " + task.getId() + " on worker " + selectedMainResource.getName()
					+ " has failed; resubmitting task to the same worker.");
			ErrorManager.warn("Job " + job.getJobId() + " for running task " + task.getId() + " on worker "
					+ selectedMainResource.getName() + " has failed; resubmitting task to the same worker.");
			job.setHistory(Job.JobHistory.RESUBMITTED);
			profile.start();
			JobDispatcher.dispatch(job);
		} else {
			this.notifyError();
		}
	}

	// EXECUTED BY SUPPORTING THREAD
	public void completedJob(Job<?> job) {
		profile.end();
		int jobId = job.getJobId();
		Worker<?> w = selectedMainResource.getResource();
		jobLogger.info("Received a notification for job " + jobId + " with state OK");

		// Job finished, update info about the generated/updated data
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
						if (job.getKind() == Job.JobKind.SERVICE) {
							continue;
						}
						break;
				}

				String name = dId.getRenaming();
				if (job.getKind() == Job.JobKind.METHOD) {
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
		task.setStatus(Task.TaskState.FINISHED);
		this.notifyCompleted();
	}

	@Override
	protected void doCompleted() {
		selectedMainResource.profiledExecution(selectedImpl, profile);
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
			ErrorManager.warn("Task " + task.getId() + " execution on worker " + selectedMainResource.getName()
					+ " has failed; rescheduling task execution. (changing worker)");
			logger.error("Task " + task.getId() + " execution on worker " + selectedMainResource.getName()
					+ " has failed; rescheduling task execution. (changing worker)");
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
	public Integer getCoreId() {
		return task.getTaskParams().getId();
	}

	@Override
	public int getPriority() {
		return task.getTaskParams().hasPriority() ? 1 : 0;
	}

	@Override
	public String toString() {
		return "ExecutionAction ( Task " + task.getId() + ", CE name " + task.getTaskParams().getName() + ")";
	}

}
