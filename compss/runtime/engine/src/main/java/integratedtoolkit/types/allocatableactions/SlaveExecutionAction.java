package integratedtoolkit.types.allocatableactions;

import integratedtoolkit.components.impl.TaskProducer;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.SchedulingInformation;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.data.operation.JobTransfersListener;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.JobStatusListener;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ResourceScheduler;


public class SlaveExecutionAction<P extends Profile, T extends WorkerResourceDescription> extends ExecutionAction<P, T> {
    
    private final MasterExecutionAction<P,T> master;

    public SlaveExecutionAction(SchedulingInformation<P, T> schedulingInformation, TaskProducer producer, Task task,
            MasterExecutionAction<P,T> master, ResourceScheduler<P, T> forcedResource) {

        super(schedulingInformation, producer, task, forcedResource);
        this.master = master;
    }

    @Override
    protected void transferInputData(JobTransfersListener<P, T> listener) {
        // Nothing to do since it is a phantom action
    }

    @Override
    public Job<?> submitJob(int transferGroupId, JobStatusListener<P, T> listener) {
        Worker<T> w = selectedResource.getResource();
        
        // Notify the master that the slave is active
        master.notifySlaveReady(w, listener);

        Job<?> job = w.newSlaveJob(task.getId(), task.getTaskDescription(), selectedImpl, listener);
        job.setTransferGroupId(transferGroupId);
        job.setHistory(Job.JobHistory.NEW);

        return job;
    }

    @Override
    protected void doOutputTransfers(Job<?> job) {
        // Nothing to do since it is a phantom action
    }

    @Override
    public String toString() {
        return "SlaveExecutionAction ( Task " + task.getId() + ", CE name " + task.getTaskDescription().getName() + ")";
    }

}
