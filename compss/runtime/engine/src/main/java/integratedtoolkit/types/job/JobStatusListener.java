package integratedtoolkit.types.job;

import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.allocatableactions.ExecutionAction;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.job.JobListener;
import integratedtoolkit.types.resources.WorkerResourceDescription;


public class JobStatusListener<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> implements JobListener {

    private final ExecutionAction<P, T, I> execution;


    public JobStatusListener(ExecutionAction<P, T, I> ex) {
        this.execution = ex;
    }

    @Override
    public void jobCompleted(Job<?> job) {
        // Mark execution as completed
        execution.completedJob(job);
    }

    @Override
    public void jobFailed(Job<?> job, JobEndStatus endStatus) {
        // Mark execution as failed
        execution.failedJob(job, endStatus);
    }

}
