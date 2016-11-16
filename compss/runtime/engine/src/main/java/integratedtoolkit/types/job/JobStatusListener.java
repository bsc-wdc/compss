package integratedtoolkit.types.job;

import integratedtoolkit.types.Profile;
import integratedtoolkit.types.allocatableactions.ExecutionAction;
import integratedtoolkit.types.job.Job.JobListener;
import integratedtoolkit.types.resources.WorkerResourceDescription;


public class JobStatusListener<P extends Profile, T extends WorkerResourceDescription> implements JobListener {

    private final ExecutionAction<P, T> execution;


    public JobStatusListener(ExecutionAction<P, T> ex) {
        this.execution = ex;
    }

    @Override
    public void jobCompleted(Job<?> job) {
        execution.completedJob(job);
    }

    @Override
    public void jobFailed(Job<?> job, JobEndStatus endStatus) {
        execution.failedJob(job, endStatus);
    }

}
