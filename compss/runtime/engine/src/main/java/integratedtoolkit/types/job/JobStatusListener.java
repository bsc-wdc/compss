package integratedtoolkit.types.job;

import integratedtoolkit.types.allocatableactions.ExecutionAction;
import integratedtoolkit.types.job.Job.JobListener;


public class JobStatusListener implements JobListener {

    private final ExecutionAction<?, ?> execution;


    public JobStatusListener(ExecutionAction<?, ?> ex) {
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
