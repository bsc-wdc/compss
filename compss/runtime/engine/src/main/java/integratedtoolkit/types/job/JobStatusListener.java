package integratedtoolkit.types.job;

import integratedtoolkit.types.allocatableactions.SingleExecution;
import integratedtoolkit.types.job.Job.JobListener;


public class JobStatusListener implements JobListener {

    private final SingleExecution<?,?> execution;

    public JobStatusListener(SingleExecution<?,?> ex) {
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
