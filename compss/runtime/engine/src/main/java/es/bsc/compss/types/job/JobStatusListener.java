package es.bsc.compss.types.job;

import es.bsc.compss.types.allocatableactions.ExecutionAction;

public class JobStatusListener implements JobListener {

    private final ExecutionAction execution;

    public JobStatusListener(ExecutionAction ex) {
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
