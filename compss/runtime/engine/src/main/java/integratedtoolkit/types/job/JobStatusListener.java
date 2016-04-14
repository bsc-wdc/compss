package integratedtoolkit.types.job;

import integratedtoolkit.components.impl.JobManager;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.job.Job.JobListener;
import integratedtoolkit.types.resources.Worker;

public class JobStatusListener implements JobListener {

    private final Worker<?> worker;
    private final Task task;
    private final JobManager jm;

    public JobStatusListener(Worker<?> worker, Task t, JobManager jm) {
        this.worker = worker;
        this.task = t;
        this.jm = jm;
    }

    @Override
    public void jobCompleted(Job<?> job) {
        jm.completedJob(job, task, worker);
    }

    @Override
    public void jobFailed(Job<?> job, JobEndStatus endStatus) {
        jm.failedJob(job, task, endStatus, worker);
    }

}
