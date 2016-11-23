package integratedtoolkit.types.job;

import java.util.ArrayList;
import java.util.List;

import integratedtoolkit.types.Profile;
import integratedtoolkit.types.allocatableactions.ExecutionAction;
import integratedtoolkit.types.job.JobListener;
import integratedtoolkit.types.resources.WorkerResourceDescription;


public class JobStatusListener<P extends Profile, T extends WorkerResourceDescription> implements JobListener {

    private final ExecutionAction<P, T> execution;
    private final List<JobStatusListener<P, T>> slaveListeners;


    public JobStatusListener(ExecutionAction<P, T> ex) {
        this.execution = ex;
        this.slaveListeners = new ArrayList<>();
    }

    public void addSlaveListeners(List<JobStatusListener<P, T>> slaveListeners) {
        this.slaveListeners.addAll(slaveListeners);
    }

    @Override
    public void jobCompleted(Job<?> job) {
        execution.completedJob(job);

        // Free slaves
        for (JobStatusListener<P, T> slaveListener : slaveListeners) {
            slaveListener.jobCompleted(job);
        }
    }

    @Override
    public void jobFailed(Job<?> job, JobEndStatus endStatus) {
        execution.failedJob(job, endStatus);

        // Free slaves
        for (JobStatusListener<P, T> slaveListener : slaveListeners) {
            slaveListener.jobFailed(job, endStatus);
        }
    }

}
