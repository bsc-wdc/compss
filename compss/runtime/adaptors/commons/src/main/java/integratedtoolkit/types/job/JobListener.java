package integratedtoolkit.types.job;

/**
 * Abstract Representation of a listener for the job execution
 * 
 */
public interface JobListener {

    /**
     * Job status types
     *
     */
    public enum JobEndStatus {
        OK, 
        TO_RESCHEDULE, 
        TRANSFERS_FAILED,
        SUBMISSION_FAILED, 
        EXECUTION_FAILED;
    }


    /**
     * Actions when job has successfully ended
     * 
     * @param job
     */
    public void jobCompleted(Job<?> job);

    /**
     * Actions when job has failed
     * 
     * @param job
     * @param endStatus
     */
    public void jobFailed(Job<?> job, JobEndStatus endStatus);

}