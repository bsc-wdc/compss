package es.bsc.compss.types.job;

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
        OK, // Success status
        TO_RESCHEDULE, // Task must be rescheduled
        TRANSFERS_FAILED, // Task transfers failed
        SUBMISSION_FAILED, // Task submission failed
        EXECUTION_FAILED; // Task execution failed
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
