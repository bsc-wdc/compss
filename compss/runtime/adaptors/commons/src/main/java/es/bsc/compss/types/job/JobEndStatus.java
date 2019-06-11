package es.bsc.compss.types.job;

/**
 * Job status types.
 */
public enum JobEndStatus {
    OK, // Success status
    TO_RESCHEDULE, // Task must be rescheduled
    TRANSFERS_FAILED, // Task transfers failed
    SUBMISSION_FAILED, // Task submission failed
    EXECUTION_FAILED; // Task execution failed
}
