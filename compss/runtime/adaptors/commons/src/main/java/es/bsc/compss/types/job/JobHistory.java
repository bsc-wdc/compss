package es.bsc.compss.types.job;

/**
 * Job history.
 */
public enum JobHistory {
    NEW, // New job
    RESUBMITTED_FILES, // Resubmit transfers
    RESUBMITTED, // Resubmit job
    FAILED // Completely failed (can create new job for reschedule)
}
