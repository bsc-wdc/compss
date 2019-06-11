package es.bsc.compss.types;

/**
 * Task states.
 */
public enum TaskState {
    TO_ANALYSE, // Task is being analysed
    TO_EXECUTE, // Task can be executed
    FINISHED, // Task has finished successfully
    CANCELED, // Task has been canceled
    FAILED // Task has failed
}