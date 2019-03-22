package es.bsc.compss.types.annotations.parameter;

/**
 * On failure behavior
 */
public enum OnFailure {
    RETRY, // Task will be resubmitted when failed
    FAIL, // Execution will stop when a task fails
    IGNORE, // Execution will continue ignoring the task and the tasks that depend on it
    CANCEL_SUCCESSORS // If execution fails, successors will not be executed
}
