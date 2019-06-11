package es.bsc.compss.types.data.operation;

/**
 * End state of any data operation.
 */
public enum OperationEndState {
    OP_OK, // Success
    OP_IN_PROGRESS, // In progress
    OP_FAILED, // Failed
    OP_PREPARATION_FAILED, // Preparation failed
    OP_WAITING_SOURCES; // Waiting for resources
}
