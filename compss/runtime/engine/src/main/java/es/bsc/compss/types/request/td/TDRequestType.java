package es.bsc.compss.types.request.td;

/**
 * Task Dispatcher requests type.
 */
public enum TDRequestType {
    ACTION_UPDATE, // Update action
    CE_REGISTRATION, // Register new coreElement
    EXECUTE_TASKS, // Execute task
    GET_CURRENT_SCHEDULE, // get the current schedule status
    PRINT_CURRENT_GRAPH, // print current task graph
    MONITORING_DATA, // print data for monitoring
    TD_SHUTDOWN, // shutdown
    UPDATE_CEI_LOCAL, // Updates CEI locally
    WORKER_UPDATE_REQUEST // Updates a worker definition
}
