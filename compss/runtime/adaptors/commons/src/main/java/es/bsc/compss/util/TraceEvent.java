/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.util;

/**
 * Representation of a tracing event.
 */
public enum TraceEvent {
    STATIC_IT(1, Tracer.API_EVENTS, "Loading Runtime"), // Static COMPSs
    START(2, Tracer.API_EVENTS, "Start"), // Start
    STOP(3, Tracer.API_EVENTS, "Stop"), // Stop
    TASK(4, Tracer.API_EVENTS, "Execute Task"), // Execute task
    NO_MORE_TASKS(5, Tracer.API_EVENTS, "Waiting for tasks end"), // No more tasks
    WAIT_FOR_ALL_TASKS(6, Tracer.API_EVENTS, "Barrier"), // Waiting for tasks
    OPEN_FILE(7, Tracer.API_EVENTS, "Waiting for open file"), // Open file
    OPEN_DIRECTORY(57, Tracer.API_EVENTS, "Waiting for open directory"), // Open directory
    GET_FILE(8, Tracer.API_EVENTS, "Waiting for get file"), // Get file
    GET_OBJECT(9, Tracer.API_EVENTS, "Waiting for get object"), // Get Object
    GET_DIRECTORY(58, Tracer.API_EVENTS, "Waiting for get Directory"), // Get Directory
    DELETE(12, Tracer.API_EVENTS, "Delete File"), // Delete file
    WAIT_FOR_CONCURRENT(59, Tracer.API_EVENTS, "Wait on concurrent"),

    TASK_RUNNING(11, Tracer.RUNTIME_EVENTS, "Task Running"), // Task running
    WORKER_RECEIVED_NEW_TASK(13, Tracer.RUNTIME_EVENTS, "Received new task"), // New task at worker

    // Access Processor Events which are not in the API
    DEBUG(17, Tracer.RUNTIME_EVENTS, "Access Processor: Debug"), // Debug
    ANALYSE_TASK(18, Tracer.RUNTIME_EVENTS, "Access Processor: Analyse task"), // Analyse task
    UPDATE_GRAPH(19, Tracer.RUNTIME_EVENTS, "Access Processor: Update graph"), // Update graph
    WAIT_FOR_TASK(20, Tracer.RUNTIME_EVENTS, "Access Processor: Wait for task"), // wait for task
    END_OF_APP(21, Tracer.RUNTIME_EVENTS, "Access Processor: End of app"), // End of application
    ALREADY_ACCESSED(22, Tracer.RUNTIME_EVENTS, "Access Processor: Already accessed"), // Already accessed
    REGISTER_DATA_ACCESS(23, Tracer.RUNTIME_EVENTS, "Access Processor: Register data access"), // Register data access
    TRANSFER_OPEN_FILE(24, Tracer.RUNTIME_EVENTS, "Access Processor: Transfer open file"), // Transfer open file
    TRANSFER_RAW_FILE(25, Tracer.RUNTIME_EVENTS, "Access Processor: Transfer raw file"), // Transfer raw file
    TRANSFER_OBJECT(26, Tracer.RUNTIME_EVENTS, "Access Processor: Transfer object"), // Transfer object
    NEW_VERSION_SAME_VALUE(27, Tracer.RUNTIME_EVENTS, "Access Processor: New version same value"), // New version
    IS_OBJECT_HERE(28, Tracer.RUNTIME_EVENTS, "Access Processor: Is object here"), // Is object here
    SET_OBJECT_VERSION_VALUE(29, Tracer.RUNTIME_EVENTS, "Access Processor: Set object version value"), // Set version
    GET_LAST_RENAMING(30, Tracer.RUNTIME_EVENTS, "Access Processor: Get last renaming"), // Get last renaming
    BLOCK_AND_GET_RESULT_FILES(31, Tracer.RUNTIME_EVENTS, "Access Processor: Block and get result files"), // Get files
    UNBLOCK_RESULT_FILES(32, Tracer.RUNTIME_EVENTS, "Access Processor: Unblock result files"), // Unblock result files
    SHUTDOWN(33, Tracer.RUNTIME_EVENTS, "Access Processor: Shutdown"), // Shutdown
    GRAPHSTATE(34, Tracer.RUNTIME_EVENTS, "Access Processor: Graphstate"), // Graph state
    TASKSTATE(35, Tracer.RUNTIME_EVENTS, "Access Processor: Taskstate"), // Task state
    DELETE_FILE(36, Tracer.RUNTIME_EVENTS, "Access Processor: Delete file"), // Delete file
    FINISH_DATA_ACCESS(37, Tracer.RUNTIME_EVENTS, "Access Processor: Finish access to file"), // Finish access to file
    CANCEL_ALL_TASKS(56, Tracer.RUNTIME_EVENTS, "Acces Processor: Cancel all tasks"),

    // Storage Events
    STORAGE_GETBYID(38, Tracer.STORAGE_TYPE, "getByID"), // Get By Id
    STORAGE_NEWREPLICA(39, Tracer.STORAGE_TYPE, "newReplica"), // New replica
    STORAGE_NEWVERSION(40, Tracer.STORAGE_TYPE, "newVersion"), // New version
    STORAGE_INVOKE(41, Tracer.STORAGE_TYPE, "invoke"), // Invoke
    STORAGE_EXECUTETASK(42, Tracer.STORAGE_TYPE, "executeTask"), // Execute task
    STORAGE_GETLOCATIONS(43, Tracer.STORAGE_TYPE, "getLocations"), // Get locations
    STORAGE_CONSOLIDATE(44, Tracer.STORAGE_TYPE, "consolidateVersion"), // Consolidate version

    // Task Dispatcher Events
    ACTION_UPDATE(45, Tracer.RUNTIME_EVENTS, "Task Dispatcher: Action update"), // Action update
    CE_REGISTRATION(46, Tracer.RUNTIME_EVENTS, "Task Dispatcher: CE registration"), // CE registration
    EXECUTE_TASKS(47, Tracer.RUNTIME_EVENTS, "Task Dispatcher: Execute tasks"), // Execute task
    GET_CURRENT_SCHEDULE(48, Tracer.RUNTIME_EVENTS, "Task Dispatcher: Get current schedule"), // Get schedule
    PRINT_CURRENT_GRAPH(49, Tracer.RUNTIME_EVENTS, "Task Dispatcher: Print current graph"), // Print graph
    MONITORING_DATA(50, Tracer.RUNTIME_EVENTS, "Task Dispatcher: Monitoring data"), // Get monitor data
    TD_SHUTDOWN(51, Tracer.RUNTIME_EVENTS, "Task Dispatcher: Shutdown"), // Shutdown
    UPDATE_CEI_LOCAL(52, Tracer.RUNTIME_EVENTS, "Task Dispatcher: Update CEI local"), // Update CEI
    WORKER_UPDATE_REQUEST(53, Tracer.RUNTIME_EVENTS, "Task Dispatcher: Worker update request"), // Update worker

    // Task Events
    CREATING_TASK_SANDBOX(54, Tracer.RUNTIME_EVENTS, "Worker: Creating task sandbox"), // Create task sandbox
    REMOVING_TASK_SANDBOX(55, Tracer.RUNTIME_EVENTS, "Worker: Removing task sandbox"), // Erase task sandbox

    // Python Events Inside Worker
    WORKER_RUNNING(1, Tracer.INSIDE_WORKER_TYPE, "Worker running"), // Worker running

    PROCESS_TASK_PYTHON(2, Tracer.INSIDE_WORKER_TYPE, "Process task"), // Process python task
    PROCESS_PING_PYTHON(3, Tracer.INSIDE_WORKER_TYPE, "Process ping"), // Process python ping
    PROCESS_QUIT_PYTHON(4, Tracer.INSIDE_WORKER_TYPE, "Process quit"), // Process python quit

    INIT_STORAGE(5, Tracer.INSIDE_WORKER_TYPE, "Init storage"), // Init storage
    STOP_STORAGE(6, Tracer.INSIDE_WORKER_TYPE, "Stop storage"), // Stop storage
    INIT_STORAGE_WORKER(7, Tracer.INSIDE_WORKER_TYPE, "Init storage at worker"), // Init storage at worker
    STOP_STORAGE_WORKER(8, Tracer.INSIDE_WORKER_TYPE, "Stop storage at worker"), // Stop storage at worker
    INIT_STORAGE_WORKER_PROCESS(9, Tracer.INSIDE_WORKER_TYPE, "Init storage at worker process"),
    STOP_STORAGE_WORKER_PROCESS(10, Tracer.INSIDE_WORKER_TYPE, "Stop storage at worker process"),

    // Python Events Inside Tasks
    CPU_BINDING_PYTHON(1, Tracer.INSIDE_TASKS_TYPE, "CPU binding"), // CPU binding
    GPU_BINDING_PYTHON(2, Tracer.INSIDE_TASKS_TYPE, "GPU binding"), // GPU binding
    SETUP_ENVIRONMENT_PYTHON(3, Tracer.INSIDE_TASKS_TYPE, "Setup environment variables"), // Setup environment
    GET_TASK_PARAMETERS(4, Tracer.INSIDE_TASKS_TYPE, "Get parameters"), // Get task parameters
    IMPORT_USER_MODULE(5, Tracer.INSIDE_TASKS_TYPE, "Import user module"), // Import user module
    EXECUTE_USER_CODE_PYTHON(6, Tracer.INSIDE_TASKS_TYPE, "User code"), // User code execution
    DESERIALIZE_STRING_PYTHON(7, Tracer.INSIDE_TASKS_TYPE, "Deserializing string"), // Deserialize from string
    DESERIALIZE_OBJECT_PYTHON(8, Tracer.INSIDE_TASKS_TYPE, "Deserializing object"), // Deserialize from file
    SERIALIZE_OBJECT_PYTHON(9, Tracer.INSIDE_TASKS_TYPE, "Serializing object"), // Serialize to file
    SERIALIZE_MPIENV_PYTHON(10, Tracer.INSIDE_TASKS_TYPE, "Serializing object MPI env"), // Serialize to file mpienv
    BUILD_SUCCESS_MESSAGE(11, Tracer.INSIDE_TASKS_TYPE, "Build success message"), // Build successful message
    BUILD_COMPSS_EXCEPTION_MESSAGE(12, Tracer.INSIDE_TASKS_TYPE, "Build COMPSs exception message"),
    BUILD_EXCEPTION_MESSAGE(13, Tracer.INSIDE_TASKS_TYPE, "Build exception message"), // Build exception message
    CLEAN_ENVIRONMENT_PYTHON(14, Tracer.INSIDE_TASKS_TYPE, "Clean environment"), // Clean environment

    GET_BY_ID(15, Tracer.INSIDE_TASKS_TYPE, "Get by ID persistent object"), // Get by id
    GET_ID(16, Tracer.INSIDE_TASKS_TYPE, "Get object ID"), // GetID
    MAKE_PERSISTENT(17, Tracer.INSIDE_TASKS_TYPE, "Make persistent object"), // Make persistent
    DELETE_PERSISTENT(18, Tracer.INSIDE_TASKS_TYPE, "Delete persistent object"), // Delete persistent

    // Other
    READY_COUNT(1, Tracer.READY_COUNTS, "Ready queue count"); // Ready count

    private final int id;
    private final int type;
    private final String signature;


    private TraceEvent(int id, int type, String signature) {
        this.id = id;
        this.type = type;
        this.signature = signature;
    }

    public int getId() {
        return this.id;
    }

    public int getType() {
        return this.type;
    }

    public String getSignature() {
        return this.signature;
    }
}
