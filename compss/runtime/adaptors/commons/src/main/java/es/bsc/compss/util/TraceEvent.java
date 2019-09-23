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
    STATIC_IT(1, Tracer.RUNTIME_EVENTS, "Loading Runtime"), // Static COMPSs
    START(2, Tracer.RUNTIME_EVENTS, "Start"), // Start
    STOP(3, Tracer.RUNTIME_EVENTS, "Stop"), // Stop
    TASK(4, Tracer.RUNTIME_EVENTS, "Execute Task"), // Execute task
    NO_MORE_TASKS(5, Tracer.RUNTIME_EVENTS, "Waiting for tasks end"), // No more tasks
    WAIT_FOR_ALL_TASKS(6, Tracer.RUNTIME_EVENTS, "Barrier"), // Waiting for tasks
    OPEN_FILE(7, Tracer.RUNTIME_EVENTS, "Waiting for open file"), // Open file
    GET_FILE(8, Tracer.RUNTIME_EVENTS, "Waiting for get file"), // Get file
    GET_OBJECT(9, Tracer.RUNTIME_EVENTS, "Waiting for get object"), // Get Object
    TASK_RUNNING(11, Tracer.RUNTIME_EVENTS, "Task Running"), // Task running
    DELETE(12, Tracer.RUNTIME_EVENTS, "Delete File"), // Delete file
    WORKER_RECEIVED_NEW_TASK(13, Tracer.RUNTIME_EVENTS, "Received new task"), // New task at worker

    // Access Processor Events
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
    FINISH_ACCESS_FILE(37, Tracer.RUNTIME_EVENTS, "Access Processor: Finish acess to file"), // Finish access to file
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
    TASK_EXECUTION_PYTHON(1, Tracer.INSIDE_TASKS_TYPE, "Task execution"), // Execute python task
    USER_CODE_PYTHON(2, Tracer.INSIDE_TASKS_TYPE, "User code execution"), // User code
    IMPORTING_MODULES_PYTHON(3, Tracer.INSIDE_TASKS_TYPE, "Importing modules"), // Import python
    THREAD_BINDING_PYTHON(4, Tracer.INSIDE_TASKS_TYPE, "Thread binding"), // Thread binding
    DESERIALIZE_OBJECT_PYTHON1(5, Tracer.INSIDE_TASKS_TYPE, "Deserializing object"), // Deserialize
    DESERIALIZE_OBJECT_PYTHON2(6, Tracer.INSIDE_TASKS_TYPE, "Deserializing object"), // Deserialize
    SERIALIZE_OBJECT_PYTHON(7, Tracer.INSIDE_TASKS_TYPE, "Serializing object"), // Serialize
    CREATE_THREADS_PYTHON(8, Tracer.INSIDE_TASKS_TYPE, "Create persistent threads"), // Create threads python
    GET_BY_ID(9, Tracer.INSIDE_TASKS_TYPE, "Get by ID persistent object"), // Get by id
    MAKE_PERSISTENT(10, Tracer.INSIDE_TASKS_TYPE, "Make persistent object"), // Make persistent
    DELETE_PERSISTENT(11, Tracer.INSIDE_TASKS_TYPE, "Delete persistent object"), // Delete persistent
    WORKER_RUNNING(102, Tracer.INSIDE_TASKS_TYPE, "Worker running"), // Worker running

    READY_COUNT(1, Tracer.READY_COUNTS, "Ready queue count"),
    CPU_COUNT(1, Tracer.NUMBER_OF_CPUS, "Number of CPUs");// Ready count

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
