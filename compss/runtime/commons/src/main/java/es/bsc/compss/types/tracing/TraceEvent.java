/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.tracing;

/**
 * Representation of a tracing event.
 */
public enum TraceEvent {

    STATIC_IT(1, TraceEventType.API, "Loading Runtime"), //
    START(2, TraceEventType.API, "Start"), //
    STOP(3, TraceEventType.API, "Stop"), //
    TASK(4, TraceEventType.API, "Execute Task"), //
    NO_MORE_TASKS(5, TraceEventType.API, "Waiting for tasks end"), //
    WAIT_FOR_ALL_TASKS(6, TraceEventType.API, "Barrier"), //
    OPEN_FILE(7, TraceEventType.API, "Waiting for open file"), //
    OPEN_DIRECTORY(57, TraceEventType.API, "Waiting for open directory"), //
    GET_FILE(8, TraceEventType.API, "Waiting for get file"), //
    GET_OBJECT(9, TraceEventType.API, "Waiting for get object"), //
    GET_BINDING_OBJECT(10, TraceEventType.API, "Waiting for get binding object"), //
    GET_DIRECTORY(58, TraceEventType.API, "Waiting for get Directory"), //
    DELETE(12, TraceEventType.API, "Delete File"), //
    WAIT_FOR_CONCURRENT(59, TraceEventType.API, "Wait on concurrent"), //
    SNAPSHOT_API(80, TraceEventType.API, "Snapshot request"), //

    // Worker runtime events
    TASK_RUNNING(11, TraceEventType.RUNTIME, "Task Running"), //
    WORKER_RECEIVED_NEW_TASK(13, TraceEventType.RUNTIME, "Received new task"), //

    // Runtime Task Events
    CREATING_TASK_SANDBOX(54, TraceEventType.RUNTIME, "Worker: Creating task sandbox"), //
    REMOVING_TASK_SANDBOX(55, TraceEventType.RUNTIME, "Worker: Removing task sandbox"), //
    FETCH_PARAM(60, TraceEventType.RUNTIME, "Fetch task parameter"), //
    REMOVE_OBSOLETES(61, TraceEventType.RUNTIME, "Remove Obsoletes"), //
    BIND_ORIG_NAME(62, TraceEventType.RUNTIME, "Bind Original File names To Renames.."), //
    UNBIND_ORIG_NAME(63, TraceEventType.RUNTIME, "Unbind Original File names To Renames.."), //
    CHECK_OUT_PARAM(64, TraceEventType.RUNTIME, "Check OUT parameters."), //
    INSTRUMENTING_CLASS(90, TraceEventType.RUNTIME, "Instrumenting nested class."), //

    // Runtime FS Events
    INIT_FS(65, TraceEventType.RUNTIME, "Init Thread for synch file system operations"), //
    LOCAL_COPY(66, TraceEventType.RUNTIME, "Local copy"), //
    LOCAL_MOVE(67, TraceEventType.RUNTIME, "Local move"), //
    LOCAL_DELETE(68, TraceEventType.RUNTIME, "Local delete"), //
    LOCAL_SERIALIZE(69, TraceEventType.RUNTIME, "Serializing Object"), //

    // Access Processor Events which are not in the API
    DEBUG(17, TraceEventType.RUNTIME, "Access Processor: Debug"), //
    ANALYSE_TASK(18, TraceEventType.RUNTIME, "Access Processor: Analyse task"), //
    UPDATE_GRAPH(19, TraceEventType.RUNTIME, "Access Processor: Update graph"), //
    WAIT_FOR_DATA(20, TraceEventType.RUNTIME, "Access Processor: Wait for data"), //
    END_OF_APP(21, TraceEventType.RUNTIME, "Access Processor: End of app"), //
    ALREADY_ACCESSED(22, TraceEventType.RUNTIME, "Access Processor: Already accessed"), //
    REGISTER_DATA_ACCESS(23, TraceEventType.RUNTIME, "Access Processor: Register data access"), //
    TRANSFER_OPEN_FILE(24, TraceEventType.RUNTIME, "Access Processor: Transfer open file"), //
    TRANSFER_RAW_FILE(25, TraceEventType.RUNTIME, "Access Processor: Transfer raw file"), //
    TRANSFER_OBJECT(26, TraceEventType.RUNTIME, "Access Processor: Transfer object"), //
    NEW_VERSION_SAME_VALUE(27, TraceEventType.RUNTIME, "Access Processor: New version same value"), //
    BLOCK_AND_GET_RESULT_FILES(31, TraceEventType.RUNTIME, "Access Processor: Block and get result files"), //
    UNBLOCK_RESULT_FILES(32, TraceEventType.RUNTIME, "Access Processor: Unblock result files"), //
    SHUTDOWN(33, TraceEventType.RUNTIME, "Access Processor: Shutdown"), //
    GRAPHSTATE(34, TraceEventType.RUNTIME, "Access Processor: Graphstate"), //
    TASKSTATE(35, TraceEventType.RUNTIME, "Access Processor: Taskstate"), //
    DELETE_DATA(36, TraceEventType.RUNTIME, "Access Processor: Delete Data"), //
    FINISH_DATA_ACCESS(37, TraceEventType.RUNTIME, "Access Processor: Finish access to file"), //
    REGISTER_REMOTE_DATA(38, TraceEventType.RUNTIME, "Access Processor: Register remote data access"), //
    CANCEL_TASK_GROUP(41, TraceEventType.RUNTIME, "Access Processor: Cancel task group"), //
    REMOVE_APP_DATA(42, TraceEventType.RUNTIME, "Access Processor: Remove application data"), //
    CANCEL_ALL_TASKS(56, TraceEventType.RUNTIME, "Access Processor: Cancel all tasks"), //
    CP_SHUTDOWN_NOTIFICATION(81, TraceEventType.RUNTIME, "Access Processor: Checkpointed shutdown notification"), //
    AP_SNAPSHOT(82, TraceEventType.RUNTIME, "Access Processor: Snapshot"), //
    AP_CHECKPOINT_REQUEST(83, TraceEventType.RUNTIME, "Access Processor: CheckpointManager Request"), //
    AP_GET_LAST_DATA(91, TraceEventType.RUNTIME, "Obtaining last data"), //

    // Task Dispatcher Events
    ACTION_UPDATE(45, TraceEventType.RUNTIME, "Task Dispatcher: Action update"), //
    CE_REGISTRATION(46, TraceEventType.RUNTIME, "Task Dispatcher: CE registration"), //
    EXECUTE_TASKS(47, TraceEventType.RUNTIME, "Task Dispatcher: Execute tasks"), //
    GET_CURRENT_SCHEDULE(48, TraceEventType.RUNTIME, "Task Dispatcher: Get current schedule"), //
    PRINT_CURRENT_GRAPH(49, TraceEventType.RUNTIME, "Task Dispatcher: Print current graph"), //
    MONITORING_DATA(50, TraceEventType.RUNTIME, "Task Dispatcher: Monitoring data"), //
    TD_SHUTDOWN(51, TraceEventType.RUNTIME, "Task Dispatcher: Shutdown"), //
    UPDATE_CEI_LOCAL(52, TraceEventType.RUNTIME, "Task Dispatcher: Update CEI local"), //
    WORKER_UPDATE_REQUEST(53, TraceEventType.RUNTIME, "Task Dispatcher: Worker update request"), //
    CANCEL_TASKS(57, TraceEventType.RUNTIME, "Task Dispatcher: Cancel tasks"), //
    WORKER_RESTART_REQUEST(59, TraceEventType.RUNTIME, "Task Dispatcher: Re-starting worker"), //

    // Timer events
    TASK_TIMEOUT(58, TraceEventType.RUNTIME, "Timer: Task timed out"), //

    // Storage Events
    STORAGE_GETBYID(38, TraceEventType.STORAGE_TYPE, "getByID"), //
    STORAGE_NEWREPLICA(39, TraceEventType.STORAGE_TYPE, "newReplica"), //
    STORAGE_NEWVERSION(40, TraceEventType.STORAGE_TYPE, "newVersion"), //
    STORAGE_INVOKE(41, TraceEventType.STORAGE_TYPE, "invoke"), //
    STORAGE_EXECUTETASK(42, TraceEventType.STORAGE_TYPE, "executeTask"), //
    STORAGE_GETLOCATIONS(43, TraceEventType.STORAGE_TYPE, "getLocations"), //
    STORAGE_CONSOLIDATE(44, TraceEventType.STORAGE_TYPE, "consolidateVersion"), //
    // Python Events Inside Worker
    WORKER_RUNNING(1, TraceEventType.BINDING_INSIDE_WORKER, "Worker running"), //
    PROCESS_TASK_PYTHON(2, TraceEventType.BINDING_INSIDE_WORKER, "Process task"), //
    PROCESS_PING_PYTHON(3, TraceEventType.BINDING_INSIDE_WORKER, "Process ping"), //
    PROCESS_QUIT_PYTHON(4, TraceEventType.BINDING_INSIDE_WORKER, "Process quit"), //
    INIT_STORAGE(5, TraceEventType.BINDING_INSIDE_WORKER, "Init storage"), //
    STOP_STORAGE(6, TraceEventType.BINDING_INSIDE_WORKER, "Stop storage"), //
    INIT_STORAGE_WORKER(7, TraceEventType.BINDING_INSIDE_WORKER, "Init storage at worker"), //
    STOP_STORAGE_WORKER(8, TraceEventType.BINDING_INSIDE_WORKER, "Stop storage at worker"), //
    INIT_STORAGE_WORKER_PROCESS(9, TraceEventType.BINDING_INSIDE_WORKER, "Init storage at worker process"), //
    STOP_STORAGE_WORKER_PROCESS(10, TraceEventType.BINDING_INSIDE_WORKER, "Stop storage at worker process"), //
    LOAD_EAR(11, TraceEventType.BINDING_INSIDE_WORKER, "Load EAR at worker"), //
    FINALIZE_EAR(12, TraceEventType.BINDING_INSIDE_WORKER, "Finalize EAR at worker"), //
    PRELOAD_IMPORT(13, TraceEventType.BINDING_INSIDE_WORKER, "Preload imports"), //

    // Python Events Inside Tasks
    CPU_BINDING_PYTHON(1, TraceEventType.BINDING_INSIDE_TASKS, "CPU binding"), //
    GPU_BINDING_PYTHON(2, TraceEventType.BINDING_INSIDE_TASKS, "GPU binding"), //
    SETUP_ENVIRONMENT_PYTHON(3, TraceEventType.BINDING_INSIDE_TASKS, "Setup environment variables"), //
    GET_TASK_PARAMETERS(4, TraceEventType.BINDING_INSIDE_TASKS, "Get parameters"), //
    IMPORT_USER_MODULE(5, TraceEventType.BINDING_INSIDE_TASKS, "Import user module"), //
    EXECUTE_USER_CODE_PYTHON(6, TraceEventType.BINDING_INSIDE_TASKS, "User code"), //
    DESERIALIZE_STRING_PYTHON(7, TraceEventType.BINDING_INSIDE_TASKS, "Deserializing string"), //
    DESERIALIZE_OBJECT_PYTHON(8, TraceEventType.BINDING_INSIDE_TASKS, "Deserializing object"), //
    SERIALIZE_OBJECT_PYTHON(9, TraceEventType.BINDING_INSIDE_TASKS, "Serializing object"), //
    SERIALIZE_MPIENV_PYTHON(10, TraceEventType.BINDING_INSIDE_TASKS, "Serializing object MPI env"), //
    BUILD_SUCCESS_MESSAGE(11, TraceEventType.BINDING_INSIDE_TASKS, "Build success message"), //
    BUILD_COMPSS_EXCEPTION_MESSAGE(12, TraceEventType.BINDING_INSIDE_TASKS, "Build COMPSs exception message"), //
    BUILD_EXCEPTION_MESSAGE(13, TraceEventType.BINDING_INSIDE_TASKS, "Build exception message"), //
    CLEAN_ENVIRONMENT_PYTHON(14, TraceEventType.BINDING_INSIDE_TASKS, "Clean environment"), //
    GET_BY_ID(15, TraceEventType.BINDING_INSIDE_TASKS, "Get by ID persistent object"), //
    GET_ID(16, TraceEventType.BINDING_INSIDE_TASKS, "Get object ID"), //
    MAKE_PERSISTENT(17, TraceEventType.BINDING_INSIDE_TASKS, "Make persistent object"), //
    DELETE_PERSISTENT(18, TraceEventType.BINDING_INSIDE_TASKS, "Delete persistent object"), //
    RETRIEVE_OBJECT_INTO_CACHE(19, TraceEventType.BINDING_INSIDE_TASKS, "Get object from cache"), //
    INSERT_OBJECT_INTO_CACHE(20, TraceEventType.BINDING_INSIDE_TASKS, "Put object in cache"), //
    REMOVE_OBJECT_FROM_CACHE(21, TraceEventType.BINDING_INSIDE_TASKS, "Remove object from cache"), //
    WAIT_ON_PYTHON(22, TraceEventType.BINDING_INSIDE_TASKS, "Wait on"), //
    WORKER_TASK_INSTANTIATION(23, TraceEventType.BINDING_INSIDE_TASKS, "Task instantiation"), //
    CACHE_HIT(24, TraceEventType.BINDING_INSIDE_TASKS, "Cache hit"), //
    CACHE_MISS(25, TraceEventType.BINDING_INSIDE_TASKS, "Cache miss"), //
    CACHE_GPU_ACCESS(26, TraceEventType.BINDING_INSIDE_TASKS, "Check GPU Access"), //
    CACHE_GPU_HIT(27, TraceEventType.BINDING_INSIDE_TASKS, "Cache hit GPU"), //
    CACHE_GPU_MISS(28, TraceEventType.BINDING_INSIDE_TASKS, "Cache miss GPU"), //
    RETRIEVE_OBJECT_FROM_GPU_CACHE(29, TraceEventType.BINDING_INSIDE_TASKS, "Get object from GPU cache"), //
    INSERT_OBJECT_INTO_GPU_CACHE(30, TraceEventType.BINDING_INSIDE_TASKS, "Put object in GPU cache"), //
    CLEANUP_TASK(31, TraceEventType.BINDING_INSIDE_TASKS, "Cleanup Task"), //
    EXECUTOR_LOAD_EAR(32, TraceEventType.BINDING_INSIDE_TASKS, "Import EAR"), //
    EXECUTOR_FINALIZE_EAR(33, TraceEventType.BINDING_INSIDE_TASKS, "Finalize EAR"), //
    MANAGE_NEW_TYPES(34, TraceEventType.BINDING_INSIDE_TASKS, "Manage new types"), //
    RELEASE_MEMORY(35, TraceEventType.BINDING_INSIDE_TASKS, "Release memory"), //

    // Python Events Inside Tasks
    CACHE_MSG_RECEIVE(1, TraceEventType.BINDING_WORKER_CACHE, "Receive message"), //
    CACHE_MSG_QUIT(2, TraceEventType.BINDING_WORKER_CACHE, "Quit"), //
    CACHE_MSG_END_PROFILING(3, TraceEventType.BINDING_WORKER_CACHE, "End profiling"), //
    CACHE_MSG_GET_EVENT(4, TraceEventType.BINDING_WORKER_CACHE, "Get from cache"), //
    CACHE_MSG_PUT_EVENT(5, TraceEventType.BINDING_WORKER_CACHE, "Put into cache"), //
    CACHE_MSG_REMOVE(6, TraceEventType.BINDING_WORKER_CACHE, "Remove from cache"), //
    CACHE_MSG_LOCK(7, TraceEventType.BINDING_WORKER_CACHE, "Lock cache entry"), //
    CACHE_MSG_UNLOCK(8, TraceEventType.BINDING_WORKER_CACHE, "Unlock cache entry"), //
    CACHE_MSG_IS_LOCKED(9, TraceEventType.BINDING_WORKER_CACHE, "Check if entry is locked"), //
    CACHE_MSG_IS_IN_CACHE(10, TraceEventType.BINDING_WORKER_CACHE, "Check if object is in cache"), //

    // Python Master Events
    PYTHON_START_RUNTIME(1, TraceEventType.BINDING_MASTER, "Start runtime"), //
    PYTHON_STOP_RUNTIME(2, TraceEventType.BINDING_MASTER, "Stop runtime"), //
    PYTHON_APPLICATION_RUNNING(3, TraceEventType.BINDING_MASTER, "Application running"), //
    // 4 is empty
    PYTHON_MASTER_INIT_STORAGE(5, TraceEventType.BINDING_MASTER, "Start storage"), //
    PYTHON_MASTER_STOP_STORAGE(6, TraceEventType.BINDING_MASTER, "Stop storage"), //
    PYTHON_ACCESSED_FILE(7, TraceEventType.BINDING_MASTER, "Accessed file"), //
    PYTHON_OPEN_FILE(8, TraceEventType.BINDING_MASTER, "Open file"), //
    PYTHON_DELETE_FILE(9, TraceEventType.BINDING_MASTER, "Delete file"), //
    PYTHON_GET_FILE(10, TraceEventType.BINDING_MASTER, "Get file"), //
    PYTHON_GET_DIRECTORY(11, TraceEventType.BINDING_MASTER, "Get directory"), //
    PYTHON_DELETE_OBJECT(12, TraceEventType.BINDING_MASTER, "Delete object"), //
    PYTHON_BARRIER(13, TraceEventType.BINDING_MASTER, "Barrier"), //
    PYTHON_BARRIER_GROUP(14, TraceEventType.BINDING_MASTER, "Barrier group"), //
    PYTHON_OPEN_TASK_GROUP(15, TraceEventType.BINDING_MASTER, "Open task group"), //
    PYTHON_CLOSE_TASK_GROUP(16, TraceEventType.BINDING_MASTER, "Close task group"), //
    PYTHON_GET_LOG_PATH(17, TraceEventType.BINDING_MASTER, "Get log path"), //
    PYTHON_GET_TMP_PATH(18, TraceEventType.BINDING_MASTER, "Get tmp path (master working dir)"), //
    PYTHON_GET_NUMBER_RESOURCES(19, TraceEventType.BINDING_MASTER, "Get number of resources"), //
    PYTHON_REQUEST_RESOURCES(20, TraceEventType.BINDING_MASTER, "Request resources"), //
    PYTHON_FREE_RESOURCES(21, TraceEventType.BINDING_MASTER, "Free resources"), //
    PYTHON_REGISTER_CORE_ELEMENT(22, TraceEventType.BINDING_MASTER, "Register Core Element"), //
    PYTHON_WAIT_ON(23, TraceEventType.BINDING_MASTER, "Wait on"), //
    PYTHON_PROCESS_TASK(24, TraceEventType.BINDING_MASTER, "Call to process task"), //
    PYTHON_WALL_CLOCK_LIMIT(25, TraceEventType.BINDING_MASTER, "Wall clock limit"), //
    PYTHON_SNAPSHOT(26, TraceEventType.BINDING_MASTER, "Snapshot"), //
    PYTHON_CANCEL_TASK_GROUP(27, TraceEventType.BINDING_MASTER, "Cancel task group"), //

    // Internal events
    PYTHON_TASK_INSTANTIATION(100, TraceEventType.BINDING_MASTER, "Task instantiation"), //
    PYTHON_INSPECT_FUNCTION_ARGUMENTS(101, TraceEventType.BINDING_MASTER, "Inspect function arguments"), //
    PYTHON_INSPECT_CONSTRAINTS(102, TraceEventType.BINDING_MASTER, "Inspect constraints"), //
    PYTHON_GET_FUNCTION_INFORMATION(103, TraceEventType.BINDING_MASTER, "Get function information"), //
    PYTHON_GET_FUNCTION_SIGNATURE(104, TraceEventType.BINDING_MASTER, "Check function signature"), //
    PYTHON_CHECK_INTERACTIVE(105, TraceEventType.BINDING_MASTER, "Check interactive"), //
    PYTHON_EXTRACT_CORE_ELEMENT(106, TraceEventType.BINDING_MASTER, "Extract core element"), //
    PYTHON_PREPARE_CORE_ELEMENT(107, TraceEventType.BINDING_MASTER, "Prepare Core Element"), //
    PYTHON_UPDATE_CORE_ELEMENT(108, TraceEventType.BINDING_MASTER, "Update Core Element"), //
    PYTHON_GET_UPPER_DECORATORS_KWARGS(109, TraceEventType.BINDING_MASTER, "Get upper decorators kwargs"), //
    PYTHON_PROCESS_OTHER_ARGUMENTS(110, TraceEventType.BINDING_MASTER, "Process task hints"), //
    PYTHON_PROCESS_PARAMETERS(111, TraceEventType.BINDING_MASTER, "Process function parameters"), //
    PYTHON_PROCESS_RETURN(112, TraceEventType.BINDING_MASTER, "Process return"), //
    PYTHON_BUILD_RETURN_OBJECTS(113, TraceEventType.BINDING_MASTER, "Build return objects"), //
    PYTHON_SERIALIZE_OBJECTS(114, TraceEventType.BINDING_MASTER, "Serialize objects"), //
    PYTHON_BUILD_COMPSS_TYPES_DIRECTIONS(115, TraceEventType.BINDING_MASTER, "Build COMPSs types and directions"), //
    PYTHON_PROCESS_TASK_BINDING(116, TraceEventType.BINDING_MASTER, "Process task binding"), //
    PYTHON_ATTRIBUTES_CLEANUP(117, TraceEventType.BINDING_MASTER, "Cleanup"), //

    // Agent events
    AGENT_ADD_RESOURCE(6002, TraceEventType.AGENT, "Add resources agent"), //
    AGENT_STOP(6003, TraceEventType.AGENT, "Stop agent"), //
    AGENT_REMOVE_NODE(6004, TraceEventType.AGENT, "Remove node agent"), //
    AGENT_REMOVE_RESOURCES(6005, TraceEventType.AGENT, "Remove resources agent"), //
    AGENT_RUN_TASK(6006, TraceEventType.AGENT, "Run task agent"), //

    // Checkpointer events
    CHECKPOINT_SHUTDOWN(7001, TraceEventType.CHECKPOINT_EVENTS_TYPE, "CheckpointManager shutdown"),

    CHECKPOINT_NEW_TASK(7002, TraceEventType.CHECKPOINT_EVENTS_TYPE, "Checkpoint New task"), // New task
    CHECKPOINT_END_TASK(7003, TraceEventType.CHECKPOINT_EVENTS_TYPE, "Checkpoint end task"), // End task
    CHECKPOINT_MAIN_ACCESS(7004, TraceEventType.CHECKPOINT_EVENTS_TYPE, "Checkpoint main data access"), // Main access
    CHECKPOINT_DELETE_DATA(7005, TraceEventType.CHECKPOINT_EVENTS_TYPE, "Checkpoint deletes data"), // delete data
    CHECKPOINT_SNAPSHOT(7006, TraceEventType.CHECKPOINT_EVENTS_TYPE, "Checkpoint snapshot"), // Snapshot

    SAVE_LAST_DATA_VERSIONS(7011, TraceEventType.CHECKPOINT_EVENTS_TYPE, "Checkpoint current versions"), // Request
    CHECKPOINT_COPY_DATA_ENDED(7012, TraceEventType.CHECKPOINT_EVENTS_TYPE, "Checkpoint copy finished"), // Request

    // Thread identifier events
    AP_THREAD_ID(Threads.AP.id, TraceEventType.THREAD_IDENTIFICATION, Threads.AP.description), //
    TD_THREAD_ID(Threads.TD.id, TraceEventType.THREAD_IDENTIFICATION, Threads.TD.description), //
    LOW_FILE_SYS_THREAD_ID(Threads.FSL.id, TraceEventType.THREAD_IDENTIFICATION, Threads.FSL.description), //
    HIGH_FILE_SYS_THREAD_ID(Threads.FSH.id, TraceEventType.THREAD_IDENTIFICATION, Threads.FSH.description), //
    TIMER_THREAD_ID(Threads.TIMER.id, TraceEventType.THREAD_IDENTIFICATION, Threads.TIMER.description), //
    WALLCLOCK_THREAD_ID(Threads.WC.id, TraceEventType.THREAD_IDENTIFICATION, Threads.WC.description), //
    EXECUTOR_THREAD_ID(Threads.EXEC.id, TraceEventType.THREAD_IDENTIFICATION, Threads.EXEC.description), //
    PYTHON_WORKER_THREAD_ID(Threads.PYTHON_WORKER.id, TraceEventType.THREAD_IDENTIFICATION,
        Threads.PYTHON_WORKER.description), //
    PYTHON_CACHE_THREAD_ID(Threads.CACHE.id, TraceEventType.THREAD_IDENTIFICATION, Threads.CACHE.description), //

    // Executor Thread events
    EXECUTOR_COUNTS(1, TraceEventType.EXECUTOR_COUNTS, "Executor counts"), // Executor start
    EXECUTOR_ACTIVE(1, TraceEventType.EXECUTOR_ACTIVITY, "Executor active"), // Executor start

    // Other
    READY_COUNT(1, TraceEventType.READY_COUNTS, "Ready queue count"); // Ready count


    private final int id;
    private final TraceEventType type;
    private final String signature;


    private TraceEvent(int id, TraceEventType type, String signature) {
        this.id = id;
        this.type = type;
        this.signature = signature;
        type.addEvent(this);
    }

    public int getId() {
        return this.id;
    }

    public TraceEventType getTraceEventType() {
        return this.type;
    }

    public TraceEventType getType() {
        return this.type;
    }

    public String getSignature() {
        return this.signature;
    }

}
