/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.tracing.binding;

import es.bsc.wdc.tracing.Event;
import es.bsc.wdc.tracing.EventType;
import es.bsc.wdc.tracing.Tracer;
import java.util.Arrays;
import java.util.List;

public enum InsideTaskEvent implements Event {

    // Python Events Inside Tasks
    CPU_BINDING_PYTHON(1, "CPU binding"), //
    GPU_BINDING_PYTHON(2, "GPU binding"), //
    SETUP_ENVIRONMENT_PYTHON(3, "Setup environment variables"), //
    GET_TASK_PARAMETERS(4, "Get parameters"), //
    IMPORT_USER_MODULE(5, "Import user module"), //
    EXECUTE_USER_CODE_PYTHON(6, "User code"), //
    DESERIALIZE_STRING_PYTHON(7, "Deserializing string"), //
    DESERIALIZE_OBJECT_PYTHON(8, "Deserializing object"), //
    SERIALIZE_OBJECT_PYTHON(9, "Serializing object"), //
    SERIALIZE_MPIENV_PYTHON(10, "Serializing object MPI env"), //
    BUILD_SUCCESS_MESSAGE(11, "Build success message"), //
    BUILD_COMPSS_EXCEPTION_MESSAGE(12, "Build COMPSs exception message"), //
    BUILD_EXCEPTION_MESSAGE(13, "Build exception message"), //
    CLEAN_ENVIRONMENT_PYTHON(14, "Clean environment"), //
    GET_BY_ID(15, "Get by ID persistent object"), //
    GET_ID(16, "Get object ID"), //
    MAKE_PERSISTENT(17, "Make persistent object"), //
    DELETE_PERSISTENT(18, "Delete persistent object"), //
    RETRIEVE_OBJECT_INTO_CACHE(19, "Get object from cache"), //
    INSERT_OBJECT_INTO_CACHE(20, "Put object in cache"), //
    REMOVE_OBJECT_FROM_CACHE(21, "Remove object from cache"), //
    WAIT_ON_PYTHON(22, "Wait on"), //
    WORKER_TASK_INSTANTIATION(23, "Task instantiation"), //
    CACHE_HIT(24, "Cache hit"), //
    CACHE_MISS(25, "Cache miss"), //
    CACHE_GPU_ACCESS(26, "Check GPU Access"), //
    CACHE_GPU_HIT(27, "Cache hit GPU"), //
    CACHE_GPU_MISS(28, "Cache miss GPU"), //
    RETRIEVE_OBJECT_FROM_GPU_CACHE(29, "Get object from GPU cache"), //
    INSERT_OBJECT_INTO_GPU_CACHE(30, "Put object in GPU cache"), //
    CLEANUP_TASK(31, "Cleanup Task"), //
    EXECUTOR_LOAD_EAR(32, "Import EAR"), //
    EXECUTOR_FINALIZE_EAR(33, "Finalize EAR"), //
    MANAGE_NEW_TYPES(34, "Manage new types"), //
    RELEASE_MEMORY(35, "Release memory"); //


    public static final EventType type;

    private final int id;
    private final String signature;

    static {
        List<Event> events = Arrays.asList(InsideTaskEvent.values());
        type = Tracer.defineNewEventType(9_000_100, "Binding events inside tasks", true, events);
    }


    InsideTaskEvent(int id, String signature) {
        this.id = id;
        this.signature = signature;
    }

    @Override
    public int getId() {
        return this.id;
    }

    @Override
    public String getSignature() {
        return this.signature;
    }

    @Override
    public EventType getType() {
        return type;
    }

}
