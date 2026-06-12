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

public enum MasterEvent implements Event {

    // Python Master Events
    PYTHON_START_RUNTIME(1, "Start runtime"), //
    PYTHON_STOP_RUNTIME(2, "Stop runtime"), //
    PYTHON_APPLICATION_RUNNING(3, "Application running"), //
    // 4 is empty
    PYTHON_MASTER_INIT_STORAGE(5, "Start storage"), //
    PYTHON_MASTER_STOP_STORAGE(6, "Stop storage"), //
    PYTHON_ACCESSED_FILE(7, "Accessed file"), //
    PYTHON_OPEN_FILE(8, "Open file"), //
    PYTHON_DELETE_FILE(9, "Delete file"), //
    PYTHON_GET_FILE(10, "Get file"), //
    PYTHON_GET_DIRECTORY(11, "Get directory"), //
    PYTHON_DELETE_OBJECT(12, "Delete object"), //
    PYTHON_BARRIER(13, "Barrier"), //
    PYTHON_BARRIER_GROUP(14, "Barrier group"), //
    PYTHON_OPEN_TASK_GROUP(15, "Open task group"), //
    PYTHON_CLOSE_TASK_GROUP(16, "Close task group"), //
    PYTHON_GET_LOG_PATH(17, "Get log path"), //
    PYTHON_GET_TMP_PATH(18, "Get tmp path (master working dir)"), //
    PYTHON_REGISTER_CORE_ELEMENT(22, "Register Core Element"), //
    PYTHON_WAIT_ON(23, "Wait on"), //
    PYTHON_PROCESS_TASK(24, "Call to process task"), //
    PYTHON_WALL_CLOCK_LIMIT(25, "Wall clock limit"), //
    PYTHON_SNAPSHOT(26, "Snapshot"), //
    PYTHON_CANCEL_TASK_GROUP(27, "Cancel task group"), //

    // Internal events
    PYTHON_TASK_INSTANTIATION(100, "Task instantiation"), //
    PYTHON_INSPECT_FUNCTION_ARGUMENTS(101, "Inspect function arguments"), //
    PYTHON_INSPECT_CONSTRAINTS(102, "Inspect constraints"), //
    PYTHON_GET_FUNCTION_INFORMATION(103, "Get function information"), //
    PYTHON_GET_FUNCTION_SIGNATURE(104, "Check function signature"), //
    PYTHON_CHECK_INTERACTIVE(105, "Check interactive"), //
    PYTHON_EXTRACT_CORE_ELEMENT(106, "Extract core element"), //
    PYTHON_PREPARE_CORE_ELEMENT(107, "Prepare Core Element"), //
    PYTHON_UPDATE_CORE_ELEMENT(108, "Update Core Element"), //
    PYTHON_GET_UPPER_DECORATORS_KWARGS(109, "Get upper decorators kwargs"), //
    PYTHON_PROCESS_OTHER_ARGUMENTS(110, "Process task hints"), //
    PYTHON_PROCESS_PARAMETERS(111, "Process function parameters"), //
    PYTHON_PROCESS_RETURN(112, "Process return"), //
    PYTHON_BUILD_RETURN_OBJECTS(113, "Build return objects"), //
    PYTHON_SERIALIZE_OBJECTS(114, "Serialize objects"), //
    PYTHON_BUILD_COMPSS_TYPES_DIRECTIONS(115, "Build COMPSs types and directions"), //
    PYTHON_PROCESS_TASK_BINDING(116, "Process task binding"), //
    PYTHON_ATTRIBUTES_CLEANUP(117, "Cleanup");


    public static final EventType type;

    private final int id;
    private final String signature;

    static {
        List<Event> events = Arrays.asList(MasterEvent.values());
        type = Tracer.defineNewEventType(9_000_300, "Binding master events", true, events);
    }


    MasterEvent(int id, String signature) {
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
