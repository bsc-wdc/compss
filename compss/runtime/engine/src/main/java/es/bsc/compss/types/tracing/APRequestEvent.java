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
package es.bsc.compss.types.tracing;

import es.bsc.wdc.tracing.Event;
import es.bsc.wdc.tracing.EventType;
import es.bsc.wdc.tracing.Tracer;
import java.util.Arrays;

public enum APRequestEvent implements Event {

    // Access Processor Events which are not in the API
    WAIT_FOR_ALL_TASKS(6, "Access Processor: Barrier"), //
    DEBUG(17, "Access Processor: Debug"), //
    ANALYSE_TASK(18, "Access Processor: Analyse task"), //
    UPDATE_GRAPH(19, "Access Processor: Update graph"), //
    WAIT_FOR_DATA(20, "Access Processor: Wait for data"), //
    END_OF_APP(21, "Access Processor: End of app"), //
    ALREADY_ACCESSED(22, "Access Processor: Already accessed"), //
    REGISTER_DATA_ACCESS(23, "Access Processor: Register data access"), //
    TRANSFER_OPEN_FILE(24, "Access Processor: Transfer open file"), //
    TRANSFER_RAW_FILE(25, "Access Processor: Transfer raw file"), //
    TRANSFER_OBJECT(26, "Access Processor: Transfer object"), //
    NEW_VERSION_SAME_VALUE(27, "Access Processor: New version same value"), //
    BLOCK_AND_GET_RESULT_FILES(31, "Access Processor: Block and get result files"), //
    UNBLOCK_RESULT_FILES(32, "Access Processor: Unblock result files"), //
    SHUTDOWN(33, "Access Processor: Shutdown"), //
    GRAPHSTATE(34, "Access Processor: Graphstate"), //
    TASKSTATE(35, "Access Processor: Taskstate"), //
    DELETE_DATA(36, "Access Processor: Delete Data"), //
    FINISH_DATA_ACCESS(37, "Access Processor: Finish access to file"), //
    REGISTER_REMOTE_DATA(38, "Access Processor: Register remote data access"), //
    CANCEL_TASK_GROUP(41, "Access Processor: Cancel task group"), //
    REMOVE_APP_DATA(42, "Access Processor: Remove application data"), //
    CANCEL_ALL_TASKS(56, "Access Processor: Cancel all tasks"), //
    CP_SHUTDOWN_NOTIFICATION(81, "Access Processor: Checkpointed shutdown notification"), //
    AP_SNAPSHOT(82, "Access Processor: Snapshot"), //
    AP_CHECKPOINT_REQUEST(83, "Access Processor: CheckpointManager Request"), //
    AP_GET_LAST_DATA(91, "Obtaining last data"); //


    public static final EventType type;

    private final int id;
    private final String signature;

    static {
        type = Tracer.defineNewEventType(8_003_001, "AP Request", true, Arrays.asList(APRequestEvent.values()));
    }


    APRequestEvent(int id, String signature) {
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
