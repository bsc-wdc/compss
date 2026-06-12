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

public enum CheckpointEvent implements Event {

    CHECKPOINT_SHUTDOWN(7001, "CheckpointManager shutdown"),

    CHECKPOINT_NEW_TASK(7002, "Checkpoint New task"), // New task
    CHECKPOINT_END_TASK(7003, "Checkpoint end task"), // End task
    CHECKPOINT_MAIN_ACCESS(7004, "Checkpoint main data access"), // Main access
    CHECKPOINT_DELETE_DATA(7005, "Checkpoint deletes data"), // delete data
    CHECKPOINT_SNAPSHOT(7006, "Checkpoint snapshot"), // Snapshot

    SAVE_LAST_DATA_VERSIONS(7011, "Checkpoint current versions"), // Request
    CHECKPOINT_COPY_DATA_ENDED(7012, "Checkpoint copy finished"), // Request
    ;


    public static final EventType type;

    private final int id;
    private final String signature;

    static {
        type = Tracer.defineNewEventType(8_001_100, "Checkpoint", true, Arrays.asList(CheckpointEvent.values()));
    }


    CheckpointEvent(int id, String signature) {
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
