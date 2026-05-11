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

import es.bsc.compss.util.Tracer;
import es.bsc.wdc.tracing.Event;
import es.bsc.wdc.tracing.EventType;
import java.util.Arrays;

public enum StorageEvent implements Event {

    STORAGE_GETBYID(38, "getByID"), STORAGE_NEWREPLICA(39, "newReplica"), STORAGE_NEWVERSION(40, "newVersion"),
    STORAGE_INVOKE(41, "invoke"), STORAGE_EXECUTETASK(42, "executeTask"), STORAGE_GETLOCATIONS(43, "getLocations"),
    STORAGE_CONSOLIDATE(44, "consolidateVersion");


    public static final EventType type;

    private final int id;
    private final String signature;

    static {
        type = Tracer.defineNewEventType(8_001_212, "Storage API", true, Arrays.asList(StorageEvent.values()));
    }


    StorageEvent(int id, String signature) {
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
