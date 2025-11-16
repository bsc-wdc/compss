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

import es.bsc.compss.util.Tracer;
import es.bsc.wdc.tracing.Event;
import es.bsc.wdc.tracing.EventType;

import java.util.Arrays;
import java.util.List;


public enum APIEvent implements Event {

    STATIC_IT(1, "Loading Runtime"), //
    START(2, "Start"), //
    STOP(3, "Stop"), //
    TASK(4, "Execute Task"), //
    NO_MORE_TASKS(5, "Waiting for tasks end"), //
    WAIT_FOR_ALL_TASKS(6, "Barrier"), //
    OPEN_FILE(7, "Waiting for open file"), //
    OPEN_DIRECTORY(57, "Waiting for open directory"), //
    GET_FILE(8, "Waiting for get file"), //
    GET_OBJECT(9, "Waiting for get object"), //
    GET_BINDING_OBJECT(10, "Waiting for get binding object"), //
    GET_DIRECTORY(58, "Waiting for get Directory"), //
    DELETE(12, "Delete File"), //
    WAIT_FOR_CONCURRENT(59, "Wait on concurrent"), //
    SNAPSHOT_API(80, "Snapshot request");


    public static final EventType type;

    private final int id;
    private final String signature;

    static {
        type = Tracer.defineNewEventType(8_001_001, "API", true, Arrays.asList(APIEvent.values()));
    }


    APIEvent(int id, String signature) {
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
