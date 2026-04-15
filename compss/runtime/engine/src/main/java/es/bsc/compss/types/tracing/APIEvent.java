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


public enum APIEvent implements Event {

    // RUNTIME MANAGEMENT
    STATIC_IT(1, "Loading Runtime"), //
    START(2, "Start"), //
    STOP(3, "Stop"), //

    // RESOURCE MANAGEMENT
    GET_RESOURCES(100, "Getting Resources"), //
    REQUEST_RESOURCES(101, "Requesting resources"), //
    FREE_RESOURCES(102, "Freeing resources"), //

    // APPLICATION MANAGEMENT
    REGISTER_APP(81, "Register workflow"), //
    DEREGISTER_APP(82, " Deregister workflow"), //
    REGISTER_CE(83, "Register CE"), //

    // TASK MANAGEMENT
    TASK(4, "Execute Task"), //
    CANCEL_APP(89, "Cancel app tasks"), //
    OPEN_GROUP(90, "Open Task Group"), //
    CLOSE_GROUP(91, "Close task group"), //
    CANCEL_GROUP(92, "Cancel task group"), //
    WAIT_FOR_GROUP_TASKS(93, "Group Barrier"), //
    WAIT_FOR_ALL_TASKS(6, "Barrier"), //
    NO_MORE_TASKS(5, "Waiting for tasks end"), //

    // DATA MANAGEMENT
    OPEN_FILE(7, "Waiting for open file"), //
    OPEN_DIRECTORY(57, "Waiting for open directory"), //
    GET_FILE(8, "Waiting for get file"), //
    GET_OBJECT(9, "Waiting for get object"), //
    GET_BINDING_OBJECT(10, "Waiting for get binding object"), //
    GET_DIRECTORY(58, "Waiting for get Directory"), //
    DELETE_FILE(12, "Delete File"), //
    DELETE_OBJECT(13, "Delete Object"), //
    DELETE_BIND_OBJECT(14, "Delete  Binding Object"), //
    WAIT_FOR_CONCURRENT(59, "Wait on concurrent"), //
    REGISTER_DATA(85, "Register data"), //
    BIND_DATA_TO_VERSION(86, "Bind data to version"), //
    CHECK_FILE(87, "Check file is accessed"), //
    CLOSE_FILE(88, "Close files"),

    // CHECKPOINTING
    SNAPSHOT_API(80, "Snapshot request"); //


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
