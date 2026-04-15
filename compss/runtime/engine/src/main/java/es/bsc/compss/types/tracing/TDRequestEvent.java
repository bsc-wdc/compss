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

public enum TDRequestEvent implements Event {

    // Task Dispatcher Events
    ACTION_UPDATE(45, "Task Dispatcher: Action update"), //
    CE_REGISTRATION(46, "Task Dispatcher: CE registration"), //
    EXECUTE_TASKS(47, "Task Dispatcher: Execute tasks"), //
    GET_CURRENT_SCHEDULE(48, "Task Dispatcher: Get current schedule"), //
    PRINT_CURRENT_GRAPH(49, "Task Dispatcher: Print current graph"), //
    MONITORING_DATA(50, "Task Dispatcher: Monitoring data"), //
    TD_SHUTDOWN(51, "Task Dispatcher: Shutdown"), //
    UPDATE_CEI_LOCAL(52, "Task Dispatcher: Update CEI local"), //
    WORKER_UPDATE_REQUEST(53, "Task Dispatcher: Worker update request"), //
    CANCEL_TASKS(57, "Task Dispatcher: Cancel tasks"), //
    WORKER_RESTART_REQUEST(59, "Task Dispatcher: Re-starting worker"); //


    public static final EventType type;

    private final int id;
    private final String signature;

    static {
        type = Tracer.defineNewEventType(8_003_001, "TD Request", true, Arrays.asList(TDRequestEvent.values()));
    }


    TDRequestEvent(int id, String signature) {
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
