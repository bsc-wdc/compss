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


/**
 * Representation of a tracing event.
 */
public class CustomTraceEvent implements Event {

    private final int id;
    private final TraceEventType type;
    private final String signature;


    /**
     * Constructs and registers as part of a type a new Custom Event.
     *
     * @param type EventType of the event
     * @param id value of the event
     * @param signature label of the event
     */
    public CustomTraceEvent(TraceEventType type, int id, String signature) {
        this.id = id;
        this.type = type;
        this.signature = signature;
        type.addEvent(this);
    }

    @Override
    public int getId() {
        return this.id;
    }

    @Override
    public String getSignature() {
        return this.signature;
    }

    public TraceEventType getType() {
        return this.type;
    }

}
