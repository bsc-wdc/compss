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

import es.bsc.compss.util.Tracer;
import es.bsc.wdc.tracing.Event;
import es.bsc.wdc.tracing.EventType;
import java.util.Arrays;
import java.util.List;

public enum WorkerCacheEvent implements Event {

    // Python Events Inside Tasks
    CACHE_MSG_RECEIVE(1, "Receive message"), //
    CACHE_MSG_QUIT(2, "Quit"), //
    CACHE_MSG_END_PROFILING(3, "End profiling"), //
    CACHE_MSG_GET_EVENT(4, "Get from cache"), //
    CACHE_MSG_PUT_EVENT(5, "Put into cache"), //
    CACHE_MSG_REMOVE(6, "Remove from cache"), //
    CACHE_MSG_LOCK(7, "Lock cache entry"), //
    CACHE_MSG_UNLOCK(8, "Unlock cache entry"), //
    CACHE_MSG_IS_LOCKED(9, "Check if entry is locked"), //
    CACHE_MSG_IS_IN_CACHE(10, "Check if object is in cache") //
    ;


    public static final EventType type;

    private final int id;
    private final String signature;

    static {
        List<Event> events = Arrays.asList(WorkerCacheEvent.values());
        type = Tracer.defineNewEventType(9_000_201, "Binding events in worker cache", true, events);
    }


    WorkerCacheEvent(int id, String signature) {
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
