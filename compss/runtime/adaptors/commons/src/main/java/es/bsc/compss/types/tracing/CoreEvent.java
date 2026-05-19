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
import es.bsc.wdc.tracing.ExtensibleEventType;
import java.util.LinkedList;

public class CoreEvent implements Event {

    private static final int TASK_FUNC_ID = 8_001_131;

    private final int id;
    private final String desc;
    public static final ExtensibleEventType TYPE =
        Tracer.defineNewExtensibleEventType(TASK_FUNC_ID, "Task", true, new LinkedList<>());


    private CoreEvent(int id, String desc) {
        this.id = id;
        this.desc = desc;
    }

    /**
     * Adds a new event to the list of core events.
     *
     * @param id id of the core
     * @param signature signature of the core event
     */
    public static void addCore(int id, String signature) {
        TYPE.addEvent(new CoreEvent(id, signature));
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getSignature() {
        return desc;
    }

    @Override
    public EventType getType() {
        return TYPE;
    }
}
