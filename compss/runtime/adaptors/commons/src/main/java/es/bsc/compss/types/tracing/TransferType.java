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

import es.bsc.wdc.tracing.EventType;
import es.bsc.wdc.tracing.Tracer;
import java.util.ArrayList;

public final class TransferType {

    public static final EventType TASK_TRANSFERS =
        Tracer.defineNewEventType(8_001_311, "Task Transfers Request", true, new ArrayList<>());

    public static final EventType DATA_TRANSFERS =
        Tracer.defineNewEventType(8_001_321, "Data Transfers", false, new ArrayList<>());


    private TransferType() {
        // Utility class
    }
}
