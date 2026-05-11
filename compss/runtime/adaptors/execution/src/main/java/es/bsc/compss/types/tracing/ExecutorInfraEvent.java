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

public enum ExecutorInfraEvent implements Event {

    EXECUTOR_COUNTS(1, ExecutorInfraType.EXECUTOR_COUNTS, "Executor counts"),
    EXECUTOR_ACTIVE(1, ExecutorInfraType.EXECUTOR_ACTIVITY, "Executor active");


    private final int id;
    private final EventType type;
    private final String signature;


    ExecutorInfraEvent(int id, EventType type, String signature) {
        this.id = id;
        this.type = type;
        this.signature = signature;

        if (type == ExecutorInfraType.EXECUTOR_COUNTS) {
            ExecutorInfraType.addExecutorCountsEvent(this);
        } else if (type == ExecutorInfraType.EXECUTOR_ACTIVITY) {
            ExecutorInfraType.addExecutorActivityEvent(this);
        }
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
        return this.type;
    }
}
