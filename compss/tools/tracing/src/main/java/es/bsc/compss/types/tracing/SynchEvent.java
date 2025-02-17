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

public class SynchEvent {

    private final String resourceId;
    private final String workerId;

    private final Long timestamp;

    // can be a timestamp and the number of cores of the worker (e.g. sync event atend).
    private final Long value;


    /**
     * Constructs a new Synch event.
     * 
     * @param resourceID resource emitting the synch event
     * @param workerId worker value assigned to the host
     * @param timestamp timestamp of the event emission
     * @param value synch event value
     */
    public SynchEvent(String resourceID, String workerId, Long timestamp, long value) {
        this.resourceId = resourceID;
        this.workerId = workerId;
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getResourceId() {
        return this.resourceId;
    }

    public String getWorkerId() {
        return workerId;
    }

    public Long getTimestamp() {
        return this.timestamp;
    }

    public long getValue() {
        return this.value;
    }
}
