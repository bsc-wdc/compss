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
package es.bsc.compss.checkpoint.types;

import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;


public class CheckpointCopy {

    private final LogicalData data;
    private final DataLocation targetLocation;
    private final EventListener listener;


    /**
     * Constructs a new pending checkpointing copy.
     * 
     * @param data data being copied
     * @param targetLocation location where the copy will be left
     * @param dataListener listener to notify changes
     */
    public CheckpointCopy(LogicalData data, DataLocation targetLocation, EventListener dataListener) {
        this.data = data;
        this.targetLocation = targetLocation;
        this.listener = dataListener;
    }

    public LogicalData getData() {
        return data;
    }

    public DataLocation getTargetLocation() {
        return targetLocation;
    }

    public EventListener getListener() {
        return listener;
    }

}
