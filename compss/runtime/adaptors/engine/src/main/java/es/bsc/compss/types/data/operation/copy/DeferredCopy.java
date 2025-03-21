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
package es.bsc.compss.types.data.operation.copy;

import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;


public class DeferredCopy extends Copy {

    /**
     * Constructs a new DeferredCopy.
     *
     * @param srcData source logical data
     * @param prefSrc preferred source data location
     * @param prefTgt preferred target data location
     * @param tgtData target logical data
     * @param reason Transfer reason
     * @param listener listener to notify events
     */
    public DeferredCopy(LogicalData srcData, DataLocation prefSrc, DataLocation prefTgt, LogicalData tgtData,
        Transferable reason, EventListener listener) {

        super(srcData, prefSrc, prefTgt, tgtData, reason, listener);

        if (DEBUG) {
            LOGGER.debug("Created Deferred Copy " + this.getName() + " (id: " + this.getId() + ")");
        }
    }

    @Override
    public void perform() {
        // No need to do anything. Will be performed later on.
    }

}
