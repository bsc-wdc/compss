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


/**
 * Representation of a Storage Copy.
 */
public class StorageCopy extends Copy {

    private final boolean preserveSourceData;


    /**
     * Create a new Storage Copy.
     *
     * @param srcData Source logical data
     * @param prefSrc preferred source location
     * @param prefTgt preferred target location
     * @param tgtData Target logical data
     * @param reason Transferable action which requested the copy
     * @param listener Listener to notify events
     */
    public StorageCopy(LogicalData srcData, DataLocation prefSrc, DataLocation prefTgt, LogicalData tgtData,
        Transferable reason, EventListener listener) {

        super(srcData, prefSrc, prefTgt, tgtData, reason, listener);
        this.preserveSourceData = reason.isSourcePreserved();
        if (DEBUG) {
            LOGGER.debug("Created StorageCopy " + this.getName() + " (id: " + this.getId() + ")");
        }
    }

    /**
     * Returns whether the source data must be preserved or not.
     *
     * @return {@literal true}, if source data must be preserved
     */
    public boolean mustPreserveSourceData() {
        return this.preserveSourceData;
    }

    @Override
    public void perform() {
        // No need to do anything. Will be performed later on.
    }
}
