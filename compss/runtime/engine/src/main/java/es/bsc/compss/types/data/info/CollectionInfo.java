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
package es.bsc.compss.types.data.info;

import es.bsc.compss.types.data.params.CollectionData;
import es.bsc.compss.types.data.params.DataOwner;

import java.util.concurrent.Semaphore;


/**
 * Information about a collection and its versions.
 *
 * @see DataInfo
 */
public class CollectionInfo extends StandardDataInfo<CollectionData> {

    /**
     * Creates a new CollectionInfo instance for the given collection.
     *
     * @param data description of the collection related to the info
     * @param owner owner of the CollectionInfo being created
     */
    public CollectionInfo(CollectionData data, DataOwner owner) {
        super(data, owner);
        owner.registerCollectionData(data.getCollectionId(), this);
    }

    /**
     * Get the collectionId.
     *
     * @return String representing the collection Id.
     */
    public String getCollectionId() {
        return this.getParams().getCollectionId();
    }

    @Override
    public void waitForDataReadyToDelete(Semaphore sem) {
        // Nothing to wait for
    }
}
