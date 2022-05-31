/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.data;

import es.bsc.compss.types.Application;

import java.util.concurrent.Semaphore;


/**
 * Information about a collection and its versions.
 * 
 * @see DataInfo
 * @see es.bsc.compss.components.impl.DataInfoProvider registerCollectionAccess method
 */
public class CollectionInfo extends DataInfo {

    private String collectionId;


    /**
     * Default constructor.
     * 
     * @param app application generating the data
     * @see DataInfo empty constructor.
     */
    public CollectionInfo(Application app) {
        super(app);
    }

    /**
     * Constructor with a String representing the collection Id.
     * 
     * @param app application generating the data
     * @param collectionId String representing the collection Id.
     */
    public CollectionInfo(Application app, String collectionId) {
        super(app);
        this.collectionId = collectionId;
    }

    /**
     * Get the collectionId.
     * 
     * @return String representing the collection Id.
     */
    public String getCollectionId() {
        return collectionId;
    }

    /**
     * Change the value of the collectionId.
     * 
     * @param collectionId String representing the collection Id.
     */
    public void setCollectionId(String collectionId) {
        this.collectionId = collectionId;
    }

    @Override
    public int waitForDataReadyToDelete(Semaphore semWait) {
        // Nothing to wait for
        return 0;
    }
}
