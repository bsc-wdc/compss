/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

/**
 * Information about a collection and its versions
 * 
 * @see DataInfo
 * @see es.bsc.compss.components.impl.DataInfoProvider registerCollectionAccess method
 */
public class CollectionInfo extends DataInfo {

    private String collectionId;


    /**
     * Default constructor
     * 
     * @see DataInfo empty constructor
     */
    public CollectionInfo() {
        super();
    }

    /**
     * Constructor with collection identifier
     * 
     * @param collectionId Collection identifier
     */
    public CollectionInfo(String collectionId) {
        super();
        this.collectionId = collectionId;
    }

    /**
     * Get the collectionId
     * 
     * @return String
     */
    public String getCollectionId() {
        return collectionId;
    }

    /**
     * Change the value of the collectionId
     * 
     * @param collectionId String
     */
    public void setCollectionId(String collectionId) {
        this.collectionId = collectionId;
    }
}
