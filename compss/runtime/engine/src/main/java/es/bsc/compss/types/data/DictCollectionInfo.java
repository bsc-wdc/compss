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
 * Information about a dictionary collection and its versions.
 * 
 * @see DataInfo
 * @see es.bsc.compss.components.impl.DataInfoProvider registerCollectionAccess method
 */
public class DictCollectionInfo extends DataInfo {

    private String dictCollectionId;


    /**
     * Default constructor.
     * 
     * @param app application generating the data
     * @see DataInfo empty constructor.
     */
    public DictCollectionInfo(Application app) {
        super(app);
    }

    /**
     * Constructor with a String representing the dictionary collection Id.
     * 
     * @param app application generating the data
     * @param dictCollectionId String representing the dictionary collection Id.
     */
    public DictCollectionInfo(Application app, String dictCollectionId) {
        super(app);
        this.dictCollectionId = dictCollectionId;
    }

    /**
     * Get the dictCollectionId.
     * 
     * @return String representing the dictionary collection Id.
     */
    public String getCollectionId() {
        return this.dictCollectionId;
    }

    /**
     * Change the value of the dictCollectionId.
     * 
     * @param dictCollectionId String representing the collection Id.
     */
    public void setCollectionId(String dictCollectionId) {
        this.dictCollectionId = dictCollectionId;
    }

    @Override
    public int waitForDataReadyToDelete(Semaphore semWait) {
        // Nothing to wait for
        return 0;
    }
}
