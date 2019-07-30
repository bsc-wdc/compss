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
package storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StorageObject implements StubItf {

    // Logger: According to Loggers.STORAGE
    private static final Logger LOGGER = LogManager.getLogger("es.bsc.compss.Storage");

    private String id = null;


    /**
     * Constructor
     */
    public StorageObject() {
    }

    /**
     * Constructor by alias
     * 
     * @param alias
     */
    public StorageObject(String alias) {
    }

    /**
     * Returns the persistent object ID
     * 
     * @return
     */
    @Override
    public String getID() {
        return this.id;
    }

    /**
     * Persist the object
     * 
     * @param id
     */
    @Override
    public void makePersistent(String id) {
        try {
            this.id = id;
            StorageItf.makePersistent(this, id);
        } catch (StorageException e) {
            LOGGER.error("Exception serializing object", e);
        }
    }

    /**
     * Deletes the persistent object occurrences
     */
    @Override
    public void deletePersistent() {
        StorageItf.removeById(this.id);
        this.id = null;
    }

    /**
     * Sets the ID (only used by this implementation)
     */
    protected void setID(String id) {
        this.id = id;
    }

}
