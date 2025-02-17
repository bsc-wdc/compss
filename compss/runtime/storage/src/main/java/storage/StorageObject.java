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
package storage;

/**
 * Abstract implementation of an Storage Object. TODO complete javadoc
 */
public class StorageObject implements StubItf {

    /**
     * Constructor.
     */
    public StorageObject() {

    }

    /**
     * Constructor by alias.
     *
     * @param alias description
     */
    public StorageObject(String alias) {

    }

    /**
     * Returns the persistent object ID.
     *
     * @return
     */
    @Override
    public String getID() {
        throw new UnsupportedOperationException();
    }

    /**
     * Persist the object.
     *
     * @param id description
     */
    @Override
    public void makePersistent(String id) {
        throw new UnsupportedOperationException();
    }

    /**
     * Deletes the persistent object occurrences.
     */
    @Override
    public void deletePersistent() {
        throw new UnsupportedOperationException();
    }

}
