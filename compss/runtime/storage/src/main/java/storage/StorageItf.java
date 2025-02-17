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

import java.util.List;


/**
 * Representation of the Storage ITF. TODO: complete javadoc
 */
public final class StorageItf {

    private static final String STORAGE_NOT_FOUND_MESSAGE = "You are trying to start a run with "
        + "persistent object storage but any back-end client is loaded in the classpath.";


    /**
     * Constructor.
     */
    public StorageItf() {

    }

    /**
     * Initializes the persistent storage.
     *
     * @param storageConf String
     * @throws StorageException Exception
     */
    public static void init(String storageConf) throws StorageException {
        throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
    }

    /**
     * Stops the persistent storage.
     *
     * @throws StorageException Exception
     */
    public static void finish() throws StorageException {
        throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
    }

    /**
     * Returns all the valid locations of a given id.
     *
     * @param pscoId String
     * @return description
     * @throws StorageException description
     */
    public static List<String> getLocations(String pscoId) throws StorageException {
        throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
    }

    /**
     * Creates a new replica of PSCO id @id in host @hostname.
     *
     * @param id String
     * @param hostName String
     * @throws StorageException description
     */
    public static void newReplica(String id, String hostName) throws StorageException {
        throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
    }

    /**
     * Create a new version of the PSCO id @id in the host @hostname. The flag preserveSource indicates whether the PSCO
     * with ID @id must be preserved or can be removed. The method returns the id of the new version.
     *
     * @param id description
     * @param preserveSource description
     * @param hostName description
     * @return description
     * @throws StorageException description
     */
    public static String newVersion(String id, boolean preserveSource, String hostName) throws StorageException {
        throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
    }

    /**
     * Returns the object with id @id This function retrieves the object from any location.
     *
     * @param id description
     * @return description
     * @throws StorageException description
     */
    public static Object getByID(String id) throws StorageException {
        throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
    }

    /**
     * Executes the task into persistent storage.
     *
     * @param id description
     * @param descriptor description
     * @param values description
     * @param hostName description
     * @param callback description
     * @return description
     * @throws StorageException description
     */
    public static String executeTask(String id, String descriptor, Object[] values, String hostName,
        CallbackHandler callback) throws StorageException {
        throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
    }

    /**
     * Retrieves the result of persistent storage execution.
     *
     * @param event CallbackEvent
     * @return
     */
    public static Object getResult(CallbackEvent event) throws StorageException {
        // Nothing to do
        return null;
    }

    /**
     * Consolidates all intermediate versions to the final id.
     *
     * @param idFinal String
     * @throws StorageException exception
     */
    public static void consolidateVersion(String idFinal) throws StorageException {
        throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
    }

}
