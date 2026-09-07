/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.data;

import es.bsc.compss.data.tracing.StorageEvent;
import es.bsc.wdc.tracing.Tracer;
import java.util.List;
import storage.CallbackEvent;
import storage.CallbackHandler;
import storage.StorageException;
import storage.StorageItf;

public class StorageHandler {

    /**
     * Initializes the persistent storage.
     *
     * @param storageConf String
     * @throws StorageException Exception
     */
    public static void init(String storageConf) throws StorageException {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(StorageEvent.STORAGE_INIT);
        }
        try {
            StorageItf.init(storageConf);
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEventEnd(StorageEvent.STORAGE_INIT);
            }
        }
    }

    /**
     * Stops the persistent storage.
     *
     * @throws StorageException Exception
     */
    public static void finish() throws StorageException {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(StorageEvent.STORAGE_FINISH);
        }
        try {
            StorageItf.finish();
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEventEnd(StorageEvent.STORAGE_FINISH);
            }
        }
    }

    /**
     * Returns all the valid locations of a given id.
     *
     * @param pscoId String
     * @return description
     * @throws StorageException description
     */
    public static List<String> getLocations(String pscoId) throws StorageException {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(StorageEvent.STORAGE_GETLOCATIONS);
        }
        try {
            return StorageItf.getLocations(pscoId);
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEventEnd(StorageEvent.STORAGE_GETLOCATIONS);
            }
        }
    }

    /**
     * Creates a new replica of PSCO id @id in host @hostname.
     *
     * @param id String
     * @param hostName String
     * @throws StorageException description
     */
    public static void newReplica(String id, String hostName) throws StorageException {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(StorageEvent.STORAGE_NEWREPLICA);
        }
        try {
            StorageItf.newReplica(id, hostName);
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEventEnd(StorageEvent.STORAGE_NEWREPLICA);
            }
        }
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
        if (Tracer.isActivated()) {
            Tracer.emitEvent(StorageEvent.STORAGE_NEWVERSION);
        }
        try {
            return StorageItf.newVersion(id, preserveSource, hostName);
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEventEnd(StorageEvent.STORAGE_NEWVERSION);
            }
        }
    }

    /**
     * Returns the object with id @id This function retrieves the object from any location.
     *
     * @param id description
     * @return description
     * @throws StorageException description
     */
    public static Object getByID(String id) throws StorageException {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(StorageEvent.STORAGE_GETBYID);
        }
        try {
            return StorageItf.getByID(id);
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEventEnd(StorageEvent.STORAGE_GETBYID);
            }
        }
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
        if (Tracer.isActivated()) {
            Tracer.emitEvent(StorageEvent.STORAGE_EXECUTETASK);
        }
        try {
            return StorageItf.executeTask(id, descriptor, values, hostName, callback);
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEventEnd(StorageEvent.STORAGE_EXECUTETASK);
            }
        }
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
        if (Tracer.isActivated()) {
            Tracer.emitEvent(StorageEvent.STORAGE_CONSOLIDATE);
        }
        try {
            StorageItf.consolidateVersion(idFinal);
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEventEnd(StorageEvent.STORAGE_CONSOLIDATE);
            }
        }
    }
}
