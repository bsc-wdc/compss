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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StorageObject implements StubItf {

    // Logger: According to Loggers.STORAGE
    private static final Logger LOGGER = LogManager.getLogger("es.bsc.compss.Storage");

    private String host;
    private String id;


    /**
     * Constructor.
     */
    public StorageObject() {
        this.id = null;
    }

    /**
     * Constructor by alias.
     * 
     * @param alias Persistent object alias.
     */
    public StorageObject(String alias) {
        this.id = null;
    }

    @Override
    public String getID() {
        return this.id;
    }

    /**
     * Returns the associated host.
     * 
     * @return The associated host.
     */
    public String getHost() {
        return this.host;
    }

    /**
     * Sets a new Id.
     * 
     * @param id New Id.
     * @throws IOException When an internal error occurs.
     * @throws StorageException When an storage error occurs.
     */
    protected void setID(String id) throws IOException, StorageException {
        // TODO: Is this the intended behaviour?
        this.id = id;
    }

    /**
     * Sets a new associated host.
     * 
     * @param host New associated host.
     */
    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public void makePersistent(String id) throws StorageException {
        // The object is already persisted
        if (this.id != null) {
            return;
        }

        // There was no given identifier, lets compute a random one
        setLocalhostAsHost();
        if (id == null) {
            id = UUID.randomUUID().toString();
        }
        this.id = id;
        // Call the storage API
        StorageItf.makePersistent(this, id);
    }

    /**
     * Persist the object. The identifier will be a pseudo-randomly generated UUID.
     * 
     * @throws IOException When an internal error occurs.
     * @throws StorageException When an storage error occurs.
     */
    public void makePersistent() throws IOException, StorageException {
        this.makePersistent(null);
    }

    @Override
    public void deletePersistent() {
        // The object is not persisted, do nothing
        if (this.id == null) {
            return;
        }

        // Call the storage API
        StorageItf.removeById(this.id);
        // Set the id to null
        this.id = null;
    }

    /**
     * Updates the object in the database. That is, removes the current version and then adds the in-memory one with the
     * same identifier.
     */
    public void updatePersistent() {
        String pId = this.getID();
        this.deletePersistent();
        try {
            this.makePersistent(pId);
        } catch (StorageException se) {
            LOGGER.error(se);
        }
    }

    private void setLocalhostAsHost() {
        String hostname = null;
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            hostname = localHost.getCanonicalHostName();
        } catch (UnknownHostException une) {
            LOGGER.error(une);
            System.exit(1);
        }
        this.host = hostname;
    }

}
