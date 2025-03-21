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
package es.bsc.compss.nio.worker.components;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.nio.exceptions.NoSourcesException;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.FileOpsManager;
import es.bsc.compss.util.serializers.Serializer;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StorageException;
import storage.StorageItf;
import storage.StorageObject;


public class DataRegister {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER);

    private boolean inMemory;
    private Object value;
    private String storageId;

    private final Set<String> files;


    /**
     * Create a new DataRegister instance.
     */
    public DataRegister() {
        this.files = new HashSet<>();
    }

    /**
     * Returns whether the data is in memory or not.
     * 
     * @return {@code true} if the data is in memory, {@code false} otherwise.
     */
    public boolean isInMemory() {
        return this.inMemory;
    }

    /**
     * Returns the data value.
     * 
     * @return The data value.
     */
    public Object getValue() {
        return this.value;
    }

    /**
     * Returns the storage Id.
     * 
     * @return The storage Id.
     */
    public String getStorageId() {
        return storageId;
    }

    /**
     * Returns whether the value is cached locally or not.
     * 
     * @return {@code true} if the value is stored locally, {@code false} otherwise.
     */
    public boolean isLocal() {
        return inMemory || !files.isEmpty() || storageId != null;
    }

    /**
     * Returns the file locations of the data.
     * 
     * @return The file locations of the data.
     */
    public Set<String> getFileLocations() {
        return files;
    }

    /**
     * Sets a new storage Id for the current data.
     * 
     * @param storageId New storage Id.
     */
    public void setStorageId(String storageId) {
        this.storageId = storageId;
    }

    /**
     * Adds a new file location for the data.
     * 
     * @param file New file location.
     */
    public void addFileLocation(String file) {
        this.files.add(file);
    }

    /**
     * Removes the given file location.
     * 
     * @param path File location to remove.
     */
    public void removeFileLocation(String path) {
        files.remove(path);
    }

    /**
     * Sets a new object value.
     * 
     * @param value The new object value.
     */
    public void setValue(Object value) {
        if (value instanceof StorageObject) {
            this.storageId = ((StorageObject) value).getID();
        }
        this.value = value;
        this.inMemory = true;
    }

    /**
     * Removes the current memory value of the data.
     */
    public void removeValue() {
        this.inMemory = false;
        this.value = null;
    }

    /**
     * Loads the value of the data from the storage or one of the registered file locations.
     * 
     * @return The loaded value.
     * @throws IOException When deserializing the value.
     * @throws ClassNotFoundException When deserializing the value.
     * @throws NoSourcesException If no source was found to load the value.
     * @throws StorageException When loading the value from the storage backend.
     */
    public Object loadValue() throws IOException, ClassNotFoundException, NoSourcesException, StorageException {
        if (this.storageId != null) {
            // Try if parameter is in cache
            LOGGER.debug("   - Retrieving psco " + this.storageId + " from Storage");
            Object obj;
            // Get Object from its ID
            if (NIOTracer.isActivated()) {
                NIOTracer.emitEvent(TraceEvent.STORAGE_GETBYID);
            }
            try {
                obj = StorageItf.getByID(this.storageId);
                return obj;
            } catch (StorageException e) {
                LOGGER.error("Cannot getByID PSCO " + this.storageId, e);
                throw e;
            } finally {
                if (NIOTracer.isActivated()) {
                    NIOTracer.emitEventEnd(TraceEvent.STORAGE_GETBYID);
                }
            }
        }

        IOException io = null;
        ClassNotFoundException cnf = null;
        if (!this.inMemory) {
            for (String path : this.files) {
                try {
                    setValue(Serializer.deserialize(path));
                    return this.value;
                } catch (IOException ioe) {
                    io = ioe;
                } catch (ClassNotFoundException cnfe) {
                    cnf = cnfe;
                }
            }
            if (cnf != null) {
                throw cnf;
            } else if (io != null) {
                throw io;
            }
        } else {
            return this.value;
        }
        throw new NoSourcesException();
    }

    /**
     * Clones the current value.
     * 
     * @return A cloned version of the current value.
     * @throws StorageException When retrieving the value from the storage backend.
     * @throws ClassNotFoundException When deserializing the value.
     * @throws IOException When deserializing the value.
     * @throws NoSourcesException If no source was found to load the value.
     */
    public Object cloneValue() throws StorageException, ClassNotFoundException, IOException, NoSourcesException {
        if (this.storageId != null) {
            return StorageItf.getByID(this.storageId);
        }

        if (!this.inMemory) {
            IOException io = null;
            ClassNotFoundException cnf = null;
            for (String path : this.files) {
                try {
                    return Serializer.deserialize(path);
                } catch (IOException ioe) {
                    io = ioe;
                } catch (ClassNotFoundException cnfe) {
                    cnf = cnfe;
                }
            }
            if (cnf != null) {
                throw cnf;
            } else if (io != null) {
                throw io;
            }
        } else {
            return Serializer.deserialize(Serializer.serialize(this.value));
        }
        throw new NoSourcesException();
    }

    /**
     * Clears all the data information.
     */
    public void clear() {
        this.storageId = null;
        removeValue();
        for (String path : this.files) {
            FileOpsManager.deleteAsync(new File(path));
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\tMemory: ");
        if (this.inMemory) {
            sb.append("<").append(value).append(">");
        } else {
            sb.append("NOT PRESENT");
        }
        sb.append("\n");
        sb.append("\tStorage ID:").append(this.storageId).append("\n");

        sb.append("\tFiles:\n");
        for (String file : this.files) {
            sb.append("\t- ").append(file).append("\n");
        }
        return sb.toString();
    }

}
