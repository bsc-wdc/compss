/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.util.Serializer;
import es.bsc.compss.util.Tracer;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
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
    private final List<String> files = new LinkedList<>();
    private final List<BindingLocation> bindingLocations = new LinkedList<>();


    public DataRegister() {
    }

    public void setValue(Object value) {
        if (value instanceof StorageObject) {
            this.storageId = ((StorageObject) value).getID();
        }
        this.value = value;
        inMemory = true;
    }

    public void removeValue() {
        this.inMemory = false;
        this.value = null;
    }

    public boolean isInMemory() {
        return inMemory;
    }

    public Object getValue() {
        return value;
    }

    public Object loadValue() throws IOException, ClassNotFoundException, NoSourcesException, StorageException {
        if (storageId != null) {
            // Try if parameter is in cache
            LOGGER.debug("   - Retrieving psco " + storageId + " from Storage");

            Object obj;
            // Get Object from its ID
            if (Tracer.isActivated()) {
                Tracer.emitEvent(Tracer.Event.STORAGE_GETBYID.getId(), Tracer.Event.STORAGE_GETBYID.getType());
            }
            try {
                obj = StorageItf.getByID(storageId);
                return obj;
            } catch (StorageException e) {
                LOGGER.error("Cannot getByID PSCO " + storageId, e);
                throw e;
            } finally {
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_GETBYID.getType());
                }
            }
        }

        IOException io = null;
        ClassNotFoundException cnf = null;
        if (!inMemory) {
            for (String path : files) {
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

    public Object cloneValue() throws StorageException, ClassNotFoundException, IOException, NoSourcesException {
        if (storageId != null) {
            return StorageItf.getByID(storageId);
        }

        if (!inMemory) {
            IOException io = null;
            ClassNotFoundException cnf = null;
            for (String path : files) {
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

    public void setStorageId(String storageId) {
        this.storageId = storageId;
    }

    public String getStorageId() {
        return storageId;
    }

    public void addFileLocation(String file) {
        this.files.add(file);
    }

    public void removeFileLocation(String path) {
        files.remove(path);
    }

    public List<String> getFileLocations() {
        return files;
    }

    public void addBindingLocation(BindingLocation loc) {
        bindingLocations.add(loc);
    }

    public boolean isLocal() {
        return inMemory || !files.isEmpty() || storageId != null || !bindingLocations.isEmpty();
    }

    public void clear() {
        storageId = null;
        removeValue();
        for (String path : files) {
            try {
                File f = new File(path);
                f.delete();
            } catch (Exception e) {
                // File was deleted beforehand. Do nothing
            }
        }
    }


    public static interface BindingLocation {

    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\t Memory:");
        if (inMemory) {
            sb.append("<").append(value).append(">");
        } else {
            sb.append(" NOT PRESENT");
        }
        sb.append("\n");
        sb.append("\t Bindings:\n");
        for (BindingLocation bLoc : this.bindingLocations) {
            sb.append("\t-").append(bLoc.toString()).append("\n");
        }
        sb.append("\t Storage ID:").append(this.storageId).append("\n");

        sb.append("\t Files:\n");
        for (String file : this.files) {
            sb.append("\t-").append(file).append("\n");
        }
        return sb.toString();
    }


    public static class NoSourcesException extends Exception {

        private static final long serialVersionUID = 1L;


        public NoSourcesException() {
            super("No sources form where to load the value");
        }

    }
}
