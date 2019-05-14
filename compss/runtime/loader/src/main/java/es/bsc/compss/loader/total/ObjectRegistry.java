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
package es.bsc.compss.loader.total;

import es.bsc.compss.loader.LoaderAPI;
import es.bsc.compss.log.Loggers;

import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ObjectRegistry {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.LOADER);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static final String EMPTY = "EMPTY";

    // Api object used to invoke calls on the Integrated Toolkit
    private final LoaderAPI itApi;
    // Temporary directory where the files containing objects will be stored (same as the stream registry dir)
    private final String serialDir;

    // Map: hash code -> object
    private final Map<Integer, Object> appTaskObjects;
    private final Map<Integer, Object> internalObjects;


    /**
     * Creates a new ObjectRegistry instance associated to a given LoaderAPI {@code api}.
     * 
     * @param api LoaderAPI.
     */
    public ObjectRegistry(LoaderAPI api) {
        this.itApi = api;
        this.serialDir = api.getTempDir();
        this.appTaskObjects = new TreeMap<>();
        this.internalObjects = new TreeMap<>();

        this.itApi.setObjectRegistry(this);
    }

    /**
     * Registers a new Object access.
     * 
     * @param o Object.
     */
    public void newObjectAccess(Object o) {
        newObjectAccess(o, true);
    }

    /**
     * Registers a new access to the given object {@code o} in mode {@code isWriter}.
     * 
     * @param o Object.
     * @param isWriter {@code true} if its a writer access, {@code false} otherwise.
     */
    public void newObjectAccess(Object o, boolean isWriter) {
        if (o == null) {
            return;
        }
        Integer hashCode = getObjectHashCode(o);
        if (hashCode == null) {
            return; // Not a task parameter object
        }

        /*
         * The object has been accessed by a task before. Check with the API that the application has the last version,
         * blocking if necessary.
         */
        if (DEBUG) {
            LOGGER.debug("New access to object with hash code " + hashCode + ", for writing: " + isWriter);
        }

        // Get the updated version of the object
        Object oUpdated = this.itApi.getObject(o, hashCode, serialDir);
        if (oUpdated != null) {
            this.internalObjects.put(hashCode, oUpdated);
        }
    }

    /**
     * Registers a new Object parameter.
     * 
     * @param obj Object parameter.
     * @return Final hashcode of the object.
     */
    public int newObjectParameter(Object obj) {
        if (obj == null) {
            return Integer.MAX_VALUE;
        }

        int objHashCode = obj.hashCode();
        int finalHashCode = checkHashCode(obj, objHashCode);

        if (DEBUG) {
            LOGGER.debug("Object " + obj + " with hash code " + finalHashCode + " registered");
        }

        return finalHashCode;
    }

    private int checkHashCode(Object obj, int objHashCode) {
        Object objStored = this.appTaskObjects.get(objHashCode);
        while (objStored != obj) {
            if (objStored == null || objStored == EMPTY) {
                if (DEBUG) {
                    LOGGER.debug("Adding " + obj + " with hash code " + objHashCode + " to object registery");
                }
                this.appTaskObjects.put(objHashCode, obj);
                // Store it as an internal one too. Read-only objects will always use this same instance
                this.internalObjects.put(objHashCode, obj);
                break;
            }

            // Coincidence of two equal hash codes for different objects.
            // Increment the hash code and try again.
            ++objHashCode;
            objStored = this.appTaskObjects.get(objHashCode);
        }

        return objHashCode;
    }

    private Integer getObjectHashCode(Object o) {
        int hashCode = o.hashCode();
        Object oStored = this.appTaskObjects.get(hashCode);
        while (oStored != o) {
            if (oStored == null) {
                return null; // Not a task parameter object
            } else {
                oStored = this.appTaskObjects.get(++hashCode);
            }
        }
        return hashCode;
    }

    /**
     * Locally serializes the given object {@code o}.
     * 
     * @param o Object.
     */
    public void serializeLocally(Object o) {
        if (o == null) {
            return;
        }

        Integer hashCode = getObjectHashCode(o);
        if (hashCode == null) {
            return; // Not a task parameter object
        }

        /*
         * The object has been accessed by a task before. Delegate its serialization to the API. Serialize the internal
         * object
         */
        if (DEBUG) {
            LOGGER.debug("About to serialize locally object with hash code " + hashCode);
        }

        this.itApi.serializeObject(internalObjects.get(hashCode), hashCode, serialDir);
    }

    /**
     * Returns the internal object representing the given object {@code o}.
     * 
     * @param o Object.
     * @return Internal object representing the given object {@code o}.
     */
    public Object getInternalObject(Object o) {
        if (o == null) {
            return null;
        }
        Integer hashCode = getObjectHashCode(o);
        if (hashCode == null) {
            return null; // Not a task parameter object
        }

        Object internal = this.internalObjects.get(hashCode);

        /*
         * The object has been accessed by a task before. Return its internal (real) value
         */
        if (DEBUG) {
            LOGGER.debug("Returning internal object " + internal + " with hash code " + hashCode);
        }
        return internal;
    }

    /**
     * Deletes the given object {@code o}.
     * 
     * @param o Object.
     * @return {@code true} if the object has been removed, {@code false} otherwise.
     */
    public boolean delete(Object o) {
        if (o == null) {
            return false;
        }
        Integer hashCode = getObjectHashCode(o);
        if (hashCode == null) {
            return false; // Not a task parameter object
        }

        if (DEBUG) {
            LOGGER.debug("About to remove object with hash code " + hashCode + " from object registry.");
        }
        this.itApi.removeObject(o, hashCode);

        return deleteFromInternal(hashCode) && deleteFromApps(hashCode);
    }

    /**
     * Deletes the internal object represented by the given hashcode {@code hashcode}.
     * 
     * @param hashcode Internal's object hashcode.
     * @return {@code true} if the object has been deleted, {@code false} otherwise.
     */
    private boolean deleteFromInternal(int hashcode) {
        Object toDelete = this.internalObjects.get(hashcode);
        if (toDelete != null) {
            this.internalObjects.remove(hashcode);
            return true;
        }
        return false;
    }

    /**
     * Deletes the application object represented by the given hashcode {@code hashcode}.
     * 
     * @param hashcode Application's object hashcode.
     * @return {@code true} if the object has been deleted, {@code false} otherwise.
     */
    private boolean deleteFromApps(int hashcode) {
        Object toDelete = this.appTaskObjects.get(hashcode);
        if (toDelete != null) {
            this.appTaskObjects.put(hashcode, EMPTY);
            return true;
        }
        return false;
    }

}
