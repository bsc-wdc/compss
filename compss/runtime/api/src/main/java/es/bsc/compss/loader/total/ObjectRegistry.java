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
package es.bsc.compss.loader.total;

import es.bsc.compss.loader.LoaderAPI;
import es.bsc.compss.log.Loggers;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ObjectRegistry {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.LOADER);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static final String EMPTY = "EMPTY";


    private static final class AppEntry {

        Long appId;
        Object object;


        public AppEntry(Long appId, Object object) {
            this.appId = appId;
            this.object = object;
        }

    }


    // Api object used to invoke calls on the Integrated Toolkit
    private final LoaderAPI itApi;
    // Temporary directory where the files containing objects will be stored (same as the stream registry dir)
    private final String serialDir;

    // Map: hash code -> object
    private final Map<Integer, AppEntry> appObjects;
    private final Map<Integer, Object> internalObjects;


    /**
     * Creates a new ObjectRegistry instance associated to a given LoaderAPI {@code api}.
     *
     * @param api LoaderAPI.
     */
    public ObjectRegistry(LoaderAPI api) {
        this.itApi = api;
        this.serialDir = api.getTempDir();
        this.appObjects = new TreeMap<>();
        this.internalObjects = new TreeMap<>();

        this.itApi.setObjectRegistry(this);
    }

    /**
     * Registers a new Object access.
     *
     * @param appId Application Id.
     * @param o Object.
     */
    public void newObjectAccess(Long appId, Object o) {
        newObjectAccess(appId, o, true);
    }

    /**
     * Registers a new access to the given object {@code o} in mode {@code isWriter}.
     *
     * @param appId Application Id.
     * @param o Object.
     * @param isWriter {@code true} if its a writer access, {@code false} otherwise.
     */
    public void newObjectAccess(Long appId, Object o, boolean isWriter) {
        if (o == null) {
            return;
        }
        Integer hashCode = getObjectHashCode(appId, o);
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
        Object oUpdated = this.itApi.getObject(appId, o, hashCode, serialDir);
        if (oUpdated != null) {
            this.internalObjects.put(hashCode, oUpdated);
        }
    }

    /**
     * Registers a new Object parameter.
     *
     * @param appId Application Id.
     * @param obj Object parameter.
     * @return Final hashcode of the object.
     */
    public int newObjectParameter(Long appId, Object obj) {
        if (obj == null) {
            return Integer.MAX_VALUE;
        }
        int finalHashCode = assignHashCode(appId, obj);

        if (DEBUG) {
            LOGGER.debug("Object " + obj + " with hash code " + finalHashCode + " registered");
        }

        return finalHashCode;
    }

    private int assignHashCode(Long appId, Object obj) {
        int objHashCode = obj.hashCode();
        AppEntry objEntry = this.appObjects.get(objHashCode);
        while (objEntry != null && objEntry.object != EMPTY
            && (objEntry.object != obj || !Objects.equals(objEntry.appId, appId))) {

            // Coincidence of two equal hash codes for different objects or for the same object on different apps.
            // Increment the hash code and try again.
            ++objHashCode;
            objEntry = this.appObjects.get(objHashCode);
        }

        if (objEntry == null || objEntry.object == EMPTY) {
            if (DEBUG) {
                LOGGER.debug("Adding " + obj + " with hash code " + objHashCode + " to object registery");
            }
            AppEntry re = new AppEntry(appId, obj);
            this.appObjects.put(objHashCode, re);
            // Store it as an internal one too. Read-only objects will always use this same instance
            this.internalObjects.put(objHashCode, obj);
        }
        return objHashCode;
    }

    private Integer getObjectHashCode(Long appId, Object obj) {
        int hashCode = obj.hashCode();
        AppEntry oEntry = this.appObjects.get(hashCode);
        while (oEntry != null) {
            if (oEntry.object == obj && Objects.equals(oEntry.appId, appId)) {
                return hashCode;
            }
            // Coincidence of two equal hash codes for different objects or for the same object on different apps.
            // Increment the hash code and try again.
            oEntry = this.appObjects.get(++hashCode);
        }
        // Not a task parameter object
        return null;
    }

    /**
     * Locally serializes the given object {@code o}.
     *
     * @param appId Application Id.
     * @param o Object.
     */
    public void serializeLocally(Long appId, Object o) {
        if (o == null) {
            return;
        }
        Integer hashCode = getObjectHashCode(appId, o);
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
     * Links a data with the last known version for an object.
     * 
     * @param appId Application Id.
     * @param o Object
     * @param dataId dataId to link
     * @return {@literal true} if the object was already registered; {@literal false} otherwise.
     */
    public boolean bindToDataIfExisting(Long appId, Object o, String dataId) {
        if (o == null) {
            return false;
        }
        Integer hashCode = getObjectHashCode(appId, o);
        if (hashCode == null) {
            // Not a task parameter object. Return the same object
            return false;
        }

        /*
         * The object has been accessed by a task before. Check with the API that the application has the last version,
         * blocking if necessary.
         */
        if (DEBUG) {
            LOGGER.debug("Linking data " + dataId + " with last value of object with hash code " + hashCode);
        }
        return this.itApi.bindExistingVersionToData(appId, o, hashCode, dataId);
    }

    /**
     * Collects the last value associated to an object.
     * 
     * @param appId Application Id.
     * @param o Object
     * @return internal value of the object, if it hadn't been registered returns the same object.
     */
    public Object collectObjectLastValue(Long appId, Object o) {
        if (o == null) {
            return o;
        }
        Integer hashCode = getObjectHashCode(appId, o);
        if (hashCode == null) {
            // Not a task parameter object. Return the same object
            return o;
        }
        /*
         * The object has been accessed by a task before. Check with the API that the application has the last version,
         * blocking if necessary.
         */
        if (DEBUG) {
            LOGGER.debug("New access to object with hash code " + hashCode + ", for writing: false");
        }

        // Get the updated version of the object
        Object oUpdated = this.itApi.getObject(appId, o, hashCode, serialDir);
        if (oUpdated != null) {
            this.internalObjects.put(hashCode, oUpdated);
            /*
             * The object has been accessed by a task before. Return its internal (real) value
             */
            if (DEBUG) {
                LOGGER.debug("Returning internal object " + oUpdated + " with hash code " + hashCode);
            }
            return oUpdated;
        }
        return o;
    }

    /**
     * Returns the internal object representing the given object {@code o}.
     *
     * @param appId Application Id.
     * @param o Object.
     * @return Internal object representing the given object {@code o}.
     */
    public Object getInternalObject(Long appId, Object o) {
        if (o == null) {
            return null;
        }
        Integer hashCode = getObjectHashCode(appId, o);
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
     * @param appId Application Id.
     * @param o Object.
     * @return {@code true} if the object has been removed, {@code false} otherwise.
     */
    public boolean delete(Long appId, Object o) {
        if (o == null) {
            LOGGER.warn("Trying to remove a null object from the object registry");
            return false;
        }
        Integer hashCode = getObjectHashCode(appId, o);
        if (hashCode == null) {
            LOGGER.warn("Trying to remove non task parameter object");
            return false; // Not a task parameter object
        }

        if (DEBUG) {
            LOGGER.debug("About to remove object with hash code " + hashCode + " from object registry.");
        }
        this.itApi.removeObject(appId, o, hashCode);

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
        AppEntry toDelete = this.appObjects.get(hashcode);
        if (toDelete != null) {
            toDelete.object = EMPTY;
            return true;
        }
        return false;
    }

}
