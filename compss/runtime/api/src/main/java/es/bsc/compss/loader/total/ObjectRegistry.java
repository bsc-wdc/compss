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

    // Api object used to invoke calls on the Integrated Toolkit
    private final LoaderAPI itApi;
    // Temporary directory where the files containing objects will be stored (same as the stream registry dir)
    private final String serialDir;

    // Map: appID -> Map: hashcode -> object
    private final Map<Long, Map<Integer, Object>> apps;


    /**
     * Creates a new ObjectRegistry instance associated to a given LoaderAPI {@code api}.
     *
     * @param api LoaderAPI.
     */
    public ObjectRegistry(LoaderAPI api) {
        this.itApi = api;
        this.serialDir = api.getTempDir();
        this.apps = new TreeMap<>();
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
        Map<Integer, Object> appObjects = apps.get(appId);
        if (appObjects == null) {
            return;
        }
        int hashCode = System.identityHashCode(o);
        if (!appObjects.containsKey(hashCode)) {
            return;
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
            appObjects.put(hashCode, oUpdated);
        }
    }

    /**
     * Registers a new Object parameter.
     *
     * @param appId Application Id.
     * @param o Object parameter.
     * @return Final hashcode of the object.
     */
    public int newObjectParameter(Long appId, Object o) {
        if (o == null) {
            return Integer.MAX_VALUE;
        }
        Map<Integer, Object> appObjects = apps.get(appId);
        if (appObjects == null) {
            appObjects = new TreeMap<>();
            apps.put(appId, appObjects);
        }
        int hashcode = System.identityHashCode(o);
        appObjects.put(hashcode, o);

        if (DEBUG) {
            LOGGER.debug("Object " + o + " with hash code " + hashcode + " registered");
        }

        return hashcode;
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
        Map<Integer, Object> appObjects = apps.get(appId);
        if (appObjects == null) {
            return;
        }
        int hashCode = System.identityHashCode(o);
        if (!appObjects.containsKey(hashCode)) {
            return;
        }
        /*
         * The object has been accessed by a task before. Delegate its serialization to the API. Serialize the internal
         * object
         */
        if (DEBUG) {
            LOGGER.debug("About to serialize locally object with hash code " + hashCode);
        }
        Object value = appObjects.get(hashCode);
        this.itApi.serializeObject(value, hashCode, serialDir);
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
        Map<Integer, Object> appObjects = apps.get(appId);
        if (appObjects == null) {
            return false;
        }
        int hashCode = System.identityHashCode(o);
        if (!appObjects.containsKey(hashCode)) {
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
        Map<Integer, Object> appObjects = apps.get(appId);
        if (appObjects == null) {
            return o;
        }
        int hashCode = System.identityHashCode(o);
        if (!appObjects.containsKey(hashCode)) {
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
            appObjects.put(hashCode, oUpdated);
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
        Map<Integer, Object> appObjects = apps.get(appId);
        if (appObjects == null) {
            return null;
        }
        int hashCode = System.identityHashCode(o);
        if (!appObjects.containsKey(hashCode)) {
            return null;
        }

        Object internal = appObjects.get(hashCode);

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
        Map<Integer, Object> appObjects = apps.get(appId);
        if (appObjects == null) {
            LOGGER.warn("Trying to remove non task parameter object");
            return false;
        }
        int hashCode = System.identityHashCode(o);
        if (!appObjects.containsKey(hashCode)) {
            LOGGER.warn("Trying to remove non task parameter object");
            return false;
        }

        if (DEBUG) {
            LOGGER.debug("About to remove object with hash code " + hashCode + " from object registry.");
        }
        this.itApi.removeObject(appId, o, hashCode);

        appObjects.remove(hashCode);

        return true;
    }

}
