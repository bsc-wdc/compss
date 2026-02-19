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
package es.bsc.compss.loader;

import es.bsc.compss.api.Workflow;
import es.bsc.compss.log.Loggers;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ObjectRegistry {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.LOADER);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    // Api object used to invoke calls on the Integrated Toolkit
    private final Workflow wf;

    // Map: representative -> actual value
    private final Map<Object, Object> appObjects;


    /**
     * Creates a new ObjectRegistry instance associated to a given LoaderAPI {@code api}.
     *
     * @param wf Workflow created with accessed objects.
     */
    public ObjectRegistry(Workflow wf) {
        this.wf = wf;
        this.appObjects = new HashMap<>();
    }

    /**
     * Registers a new access to the given object {@code o} in mode {@code isWriter}.
     *
     * @param o Object.
     * @param isWriter {@code true} if its a writer access, {@code false} otherwise.
     * @return synchronized value of the object. If it hadn't been registered, it returns the same object.
     */
    public <T> T newObjectAccess(T o, boolean isWriter) {
        if (o == null) {
            return o;
        }

        if (!appObjects.containsKey(o)) {
            return o;
        }
        /*
         * The object has been accessed by a task before. Check with the API that the application has the last version,
         * blocking if necessary.
         */
        if (DEBUG) {
            int hashCode = System.identityHashCode(o);
            LOGGER.debug("New access to object with hash code " + hashCode + ", for writing: " + isWriter);
        }
        // Get the updated version of the object
        T oUpdated = this.wf.getObject(o);
        if (oUpdated != null) {
            appObjects.put(o, oUpdated);

            if (DEBUG) {
                int hashCode = System.identityHashCode(o);
                LOGGER.debug("Returning internal object " + oUpdated + " with hash code " + hashCode);
            }
            return oUpdated;
        }
        return o;
    }

    /**
     * Registers a new Object parameter.
     *
     * @param o Object parameter.
     */
    public void newObjectParameter(Object o) {
        if (o == null) {
            return;
        }
        appObjects.put(o, o);

        if (DEBUG) {
            int hashCode = System.identityHashCode(o);
            LOGGER.debug("Object " + o + " with hash code " + hashCode + " registered");
        }
    }

    /**
     * Links a data with the last known version for an object.
     *
     * @param o Object
     * @param dataId dataId to link
     * @return {@literal true} if the object was already registered; {@literal false} otherwise.
     */
    public boolean bindToDataIfExisting(Object o, String dataId) {
        if (o == null) {
            return false;
        }

        if (!appObjects.containsKey(o)) {
            return false;
        }

        /*
         * The object has been accessed by a task before. Check with the API that the application has the last version,
         * blocking if necessary.
         */
        if (DEBUG) {
            int hashCode = System.identityHashCode(o);
            LOGGER.debug("Linking data " + dataId + " with last value of object with hash code " + hashCode);
        }
        return this.wf.bindExistingVersionToData(o, dataId);
    }

    /**
     * Returns the internal object representing the given object {@code o}.
     *
     * @param o Object.
     * @return Internal object representing the given object {@code o}.
     */
    public <T> T getInternalObject(T o) {
        if (o == null) {
            return null;
        }

        if (!appObjects.containsKey(o)) {
            return null;
        }

        T internal = (T) appObjects.get(o);

        /*
         * The object has been accessed by a task before. Return its internal (real) value
         */
        if (DEBUG) {
            int hashCode = System.identityHashCode(o);
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
            LOGGER.warn("Trying to remove a null object from the object registry");
            return false;
        }

        if (!appObjects.containsKey(o)) {
            LOGGER.warn("Trying to remove non task parameter object");
            return false;
        }

        if (DEBUG) {
            int hashCode = System.identityHashCode(o);
            LOGGER.debug("About to remove object with hash code " + hashCode + " from object registry.");
        }
        this.wf.removeObject(o);

        appObjects.remove(o);

        return true;
    }

}
