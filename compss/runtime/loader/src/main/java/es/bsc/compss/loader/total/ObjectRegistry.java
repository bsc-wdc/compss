package es.bsc.compss.loader.total;

import es.bsc.compss.loader.LoaderAPI;
import es.bsc.compss.log.Loggers;

import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ObjectRegistry {

    // Api object used to invoke calls on the Integrated Toolkit
    private LoaderAPI itApi;
    // Temporary directory where the files containing objects will be stored (same as the stream registry dir)
    private String serialDir;
    // Map: hash code -> object
    // Objects
    private Map<Integer, Object> appTaskObjects;
    private Map<Integer, Object> internalObjects;

    private static final Logger LOGGER = LogManager.getLogger(Loggers.LOADER);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();


    public ObjectRegistry(LoaderAPI api) {
        this.itApi = api;
        this.serialDir = api.getTempDir();
        this.appTaskObjects = new TreeMap<>();
        this.internalObjects = new TreeMap<>();

        this.itApi.setObjectRegistry(this);
    }

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
            if (objStored == null) {
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

    public void newObjectAccess(Object o) {
        newObjectAccess(o, true);
    }

    public void newObjectAccess(Object o, boolean isWriter) {
        if (o == null) {
            return;
        }

        int hashCode = o.hashCode();

        Object oStored = this.appTaskObjects.get(hashCode);
        while (oStored != o) {
            if (oStored == null) {
                return; // Not a task parameter object
            } else {
                oStored = this.appTaskObjects.get(++hashCode);
            }
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

    public void serializeLocally(Object o) {
        if (o == null) {
            return;
        }

        int hashCode = o.hashCode();
        Object oStored = this.appTaskObjects.get(hashCode);
        while (oStored != o) {
            if (oStored == null) {
                return; // Not a task parameter object
            } else {
                oStored = this.appTaskObjects.get(++hashCode);
            }
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

    public Object getInternalObject(Object o) {
        if (o == null) {
            return null;
        }

        int hashCode = o.hashCode();
        Object oStored = this.appTaskObjects.get(hashCode);
        while (oStored != o) {
            if (oStored == null) {
                return null; // Not a task parameter object
            } else {
                oStored = this.appTaskObjects.get(++hashCode);
            }
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

}
