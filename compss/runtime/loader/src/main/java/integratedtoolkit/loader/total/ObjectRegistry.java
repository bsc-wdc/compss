package integratedtoolkit.loader.total;

import integratedtoolkit.ITConstants;
import integratedtoolkit.loader.LoaderAPI;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.parameter.PSCOId;

import java.util.TreeMap;

import org.apache.log4j.Logger;

import storage.StorageItf;
import storage.StubItf;


public class ObjectRegistry {

    // Api object used to invoke calls on the Integrated Toolkit
    private LoaderAPI itApi;
    // Temporary directory where the files containing objects will be stored (same as the stream registry dir)
    private String serialDir;
    // Map: hash code -> object
    // Objects 
    private TreeMap<Integer, Object> appTaskObjects;
    private TreeMap<Integer, Object> internalObjects;

    private static final Logger logger = Logger.getLogger(Loggers.LOADER);
    private static final boolean debug = logger.isDebugEnabled();
    
	// Tracing
	protected static boolean tracing = System
			.getProperty(ITConstants.IT_TRACING) != null
			&& System.getProperty(ITConstants.IT_TRACING).equals("true") ? true
			: false;  
    
    public ObjectRegistry(LoaderAPI api) {
        this.itApi = api;
        this.serialDir = api.getTempDir();
        this.appTaskObjects = new TreeMap<Integer, Object>();
        this.internalObjects = new TreeMap<Integer, Object>();

        itApi.setObjectRegistry(this);
    }

    public int newObjectParameter(Object o) {
        if (o == null) {
            return Integer.MAX_VALUE;
        }

        int hashCode = o.hashCode();
        Object oStored = appTaskObjects.get(hashCode);
        while (oStored != o) {
            if (oStored == null) {
                appTaskObjects.put(hashCode, o);
                // Store it as an internal one too. Read-only objects will always use this same instance
                internalObjects.put(hashCode, o);
                if (debug) {
                    logger.debug("Object " + o + " with hash code " + hashCode + " registered");
                }

                break;
            }
            // (oStored != o != null) {
    		/* Coincidence of two equal hash codes for different objects.
             * Increment the hash code and try again.
             */
            oStored = appTaskObjects.get(++hashCode);
        }

        return hashCode;
    }

    public void newObjectAccess(Object o) {
        newObjectAccess(o, true);
    }

    public void newObjectAccess(Object o, boolean isWriter) {
        if (o == null) {
            return;
        }

        int hashCode = o.hashCode();
        
        Object oStored = appTaskObjects.get(hashCode);
        while (oStored != o) {
            if (oStored == null) {
                return; // Not a task parameter object
            } else {
                oStored = appTaskObjects.get(++hashCode);
            }
        }
        /* The object has been accessed by a task before.
         * Check with the API that the application has the last version,
         * blocking if necessary.
         */
        if (debug) {
            logger.debug("New access to object with hash code " + hashCode + ", for writing: " + isWriter);
        }

        // Get the updated version of the object
        Object oUpdated = itApi.getObject(o, hashCode, serialDir);
        if (oUpdated != null) {
            internalObjects.put(hashCode, oUpdated);
        }
    }

    public void serializeLocally(Object o) {
        if (o == null) {
            return;
        }

        int hashCode = o.hashCode();
        Object oStored = appTaskObjects.get(hashCode);
        
        if (oStored instanceof PSCOId) {
        	return; // A PSCO doesn't need to be serialized
        }        
        
        while (oStored != o) {
            if (oStored == null) {
                return; // Not a task parameter object
            } else {
                oStored = appTaskObjects.get(++hashCode);
            }
        }
        /* The object has been accessed by a task before.
         * Delegate its serialization to the API.
         * Serialize the internal object
         */
        if (debug) {
            logger.debug("About to serialize locally object with hash code " + hashCode);
        }

        itApi.serializeObject(internalObjects.get(hashCode), hashCode, serialDir);
    }

    public Object getInternalObject(Object o) {
        if (o == null) {
            return null;
        }

        int hashCode = o.hashCode();
        Object oStored = appTaskObjects.get(hashCode);
        while (oStored != o) {
            if (oStored == null) {
                return null; // Not a task parameter object
            } else {
                oStored = appTaskObjects.get(++hashCode);
            }
        }
        
		Object internal = internalObjects.get(hashCode);
    	
        if (oStored instanceof StubItf) {        	       	        		
        	if (internal instanceof PSCOId) {
        		PSCOId pscoId = new PSCOId(o, ((PSCOId) internal).getId());        			
        		internalObjects.put(hashCode, pscoId);
        		oStored = pscoId;       		
        	} 
        }
        
        if (oStored instanceof PSCOId) {
        	try {
        		//String firstPSCOId = ((PSCOId) oStored).getId();        		        	
        		String lastPSCOId = ((PSCOId) internalObjects.get(hashCode)).getId();
        		/*
        		StorageItf.consolidateVersion(lastPSCOId);
        		if (debug) {
        			logger.debug("Consolidate internal object " + hashCode + " and PSCO Id " + lastPSCOId);
        		}
        		Object oStorage = StorageItf.getByID(firstPSCOId);
        		*/
        		
    	        
        		//if (tracing){
    	        //    Tracer.emitEvent(Tracer.Event.STORAGE_GETBYID.getId(), Tracer.getRuntimeEventsType());
    	        //}
    			
        		Object oStorage = StorageItf.getByID(lastPSCOId);
    			
    	        //if (tracing){
    	        //    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
    	        //}
        		
        		internal = oStorage;        		        		        		        		        
        	} catch (Exception e) {
            	logger.debug("Error returning internal " + ((PSCOId) oStored).getId() + " hash code " + hashCode);
        	}
        }
                
        /* The object has been accessed by a task before.
         * Return its internal (real) value
         */
        if (debug) {
           logger.debug("Returning internal object " + internal + " with hash code "+ hashCode );
        }    
        return internal;    	
    }
}
