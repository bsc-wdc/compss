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
import es.bsc.compss.nio.worker.exceptions.InitializationException;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DataManager {

    // Logger
    private static final Logger wLogger = LogManager.getLogger(Loggers.WORKER);
    // Cache
    private final HashMap<String, Object> objectCache;


    /**
     * Instantiates a new Data Manager
     * 
     */
    public DataManager() {
        objectCache = new HashMap<>();
    }

    /**
     * Initializes the Data Manager
     * 
     * @throws InitializationException
     */
    public void init() throws InitializationException {
        // All structures are already defined
    }

    /**
     * Stops the Data Manager and its sub-components
     * 
     */
    public void stop() {

    }

    /*
     * ****************************************************************************************************************
     * STORE METHODS
     *****************************************************************************************************************/
    public synchronized void storeObject(String name, Object value) {
        try {
            objectCache.put(name, value);
        } catch (NullPointerException e) {
            wLogger.error("Object Cache " + objectCache + " dataId " + name + " object " + value);
        }
    }

    /*
     * ****************************************************************************************************************
     * GET METHODS
     *****************************************************************************************************************/
    public synchronized Object getObject(String name) {
        return objectCache.get(name);
    }

    /*
     * ****************************************************************************************************************
     * REMOVE METHODS
     *****************************************************************************************************************/
    public synchronized void remove(String name) {
        objectCache.remove(name);
    }

    /*
     * ****************************************************************************************************************
     * CHECKER METHODS
     *****************************************************************************************************************/
    public synchronized boolean checkPresence(String name) {
        return objectCache.containsKey(name);
    }

}
