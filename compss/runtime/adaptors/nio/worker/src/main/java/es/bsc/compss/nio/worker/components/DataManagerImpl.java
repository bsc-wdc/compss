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

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.data.DataManager;
import es.bsc.compss.data.DataProvider;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamURI;
import es.bsc.compss.types.execution.exceptions.InitializationException;
import es.bsc.compss.util.BindingDataManager;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Serializer;
import es.bsc.compss.util.Tracer;
import java.io.File;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import storage.StorageException;
import storage.StorageItf;
import storage.StubItf;


public class DataManagerImpl implements DataManager {

    private static final String ERROR_SERIALIZED_OBJ = "ERROR: Cannot obtain object";
    private static final String ERROR_PERSISTENT_OBJ = "ERROR: Cannot getById persistent object";
    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER);

    // Data Provider
    private final DataProvider provider;
    //String hostName
    private final String hostName;
    //Default data folder
    private final String baseFolder;
    //Storage configuration file
    private final String storageConf;

    // Cache
    private final HashMap<String, Object> objectCache;

    /**
     * Instantiates a new Data Manager
     *
     * @param hostName
     * @param baseFolder
     * @param provider
     */
    public DataManagerImpl(String hostName, String baseFolder, DataProvider provider) {
        this.objectCache = new HashMap<>();
        this.provider = provider;
        this.hostName = hostName;
        this.baseFolder = baseFolder;
        String storageCfg = System.getProperty(COMPSsConstants.STORAGE_CONF);
        if (storageCfg == null || storageCfg.equals("") || storageCfg.equals("null")) {
            storageCfg = null;
        }
        this.storageConf = storageCfg;
    }

    /**
     * Initializes the Data Manager
     *
     * @throws InitializationException
     */
    @Override
    public void init() throws InitializationException {
        if (storageConf != null) {
            try {
                StorageItf.init(storageConf);
            } catch (StorageException e) {
                ErrorManager.fatal("Error loading storage configuration file: " + storageConf, e);
            }
        } else {
            LOGGER.warn("No storage configuration file passed");
        }
        // All structures are already instanciated
    }

    /**
     * Returns the path of the configuration file used by the persistent storage
     *
     * @return path of the storage configuration file
     */
    @Override
    public String getStorageConf() {
        return storageConf;
    }

    /**
     * Stops the Data Manager and its sub-components
     *
     */
    @Override
    public void stop() {
        // End storage
        if (storageConf != null) {
            try {
                StorageItf.finish();
            } catch (StorageException e) {
                LOGGER.error("Error releasing storage library: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void removeObsoletes(List<String> obsoletes) {

        try {
            for (String name : obsoletes) {
                if (name.startsWith(File.separator)) {
                    LOGGER.debug("Removing file " + name);
                    File f = new File(name);
                    if (!f.delete()) {
                        LOGGER.error("Error removing file " + f.getAbsolutePath());
                    }
                    // Now only manage at C (python could do the same when cache available)
                    //if (COMPSsConstants.Lang.valueOf(lang.toUpperCase()) == COMPSsConstants.Lang.C && persistentC) {
                    //    if (BindingDataManager.removeData(f.getName()) != 0) {
                    //        LOGGER.error("Error removing data " + f.getName() + " from Binding");
                    //    } else {
                    //        LOGGER.debug("Data removed from cache " + f.getName());
                    //    }
                    //}

                }
                String dataName = new File(name).getName();
                synchronized (objectCache) {
                    objectCache.remove(dataName);
                    LOGGER.debug(dataName + " removed from cache.");
                }
                LOGGER.debug(name + " removed from cache.");
            }
        } catch (Exception e) {
            LOGGER.error("Exception", e);
        }
    }

    @Override
    public void fetchParam(InvocationParam param, int i, LoadDataListener tt) {
        switch (param.getType()) {
            case OBJECT_T:
                fetchObject(param, i, tt);
                break;
            case PSCO_T:
                fetchPSCO(param, i, tt);
                break;
            case BINDING_OBJECT_T:
                fetchBindingObject(param, i, tt);
                break;
            case FILE_T:
                fetchFile(param, i, tt);
                break;
            case EXTERNAL_PSCO_T:
                // Nothing to do since external parameters send their ID directly
                tt.loadedValue();
                break;
            default:
            //Nothing to do since basic type parameters require no action
        }
    }

    private void fetchObject(InvocationParam param, int index, LoadDataListener tt) {
        String rename = (String) param.getValue();
        LOGGER.debug("   - " + rename + " registered as object.");
        boolean askTransfer = false;

        // Try if parameter is in cache
        LOGGER.debug("   - Checking if " + rename + " is in cache.");
        boolean catched;
        synchronized (objectCache) {
            catched = objectCache.containsKey(rename);
        }
        if (!catched) {
            // Try if any of the object locations is in cache
            boolean locationsInCache = false;
            LOGGER.debug("   - Checking if " + rename + " locations are catched");
            for (InvocationParamURI loc : param.getSources()) {
                String locName = loc.getPath();
                synchronized (objectCache) {
                    catched = objectCache.containsKey(locName);
                }
                if (catched) {
                    // Object found
                    LOGGER.debug("   - Parameter " + index + "(" + rename + ") location found in cache.");
                    try {
                        if (param.isPreserveSourceData()) {
                            LOGGER.debug("   - Parameter " + index + "(" + rename + ") preserves sources. CACHE-COPYING");
                            Object old;
                            synchronized (objectCache) {
                                old = objectCache.get(locName);
                            }
                            Object o = Serializer.deserialize(Serializer.serialize(old));
                            storeObject(rename, o);
                        } else {
                            LOGGER.debug("   - Parameter " + index + "(" + rename + ") erases sources. CACHE-MOVING");
                            Object o;
                            synchronized (objectCache) {
                                o = objectCache.get(locName);
                                objectCache.remove(locName);
                            }
                            storeObject(rename, o);
                        }
                        locationsInCache = true;
                    } catch (IOException ioe) {
                        // If exception is raised, locationsInCache remains false. We log the exception
                        // and try host files
                        LOGGER.error("IOException", ioe);
                    } catch (ClassNotFoundException e) {
                        // If exception is raised, locationsInCache remains false. We log the exception
                        // and try host files
                        LOGGER.error("ClassNotFoundException", e);
                    }
                    // Stop looking for locations
                    break;
                }
            }

            if (!locationsInCache) {
                // Try if any of the object locations is in the host
                boolean existInHost = false;
                LOGGER.debug("   - Checking if " + rename + " locations are in host");
                for (InvocationParamURI loc : param.getSources()) {
                    if (loc.isHost(hostName)) {
                        LOGGER.debug("   - Parameter " + index + "(" + rename + ") found at host.");
                        try {
                            File source = new File(baseFolder + File.separator + loc.getPath());
                            File target = new File(baseFolder + File.separator + param.getValue().toString());
                            if (param.isPreserveSourceData()) {
                                LOGGER
                                        .debug("   - Parameter " + index + "(" + rename + ") preserves sources. COPYING");
                                LOGGER.debug("         Source: " + source);
                                LOGGER.debug("         Target: " + target);
                                Files.copy(source.toPath(), target.toPath());
                            } else {
                                LOGGER.debug("   - Parameter " + index + "(" + rename + ") erases sources. MOVING");
                                LOGGER.debug("         Source: " + source);
                                LOGGER.debug("         Target: " + target);
                                if (!source.renameTo(target)) {
                                    LOGGER
                                            .error("Error renaming file from " + source.getAbsolutePath() + " to " + target.getAbsolutePath());
                                }
                            }
                            // Move object to cache
                            Object o = Serializer.deserialize(rename);
                            storeObject(rename, o);
                            existInHost = true;
                        } catch (IOException ioe) {
                            // If exception is raised, locationsInCache remains false. We log the exception
                            // and try host files
                            LOGGER.error("IOException", ioe);
                        } catch (ClassNotFoundException e) {
                            // If exception is raised, locationsInCache remains false. We log the exception
                            // and try host files
                            LOGGER.error("ClassNotFoundException", e);
                        }
                    }
                }
                if (!existInHost) {
                    // We must transfer the file
                    askTransfer = true;
                }
            }
        }

        // Request the transfer if needed
        askForTransfer(askTransfer, param, index, tt);
    }

    private void fetchBindingObject(InvocationParam param, int index, LoadDataListener tt) {
        String name = (String) param.getValue();
        LOGGER.debug("   - " + name + " registered as binding object.");
        String[] extObjVals = (name).split("#");
        String value = extObjVals[0];
        int type = Integer.parseInt(extObjVals[1]);
        int elements = Integer.parseInt(extObjVals[2]);
        boolean askTransfer = false;

        // Try if parameter is in cache
        LOGGER.debug("   - Checking if " + value + " is in binding cache.");
        boolean cached = BindingDataManager.isInBinding(value);
        if (!cached) {
            // Try if any of the object locations is in cache
            boolean locationsInCache = false;
            LOGGER.debug("   - Checking if " + value + " locations are catched");
            for (InvocationParamURI loc : param.getSources()) {
                String locName = loc.getPath();
                BindingObject bo = BindingObject.generate(locName);
                if (BindingDataManager.isInBinding(bo.getId())) {
                    // Object found
                    LOGGER.debug("   - Parameter " + index + "(" + value + ") location found in cache.");
                    if (param.isPreserveSourceData()) {
                        LOGGER.debug("   - Parameter " + index + "(" + value + ") preserves sources. CACHE-COPYING");
                        int res = BindingDataManager.copyCachedData(bo.getId(), value);
                        if (res != 0) {
                            LOGGER.error("CACHE-COPY from " + bo.getId() + " to " + value + " has failed. ");
                            break;
                        }
                    } else {
                        LOGGER.debug("   - Parameter " + index + "(" + value + ") erases sources. CACHE-MOVING");
                        int res = BindingDataManager.moveCachedData(bo.getId(), value);
                        if (res != 0) {
                            LOGGER.error("CACHE-MOVE from " + locName + " to " + value + " has failed. ");
                            break;
                        }
                    }
                    locationsInCache = true;
                    break;
                }
            }
            //TODO is it better to transfer again or to load from file??
            if (!locationsInCache) {
                // Try if any of the object locations is in the host
                boolean existInHost = false;
                LOGGER.debug("   - Checking if " + name + " locations are in host");
                for (InvocationParamURI loc : param.getSources()) {
                    if (loc.isHost(hostName)) {
                        BindingObject bo = BindingObject.generate(loc.getPath());
                        LOGGER.debug("   - Parameter " + index + "(" + name + ") found at host with location " + loc.getPath() + " Checking if in cache...");
                        if (BindingDataManager.isInBinding(bo.getId())) {
                            // Object found
                            LOGGER.debug("   - Parameter " + index + "(" + value + ") location found in cache.");
                            if (param.isPreserveSourceData()) {
                                LOGGER.debug("   - Parameter " + index + "(" + value + ") preserves sources. CACHE-COPYING");
                                int res = BindingDataManager.copyCachedData(bo.getId(), value);
                                if (res != 0) {
                                    LOGGER.error("CACHE-COPY from " + bo.getId() + " to " + value + " has failed. ");
                                }
                            } else {
                                LOGGER.debug("   - Parameter " + index + "(" + value + ") erases sources. CACHE-MOVING");
                                int res = BindingDataManager.moveCachedData(bo.getId(), value);
                                if (res != 0) {
                                    LOGGER.error("CACHE-MOVE from " + bo.getId() + " to " + value + " has failed. ");
                                }
                            }
                            existInHost = true;
                        } else {
                            LOGGER.debug("   - Parameter " + index + "(" + name + ") not in cache.");

                            if (new File(baseFolder + File.separator + loc.getPath()).exists()) {
                                int res = BindingDataManager.loadFromFile(value, loc.getPath(), type, elements);
                                if (res == 0) {
                                    existInHost = true;
                                } else {
                                    LOGGER.error("Error loading " + param.getValue() + " from file " + loc.getPath());
                                }
                            }
                        }
                    }
                }
                if (!existInHost) {
                    // We must transfer the file
                    askTransfer = true;
                }
            }
        }

        // Request the transfer if needed
        askForTransfer(askTransfer, param, index, tt);

    }

    private void fetchPSCO(InvocationParam param, int paramIdx, LoadDataListener tt) {
        String rename = param.getDataMgmtId();
        String pscoId = (String) param.getValue();
        LOGGER.debug("   - " + pscoId + " registered as PSCO.");
        // The replica must have been ordered by the master so the real object must be
        // catched or can be retrieved by the ID

        // Try if parameter is in cache
        LOGGER.debug("   - Checking if " + pscoId + " is in cache.");
        LOGGER.debug("   - Retrieving psco " + pscoId + " from Storage");
        // Get Object from its ID
        Object obj = null;
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.STORAGE_GETBYID.getId(), Tracer.Event.STORAGE_GETBYID.getType());
        }
        try {
            obj = StorageItf.getByID(pscoId);
            param.setValue(obj);
            tt.loadedValue();
        } catch (StorageException e) {
            LOGGER.error("Cannot getByID PSCO " + pscoId, e);
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_GETBYID.getType());
            }
        }

        LOGGER.debug("   - PSCO with id " + pscoId + " stored");
    }

    private void fetchFile(InvocationParam param, int index, LoadDataListener tt) {
        LOGGER.debug("   - " + (String) param.getValue() + " registered as file.");

        boolean locationsInHost = false;
        boolean askTransfer = false;

        // Try if parameter is in the host
        LOGGER.debug("   - Checking if file " + (String) param.getValue() + " exists.");
        File f = new File(param.getValue().toString());
        if (!f.exists()) {
            // Try if any of the locations is in the same host
            LOGGER.debug("   - Checking if " + (String) param.getValue() + " exists in worker");
            for (InvocationParamURI loc : param.getSources()) {
                if (loc.isHost(hostName)) {
                    // Data is already present at host
                    LOGGER.debug("   - Parameter " + index + "(" + (String) param.getValue() + ") found at host.");
                    try {
                        File source = new File(loc.getPath());
                        File target = new File(param.getValue().toString());
                        if (param.isPreserveSourceData()) {
                            LOGGER.debug("   - Parameter " + index + "(" + (String) param.getValue() + ") preserves sources. COPYING");
                            LOGGER.debug("         Source: " + source);
                            LOGGER.debug("         Target: " + target);
                            // if (!source.exists() && (Lang.valueOf(getLang().toUpperCase()) != Lang.C)) {
                            if (!source.exists()) {
                                LOGGER.debug("source does not exist, preserve data");
                                /*
                                LOGGER.debug("lang is " + getLang());
                                if (getLang().toUpperCase() != "C") {
                                    LOGGER.debug(
                                            "[ERROR] File " + loc.getPath() + " does not exist but it could be an object in cache. Ignoring.");
                                }
                                 */
                                // TODO The file to be copied needs to be serialized to a file from cache (or serialize from
                                // memory to memory
                                // if possible with a specific function
                            } else {
                                LOGGER.debug("before copy");
                                Files.copy(source.toPath(), target.toPath());
                                LOGGER.debug("after copy");
                            }
                        } else {
                            LOGGER.debug("   - Parameter " + index + "(" + (String) param.getValue() + ") erases sources. MOVING");
                            LOGGER.debug("         Source: " + source);
                            LOGGER.debug("         Target: " + target);
                            if (!source.exists()) {
                                LOGGER.debug("source does not exist, no preserve data");
                                /*
                                LOGGER.debug("lang is " + getLang() + ", in uppercase is " + getLang().toUpperCase());
                                if (getLang().toUpperCase() != "C") {
                                    LOGGER
                                            .debug("File " + loc.getPath() + " does not exist but it could be an object in cache. Ignoring.");
                                }
                                 */
                            } else {
                                try {
                                    Files.move(source.toPath(), target.toPath(), StandardCopyOption.ATOMIC_MOVE);
                                } catch (AtomicMoveNotSupportedException amnse) {
                                    LOGGER.warn(
                                            "WARN: AtomicMoveNotSupportedException. File cannot be atomically moved. Trying to move without atomic");
                                    Files.move(source.toPath(), target.toPath());
                                }
                            }
                        }
                        locationsInHost = true;
                    } catch (IOException ioe) {
                        LOGGER.error("IOException", ioe);
                    }
                }
            }

            if (!locationsInHost) {
                // We must transfer the file
                askTransfer = true;
            }
        }
        // Request the transfer if needed
        askForTransfer(askTransfer, param, index, tt);
    }

    @Override
    public void loadParam(InvocationParam param) throws Exception {
        switch (param.getType()) {
            case OBJECT_T:
                loadObject(param);
                break;
            case PSCO_T: // fetch stage already set the value on the param
            case FILE_T: // value already contains the path
            case BINDING_OBJECT_T: // value corresponds to the ID of the object on the binding (already set)
            case EXTERNAL_PSCO_T: // value corresponds to the ID of the 
                break;
            default:
            //Nothing to do since basic type parameters require no action
        }
    }

    private void loadObject(InvocationParam param) throws Exception {
        String rename = param.getDataMgmtId();
        Object o = null;
        boolean contained = false;
        synchronized (objectCache) {
            contained = objectCache.containsKey(rename);
            if (contained) {
                o = objectCache.get(rename);
            }
        }

        if (!contained) {
            //Might be stored as a file
            String file = baseFolder + File.separator + param.getValue().toString();
            File f = new File(file);
            if (!f.exists()) {
                try {
                    o = Serializer.deserialize(file);
                    storeObject(rename, o);
                    contained = true;
                } catch (IOException | ClassNotFoundException ex) {
                    LOGGER.warn(ERROR_SERIALIZED_OBJ, ex);
                }
            }
        }
        if (!contained) {
            throw new Exception("Value was not loaded on memory nor serialized on a file");
        }
        param.setValue(o);
    }

    private void loadPSCO(InvocationParam param) {
        String pscoId = (String) param.getValue();
        Object o = null;
        synchronized (objectCache) {
            o = objectCache.get(pscoId);
        }
        param.setValue(o);
    }

    @Override
    public void storeParam(InvocationParam param) {
        switch (param.getType()) {
            case OBJECT_T:
                storeObject(param.getDataMgmtId(), param.getValue());
                break;
            case PSCO_T:
            case EXTERNAL_PSCO_T:
                //storePSCO(param);
                break;
            case BINDING_OBJECT_T:
                //Already stored on the binding
                break;
            case FILE_T:
                //Already stored
                break;

        }
    }

    @Override
    public void storeValue(String name, Object value) {
        storeObject(name, value);
    }

    @Override
    public synchronized Object getObject(String dataMgmtId) {
        return objectCache.get(dataMgmtId);
    }

    /*
     * ****************************************************************************************************************
     * STORE METHODS
     *****************************************************************************************************************/
    public synchronized void storeObject(String name, Object value) {
        try {
            objectCache.put(name, value);
        } catch (NullPointerException e) {
            LOGGER.error("Object Cache " + objectCache + " dataId " + name + " object " + value);
        }
    }

    private void askForTransfer(boolean askTransfer, InvocationParam param, int index, LoadDataListener tt) {
        if (askTransfer) {
            LOGGER.info("- Parameter " + index + "(" + (String) param.getValue() + ") does not exist, requesting data transfer");
            provider.askForTransfer(param, index, tt);
        } else {
            LOGGER.info("- Parameter " + index + "(" + (String) param.getValue() + ") already exists.");
            tt.loadedValue();
        }
    }

}
