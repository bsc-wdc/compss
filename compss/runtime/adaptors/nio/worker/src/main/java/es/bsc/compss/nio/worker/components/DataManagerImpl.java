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
import es.bsc.compss.types.data.location.DataLocation.Protocol;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamURI;
import es.bsc.compss.types.execution.exceptions.InitializationException;
import es.bsc.compss.util.BindingDataManager;
import es.bsc.compss.util.ErrorManager;
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

    private final HashMap<String, DataRegister> registry;

    /**
     * Instantiates a new Data Manager
     *
     * @param hostName
     * @param baseFolder
     * @param provider
     */
    public DataManagerImpl(String hostName, String baseFolder, DataProvider provider) {
        this.registry = new HashMap<>();
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
                DataRegister register = null;
                synchronized (registry) {
                    register = registry.remove(dataName);
                    LOGGER.debug(dataName + " removed from cache.");
                }
                if (register != null) {
                    synchronized (register) {
                        register.clear();
                    }
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

    private DataRegister getOriginalDataRegister(InvocationParam param) {
        String originalRename = param.getSourceDataId();
        DataRegister originalRegister;
        boolean newRegister = false;
        synchronized (registry) {
            originalRegister = registry.get(originalRename);
            if (originalRegister == null) {
                originalRegister = new DataRegister();
                registry.put(originalRename, originalRegister);
                newRegister = true;
            }
        }

        if (newRegister) {
            synchronized (originalRegister) {
                for (InvocationParamURI loc : param.getSources()) {
                    switch (loc.getProtocol()) {
                        case FILE_URI:
                            if (loc.isHost(hostName)) {
                                originalRegister.addFileLocation(loc.getPath());
                            }
                            break;
                        case PERSISTENT_URI:
                            String pscoId = loc.getPath();
                            originalRegister.setStorageId(pscoId);
                            break;
                        case OBJECT_URI:
                        case BINDING_URI:
                            if (loc.isHost(hostName)) {
                                LOGGER.error("WORKER IS NOT AWARE OF THE PRESENCE OF A"
                                        + (loc.getProtocol() == Protocol.OBJECT_URI ? "N OBJECT " : " BINDING OBJECT ")
                                );
                            }
                            break;
                        case SHARED_URI:
                            break;

                        default:
                    }
                }
            }
        }

        return originalRegister;
    }

    private void fetchObject(InvocationParam param, int index, LoadDataListener tt) {
        final String finalRename = param.getDataMgmtId();
        final String originalRename = param.getSourceDataId();
        LOGGER.debug("   - " + finalRename + " registered as object.");
        DataRegister originalRegister = getOriginalDataRegister(param);
        // Try if parameter is in cache
        LOGGER.debug("   - Checking if " + finalRename + " is in cache.");
        synchronized (originalRegister) {
            if (originalRegister.isLocal()) {
                if (finalRename.equals(originalRename)) {
                    try {
                        originalRegister.loadValue();
                    } catch (Exception e) {
                        LOGGER.error(e);
                    }
                } else {
                    try {
                        Object o;
                        if (param.isPreserveSourceData()) {
                            o = originalRegister.cloneValue();
                        } else {
                            o = originalRegister.loadValue();
                            originalRegister.removeValue();
                        }
                        DataRegister dr = new DataRegister();
                        dr.setValue(o);
                        registry.put(finalRename, dr);
                    } catch (Exception e) {
                        LOGGER.error(e);
                    }
                }
                fetchedLocalParameter(param, index, tt);
            } else {
                transferParameter(param, index, tt);
            }
        }
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
        String finalRename = param.getDataMgmtId();
        String pscoId = (String) param.getValue();
        LOGGER.debug("   - " + pscoId + " registered as PSCO.");
        // The replica must have been ordered by the master so the real object must be
        // catched or can be retrieved by the ID

        DataRegister dr = new DataRegister();
        dr.setStorageId(pscoId);
        registry.put(finalRename, dr);
        tt.loadedValue();
    }

    private void fetchFile(InvocationParam param, int index, LoadDataListener tt) {
        LOGGER.debug("   - " + (String) param.getValue() + " registered as file.");
        final String originalName = param.getSourceDataId();
        final String expectedFileLocation = param.getValue().toString();
        LOGGER.debug("   - Checking if file " + (String) param.getValue() + " exists.");
        File f = new File(expectedFileLocation);
        if (f.exists()) {
            LOGGER.info("- Parameter " + index + "(" + expectedFileLocation + ") already exists.");
            fetchedLocalParameter(param, index, tt);
            return;
        }
        LOGGER.debug("   - Checking if " + expectedFileLocation + " exists in worker");
        DataRegister originalRegister = getOriginalDataRegister(param);
        synchronized (originalRegister) {
            if (originalRegister.isLocal()) {
                LOGGER.debug("   - Parameter " + index + "(" + expectedFileLocation + ") found at host.");

                File target = new File(expectedFileLocation);
                List<String> files = originalRegister.getFileLocations();
                for (String path : files) {
                    File source = new File(path);
                    try {
                        LOGGER.debug("   - Parameter " + index + "(" + expectedFileLocation + ") "
                                + (param.isPreserveSourceData()
                                        ? "preserves sources. COPYING"
                                        : "erases sources. MOVING")
                        );
                        LOGGER.debug("         Source: " + source);
                        LOGGER.debug("         Target: " + target);
                        if (param.isPreserveSourceData()) {
                            Files.copy(source.toPath(), target.toPath());
                        } else {
                            try {
                                Files.move(source.toPath(), target.toPath(), StandardCopyOption.ATOMIC_MOVE);
                            } catch (AtomicMoveNotSupportedException amnse) {
                                LOGGER.warn(
                                        "WARN: AtomicMoveNotSupportedException. File cannot be atomically moved. Trying to move without atomic");
                                Files.move(source.toPath(), target.toPath());
                            }
                            originalRegister.removeFileLocation(path);
                        }
                        DataRegister dr = new DataRegister();
                        dr.addFileLocation(path);
                        registry.put(originalName, dr);
                        fetchedLocalParameter(param, index, tt);
                        return;
                    } catch (IOException ioe) {
                        LOGGER.error("IOException", ioe);
                    }
                }
            } else {
                LOGGER.info("- Parameter " + index + "(" + expectedFileLocation + ") does not exist, requesting data transfer");
                transferParameter(param, index, tt);
            }
        }
    }

    @Override
    public void loadParam(InvocationParam param) throws Exception {
        switch (param.getType()) {
            case OBJECT_T:
            case PSCO_T: // fetch stage already set the value on the param, but we make sure to collect the last version
                loadObject(param);
                break;
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
        DataRegister register;
        synchronized (registry) {
            register = registry.get(rename);
        }
        synchronized (register) {
            o = register.loadValue();
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

    private void storeObject(String rename, Object value) {
        DataRegister register;
        synchronized (registry) {
            register = registry.get(rename);
            if (register == null) {
                register = new DataRegister();
                registry.put(rename, register);
            }
        }
        synchronized (register) {
            register.setValue(value);
        }
    }

    @Override
    public void storeValue(String name, Object value) {
        storeObject(name, value);
    }

    @Override
    public void storeFile(String rename, String path) {
        DataRegister register;
        synchronized (registry) {
            register = registry.get(rename);
            if (register == null) {
                register = new DataRegister();
                registry.put(rename, register);
            }
        }
        synchronized (register) {
            register.addFileLocation(path);
        }
    }

    @Override
    public Object getObject(String dataMgmtId) {
        Object o = null;
        DataRegister register;
        synchronized (registry) {
            register = registry.get(dataMgmtId);
        }
        synchronized (register) {
            try {
                o = register.loadValue();
            } catch (IOException ex) {

            } catch (ClassNotFoundException ex) {

            } catch (DataRegister.NoSourcesException ex) {

            } catch (StorageException ex) {

            }
        }
        return o;
    }

    /*
     * ****************************************************************************************************************
     * STORE METHODS
     *****************************************************************************************************************/
    private void askForTransfer(boolean askTransfer, InvocationParam param, int index, LoadDataListener tt) {
        if (askTransfer) {
            transferParameter(param, index, tt);
        } else {
            fetchedLocalParameter(param, index, tt);
        }
    }

    private void fetchedLocalParameter(InvocationParam param, int index, LoadDataListener tt) {
        LOGGER.info("- Parameter " + index + "(" + (String) param.getValue() + ") already exists.");
        tt.loadedValue();
    }

    private void transferParameter(InvocationParam param, int index, LoadDataListener tt) {
        LOGGER.info("- Parameter " + index + "(" + (String) param.getValue() + ") does not exist, requesting data transfer");
        provider.askForTransfer(param, index, tt);
    }

}
