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
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOURI;
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

    // Logger
    private static final Logger WORKER_LOGGER = LogManager.getLogger(Loggers.WORKER);
    private static final boolean WORKER_LOGGER_DEBUG = WORKER_LOGGER.isDebugEnabled();

    // Data Provider
    private final DataProvider provider;
    // String hostName
    private final String hostName;
    // Default data folder
    private final String baseFolder;
    // Storage configuration file
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
            WORKER_LOGGER.warn("No storage configuration file passed");
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
                WORKER_LOGGER.error("Error releasing storage library: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void removeObsoletes(List<String> obsoletes) {

        try {
            for (String name : obsoletes) {
                if (name.startsWith(File.separator)) {
                    WORKER_LOGGER.debug("Removing file " + name);
                    File f = new File(name);
                    if (!f.delete()) {
                        WORKER_LOGGER.error("Error removing file " + f.getAbsolutePath());
                    }
                    // Now only manage at C (python could do the same when cache available)
                    // if (COMPSsConstants.Lang.valueOf(lang.toUpperCase()) == COMPSsConstants.Lang.C && persistentC) {
                    // if (BindingDataManager.removeData(f.getName()) != 0) {
                    // LOGGER.error("Error removing data " + f.getName() + " from Binding");
                    // } else {
                    // LOGGER.debug("Data removed from cache " + f.getName());
                    // }
                    // }

                }
                String dataName = new File(name).getName();
                DataRegister register = null;
                synchronized (registry) {
                    register = registry.remove(dataName);
                    WORKER_LOGGER.debug(dataName + " removed from cache.");
                }
                if (register != null) {
                    synchronized (register) {
                        register.clear();
                    }
                }
                WORKER_LOGGER.debug(name + " removed from cache.");
            }
        } catch (Exception e) {
            WORKER_LOGGER.error("Exception", e);
        }
    }

    @Override
    public void fetchParam(InvocationParam param, int paramIdx, FetchDataListener tt) {
        switch (param.getType()) {
            case OBJECT_T:
                fetchObject(param, paramIdx, tt);
                break;
            case PSCO_T:
                fetchPSCO(param, paramIdx, tt);
                break;
            case BINDING_OBJECT_T:
                fetchBindingObject(param, paramIdx, tt);
                break;
            case FILE_T:
                fetchFile(param, paramIdx, tt);
                break;
            case EXTERNAL_PSCO_T:
                // Nothing to do since external parameters send their ID directly
                tt.fetchedValue();
                break;
            default:
            // Nothing to do since basic type parameters require no action
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
                                WORKER_LOGGER.error("WORKER IS NOT AWARE OF THE PRESENCE OF A"
                                        + (loc.getProtocol() == Protocol.OBJECT_URI ? "N OBJECT " : " BINDING OBJECT "));
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

    private void fetchObject(InvocationParam param, int index, FetchDataListener tt) {
        final String finalRename = param.getDataMgmtId();
        final String originalRename = param.getSourceDataId();
        WORKER_LOGGER.debug("   - " + finalRename + " registered as object.");
        DataRegister originalRegister = getOriginalDataRegister(param);
        // Try if parameter is in cache
        WORKER_LOGGER.debug("   - Checking if " + finalRename + " is in cache.");
        synchronized (originalRegister) {
            if (originalRegister.isLocal()) {
                if (finalRename.equals(originalRename)) {
                    try {
                        originalRegister.loadValue();
                    } catch (Exception e) {
                        WORKER_LOGGER.error(e);
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
                        WORKER_LOGGER.error(e);
                    }
                }
                fetchedLocalParameter(param, index, tt);
            } else {
                transferParameter(param, index, tt);
            }
        }
    }

    private void fetchBindingObject(InvocationParam param, int index, FetchDataListener tt) {
        if (WORKER_LOGGER_DEBUG) {
            WORKER_LOGGER.debug("   - " + param.getValue() + " registered as binding object.");
        }
        
        String name = (String) param.getValue();
        WORKER_LOGGER.debug("   - " + name + " registered as binding object.");

        BindingObject dest_bo = BindingObject.generate(name);
        String value = dest_bo.getName();
        int type = dest_bo.getType();
        int elements = dest_bo.getElements();


        boolean askTransfer = false;

        boolean cached = false;
        boolean locationsInCache = false;

        if (provider.isPersistentEnabled()) {

            // Try if parameter is in cache
            if (WORKER_LOGGER_DEBUG) {
                WORKER_LOGGER.debug("   - fetching Binding object for persistent worker.");
                WORKER_LOGGER.debug("   - Checking if " + value + " is in binding cache.");
            }

            cached = BindingDataManager.isInBinding(value);

            //If is not cached and the worker is persistent
            if (!cached) {
                if (WORKER_LOGGER_DEBUG) {
                    WORKER_LOGGER.debug("   - Checking if " + value + " locations are cached.");
                }

                for (InvocationParamURI loc : param.getSources()) {
                    BindingObject bo = BindingObject.generate(loc.getPath());
                    if (loc.isHost(hostName) && BindingDataManager.isInBinding(bo.getName())) {
                        //The value we want is not directly cached, but one of it sources is
                        if (WORKER_LOGGER_DEBUG) {
                            WORKER_LOGGER.debug("   - Parameter " + index + "(" + value + ") sources location found in cache.");
                        }

                        int res;
                        if (param.isPreserveSourceData()) {
                            if (WORKER_LOGGER_DEBUG) {
                                WORKER_LOGGER.debug("   - Parameter " + index + "(" + value + ") preserves sources. CACHE-COPYING");
                                WORKER_LOGGER.debug("   - Parameters to issue the copy are: " + bo.getName() + " and " + value);
                            }

                            res = BindingDataManager.copyCachedData(bo.getName(), value);
                            if (res != 0) {
                                WORKER_LOGGER.error("CACHE-COPY from " + bo.getName() + " to " + value + " has failed. ");
                                break;
                            }
                        }
                        else {
                            if (WORKER_LOGGER_DEBUG) {
                                WORKER_LOGGER.debug("   - Parameter " + index + "(" + value + ") overwrites sources. CACHE-MOVING");
                            }
                            res = BindingDataManager.moveCachedData(bo.getName(), value);
                            if (res != 0) {
                                WORKER_LOGGER.error("CACHE-MOVE from " + bo.getName() + " to " + value + " has failed. ");
                                break;
                            }
                        }
                        locationsInCache = true;
                        break;
                    }
                }
            }
        }
        else {
            if (WORKER_LOGGER_DEBUG) {
                WORKER_LOGGER.debug("   - fetching Binding object for NOT persistent worker.");
            }
        }

        // TODO is it better to transfer again or to load from file??
        boolean existInHost = false;
        if (!locationsInCache) {
            //The condition can be read as:
            //If persistent and value is not renamed in cache ---> surely is persistent and not renamed or is not persistent

            if (WORKER_LOGGER_DEBUG) {
                WORKER_LOGGER.debug("   - Checking if " + value + " is in host as file.");
            }

            for (InvocationParamURI loc : param.getSources()) {
                if (loc.isHost(hostName)) {
                    BindingObject bo = BindingObject.generate(loc.getPath());

                    if (WORKER_LOGGER_DEBUG) {
                        WORKER_LOGGER.debug("   - Parameter " + index + "(" + param.getValue() + ") found at host with location " + loc.getPath() + " Checking if id " + bo.getName() + " is in host...");
                    }

                    File inFile = new File(bo.getId());
                    if (!inFile.isAbsolute()) {
                        inFile = new File(baseFolder + File.separator + bo.getId());
                    }

                    String path = inFile.getAbsolutePath();
                    if (inFile.exists()) {
                        existInHost = true;

                        int res;
                        if (provider.isPersistentEnabled()) {
                            //Load data from file into cache
                            res = BindingDataManager.loadFromFile(value, path, type, elements);
                            if (res != 0) {
                                existInHost = false;
                                WORKER_LOGGER.error("   - Error loading " + value + " from file " + path);
                            }
                            else {
                                break;
                            }
                        }
                        else {
                            //Copy or move file
                            File outFile = new File(value);
                            try {
                                if (param.isPreserveSourceData()) {
                                    Files.copy(inFile.toPath(), outFile.toPath());
                                    break;
                                }
                                else {
                                    try {
                                        Files.move(inFile.toPath(), outFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
                                    }
                                    catch (AtomicMoveNotSupportedException amnse) {
                                        WORKER_LOGGER.warn(
                                                "   - AtomicMoveNotSupportedException. File cannot be atomically moved. Trying to move without atomic");
                                        Files.move(inFile.toPath(), outFile.toPath());
                                    }
                                    break;
                                }
                            }
                            catch (IOException e) {
                                WORKER_LOGGER.error("   - Error copying or moving " + bo.getName() + " to " + value);
                                WORKER_LOGGER.error(e);
                                existInHost = false;
                            }
                        }
                    }
                }
            }
        }

        if (!cached && !locationsInCache && !existInHost) {
            //Only if all three options failed, we must transfer the data
            if (WORKER_LOGGER_DEBUG) {
                WORKER_LOGGER.debug("   - The state of " + value + " in the worker is : \n"
                                        + "\t Cached: " + cached                            + "\n"
                                        + "\t Renamed in cache:     "   + locationsInCache  + "\n"
                                        + "\t In host as a file:    "   + existInHost);
                WORKER_LOGGER.debug("   - Not possible to fetch     "   + value + " in the current node, requesting for transfer.");
            }

            askTransfer = true;
        }

        //Request the transfer if needed
        askForTransfer(askTransfer, param, index, tt);

    }

    private void fetchPSCO(InvocationParam param, int paramIdx, FetchDataListener tt) {
        String finalRename = param.getDataMgmtId();
        String pscoId = (String) param.getValue();
        WORKER_LOGGER.debug("   - " + pscoId + " registered as PSCO.");
        // The replica must have been ordered by the master so the real object must be
        // catched or can be retrieved by the ID

        DataRegister dr = new DataRegister();
        dr.setStorageId(pscoId);
        registry.put(finalRename, dr);
        tt.fetchedValue();
    }

    private void fetchFile(InvocationParam param, int index, FetchDataListener tt) {
        WORKER_LOGGER.debug("   - " + (String) param.getValue() + " registered as file.");
        final String originalName = param.getSourceDataId();
        final String expectedFileLocation = param.getValue().toString();
        WORKER_LOGGER.debug("   - Checking if file " + (String) param.getValue() + " exists.");
        File f = new File(expectedFileLocation);
        if (f.exists()) {
            WORKER_LOGGER.info("- Parameter " + index + "(" + expectedFileLocation + ") already exists.");
            fetchedLocalParameter(param, index, tt);
            return;
        }
        WORKER_LOGGER.debug("   - Checking if " + expectedFileLocation + " exists in worker");
        DataRegister originalRegister = getOriginalDataRegister(param);
        synchronized (originalRegister) {
            if (originalRegister.isLocal()) {
                WORKER_LOGGER.debug("   - Parameter " + index + "(" + expectedFileLocation + ") found at host.");

                File target = new File(expectedFileLocation);
                List<String> files = originalRegister.getFileLocations();
                for (String path : files) {
                    File source = new File(path);
                    try {

                        WORKER_LOGGER.debug("   - Parameter " + index + "(" + expectedFileLocation + ") "
                                + (param.isPreserveSourceData()
                                        ? "preserves sources. COPYING"
                                        : "erases sources. MOVING")
                        );
                        WORKER_LOGGER.debug("         Source: " + source);
                        WORKER_LOGGER.debug("         Target: " + target);

                        if (param.isPreserveSourceData()) {
                            Files.copy(source.toPath(), target.toPath());
                        } else {
                            try {
                                Files.move(source.toPath(), target.toPath(), StandardCopyOption.ATOMIC_MOVE);
                            } catch (AtomicMoveNotSupportedException amnse) {
                                WORKER_LOGGER.warn(
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
                        WORKER_LOGGER.error("IOException", ioe);
                    }
                }
            } else {
                WORKER_LOGGER.info("- Parameter " + index + "(" + expectedFileLocation + ") does not exist, requesting data transfer");
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
            // Nothing to do since basic type parameters require no action
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
                // storePSCO(param);
                break;
            case BINDING_OBJECT_T:
                // Already stored on the binding
                break;
            case FILE_T:
                // Already stored
                break;
            default:
                // For char, strings and others. Nothing to do
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
    private void askForTransfer(boolean askTransfer, InvocationParam param, int index, FetchDataListener tt) {
        if (askTransfer) {
            transferParameter(param, index, tt);
        } else {
            fetchedLocalParameter(param, index, tt);
        }
    }

    private void fetchedLocalParameter(InvocationParam param, int index, FetchDataListener tt) {
        WORKER_LOGGER.info("- Parameter " + index + "(" + (String) param.getValue() + ") already exists.");
        tt.fetchedValue();
    }

    private void transferParameter(InvocationParam param, int index, FetchDataListener tt) {
        WORKER_LOGGER.info("- Parameter " + index + "(" + (String) param.getValue() + ") does not exist, requesting data transfer");
        provider.askForTransfer(param, index, tt);
    }

}
