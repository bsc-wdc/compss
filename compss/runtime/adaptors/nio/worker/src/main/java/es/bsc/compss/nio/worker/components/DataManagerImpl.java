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
package es.bsc.compss.nio.worker.components;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.data.BindingDataManager;
import es.bsc.compss.data.DataManager;
import es.bsc.compss.data.DataProvider;
import es.bsc.compss.data.FetchDataListener;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOParamCollection;
import es.bsc.compss.nio.exceptions.NoSourcesException;
import es.bsc.compss.nio.listeners.CollectionFetchOperationsListener;
import es.bsc.compss.nio.listeners.DictCollectionFetchOperationsListener;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamURI;
import es.bsc.compss.types.execution.exceptions.InitializationException;
import es.bsc.compss.types.execution.exceptions.UnloadableValueException;
import es.bsc.compss.util.FileOpsManager;
import es.bsc.distrostreamlib.client.DistroStreamClient;
import es.bsc.distrostreamlib.exceptions.DistroStreamClientInitException;
import es.bsc.distrostreamlib.requests.StopRequest;
import es.bsc.distrostreamlib.server.types.StreamBackend;

import java.io.File;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StorageException;
import storage.StorageItf;


public class DataManagerImpl implements DataManager {

    // Logger
    private static final Logger WORKER_LOGGER = LogManager.getLogger(Loggers.WORKER);
    private static final boolean WORKER_LOGGER_DEBUG = WORKER_LOGGER.isDebugEnabled();

    // Streaming
    private static final StreamBackend STREAMING_BACKEND;

    // Storage
    private static final String STORAGE_CONF;

    // String hostName
    private final String hostName;
    private final String masterName;
    private final int streamingPort;

    // Data Provider
    private final DataProvider provider;
    // Default data folder
    private final String baseFolder;
    // Data registry
    private final HashMap<String, DataRegister> registry;

    static {
        String streamBackendProperty = System.getProperty(COMPSsConstants.STREAMING_BACKEND);
        String streamBackendPropertyFixed =
            (streamBackendProperty == null || streamBackendProperty.isEmpty() || streamBackendProperty.equals("null"))
                ? "NONE"
                : streamBackendProperty.toUpperCase();
        STREAMING_BACKEND = StreamBackend.valueOf(streamBackendPropertyFixed);

        String storageCfgProperty = System.getProperty(COMPSsConstants.STORAGE_CONF);
        STORAGE_CONF =
            (storageCfgProperty == null || storageCfgProperty.isEmpty() || storageCfgProperty.equals("null")) ? null
                : storageCfgProperty;
    }


    /**
     * Instantiates a new Data Manager.
     *
     * @param hostName Worker hostname.
     * @param masterName Master hostname.
     * @param streamingPort Streaming port to communicate with the master.
     * @param baseFolder Base worker working directory.
     * @param provider Data provider.
     */
    public DataManagerImpl(String hostName, String masterName, int streamingPort, String baseFolder,
        DataProvider provider) {

        this.hostName = hostName;
        this.masterName = masterName;
        this.streamingPort = streamingPort;
        this.baseFolder = baseFolder;
        this.provider = provider;
        this.registry = new HashMap<>();
    }

    @Override
    public void init() throws InitializationException {
        // Start streaming library
        if (STREAMING_BACKEND.equals(StreamBackend.NONE)) {
            WORKER_LOGGER.warn("No streaming backend passed");
        } else {
            if (WORKER_LOGGER_DEBUG) {
                WORKER_LOGGER.debug("Initializing Streaming Client for " + this.masterName + ":" + this.streamingPort);
            }
            try {
                DistroStreamClient.initAndStart(this.masterName, this.streamingPort);
            } catch (DistroStreamClientInitException dscie) {
                throw new InitializationException("Error initializing DS Client", dscie);
            }
        }

        // Start storage interface
        if (STORAGE_CONF == null) {
            WORKER_LOGGER.warn("No storage configuration file passed");
        } else {
            WORKER_LOGGER.debug("Initializing Storage with: " + STORAGE_CONF);
            try {
                StorageItf.init(STORAGE_CONF);
            } catch (StorageException se) {
                throw new InitializationException("Error loading storage configuration file: " + STORAGE_CONF, se);
            }
        }
    }

    @Override
    public String getStorageConf() {
        return STORAGE_CONF;
    }

    @Override
    public StreamBackend getStreamingBackend() {
        return STREAMING_BACKEND;
    }

    @Override
    public String getStreamingMasterName() {
        return this.masterName;
    }

    @Override
    public int getStreamingMasterPort() {
        return this.streamingPort;
    }

    @Override
    public void stop() {
        // End Streaming
        if (!STREAMING_BACKEND.equals(StreamBackend.NONE)) {
            WORKER_LOGGER.debug("Stopping Streaming Client...");
            StopRequest stopRequest = new StopRequest();
            DistroStreamClient.request(stopRequest);
            stopRequest.waitProcessed();
            int errorCode = stopRequest.getErrorCode();
            if (errorCode != 0) {
                WORKER_LOGGER.error("Error stopping Streaming Client");
                WORKER_LOGGER.error("Error Code: " + errorCode);
                WORKER_LOGGER.error("Error Message: " + stopRequest.getErrorMessage());
            }
        }

        // End storage
        if (STORAGE_CONF != null) {
            WORKER_LOGGER.debug("Stopping Storage...");
            try {
                StorageItf.finish();
            } catch (StorageException e) {
                WORKER_LOGGER.error("Error releasing storage library: " + e.getMessage(), e);
            }
        }
        FileOpsManager.shutdown();
    }

    @Override
    public void removeObsoletes(List<String> obsoletes) {
        try {
            for (String name : obsoletes) {
                if (WORKER_LOGGER_DEBUG) {
                    WORKER_LOGGER.debug("Removing obsolete " + name);
                }
                final File f = new File(name);
                /*
                 * if (name.startsWith(File.separator)) { FileOpsManager.deleteAsync(f); }
                 */
                String dataName = f.getName();
                DataRegister register = null;
                synchronized (registry) {
                    register = this.registry.remove(dataName);
                }
                if (register != null) {
                    synchronized (register) {
                        register.clear();
                    }
                }
                if (WORKER_LOGGER_DEBUG) {
                    WORKER_LOGGER.debug(name + " removed from cache.");
                }
            }
        } catch (Exception e) {
            WORKER_LOGGER.error("Exception", e);
        }
        WORKER_LOGGER.debug("Remove obsoletes finished");
    }

    @Override
    public void fetchParam(InvocationParam param, int paramIdx, FetchDataListener tt) {
        switch (param.getType()) {
            case COLLECTION_T:
            case DICT_COLLECTION_T:
                fetchCollection(param, paramIdx, tt);
                break;
            case OBJECT_T:
            case STREAM_T:
                fetchObject(param, paramIdx, tt);
                break;
            case PSCO_T:
                fetchPSCO(param, paramIdx, tt);
                break;
            case BINDING_OBJECT_T:
                fetchBindingObject(param, paramIdx, tt);
                break;
            case DIRECTORY_T:
            case FILE_T:
            case EXTERNAL_STREAM_T:
                fetchFile(param, paramIdx, tt);
                break;
            case EXTERNAL_PSCO_T:
                // Nothing to do since external parameters send their ID directly
                tt.fetchedValue(param.getDataMgmtId());
                break;
            default:
                // Nothing to do since basic type parameters require no action
                tt.fetchedValue(param.getDataMgmtId());
                break;
        }
    }

    private DataRegister getOriginalDataRegister(InvocationParam param) {
        String originalRename = param.getSourceDataId();
        DataRegister originalRegister;
        synchronized (registry) {
            originalRegister = this.registry.get(originalRename);
            if (originalRegister == null) {
                originalRegister = new DataRegister();
                this.registry.put(originalRename, originalRegister);
            }
        }

        WORKER_LOGGER.debug("Checking locations for " + originalRename);
        for (InvocationParamURI loc : param.getSources()) {
            switch (loc.getProtocol()) {
                case DIR_URI:
                case FILE_URI:
                    if (loc.isHost(this.hostName)) {
                        synchronized (originalRegister) {
                            originalRegister.addFileLocation(loc.getPath());
                        }
                    }
                    break;
                case PERSISTENT_URI:
                    String pscoId = loc.getPath();
                    synchronized (originalRegister) {
                        originalRegister.setStorageId(pscoId);
                    }
                    break;
                case OBJECT_URI:
                case BINDING_URI:
                    if (loc.isHost(this.hostName)) {
                        WORKER_LOGGER.error("WORKER IS NOT AWARE OF THE PRESENCE OF A"
                            + (loc.getProtocol() == ProtocolType.OBJECT_URI ? "N OBJECT " : " BINDING OBJECT "));
                    }
                    break;
                case SHARED_URI:
                    break;

                default:
            }
        }

        return originalRegister;
    }

    private void fetchCollection(InvocationParam param, int index, FetchDataListener listener) {
        try {
            NIOParamCollection npc = (NIOParamCollection) param;
            List<NIOParam> elements = npc.getCollectionParameters();
            WORKER_LOGGER.info("Checking NIOParamCollection (received " + elements.size() + " params)");
            int subIndex = 0;
            CollectionFetchOperationsListener cfol =
                new CollectionFetchOperationsListener(param.getDataMgmtId(), listener);
            for (NIOParam subNioParam : npc.getCollectionParameters()) {
                cfol.addOperation();
                fetchParam(subNioParam, subIndex, cfol);
                subIndex++;
            }
            cfol.enable();
        } catch (Exception e) {
            listener.errorFetchingValue(param.getDataMgmtId(), e);
        }
    }

    private void fetchObject(InvocationParam param, int index, FetchDataListener listener) {
        final String finalRename = param.getDataMgmtId();
        final String originalRename = param.getSourceDataId();
        WORKER_LOGGER.debug("   - " + finalRename + " registered as object.");
        DataRegister originalRegister = getOriginalDataRegister(param);

        // Try if parameter is in cache
        WORKER_LOGGER.debug("   - Checking if " + finalRename + " is in cache.");
        if (originalRegister.isLocal()) {
            if (finalRename.equals(originalRename)) {
                try {
                    synchronized (originalRegister) {
                        originalRegister.loadValue();
                    }
                } catch (Exception e) {
                    WORKER_LOGGER.error(e);
                    listener.errorFetchingValue(param.getDataMgmtId(), e);
                }
            } else {
                try {
                    Object o;
                    if (param.isPreserveSourceData()) {
                        synchronized (originalRegister) {
                            o = originalRegister.cloneValue();
                        }
                    } else {
                        synchronized (originalRegister) {
                            o = originalRegister.loadValue();
                            originalRegister.removeValue();
                        }
                    }
                    DataRegister dr = new DataRegister();
                    dr.setValue(o);
                    synchronized (this.registry) {
                        this.registry.put(finalRename, dr);
                    }
                } catch (Exception e) {
                    WORKER_LOGGER.error(e);
                    listener.errorFetchingValue(param.getDataMgmtId(), e);
                }
            }
            fetchedLocalParameter(param, index, listener);
        } else {
            transferParameter(param, index, listener);
        }
    }

    private void fetchBindingObject(InvocationParam param, int index, FetchDataListener tt) {
        if (WORKER_LOGGER_DEBUG) {
            WORKER_LOGGER.debug("   - " + param.getValue() + " registered as binding object.");
        }

        String name = (String) param.getValue();
        WORKER_LOGGER.debug("   - " + name + " registered as binding object.");

        BindingObject destBo = BindingObject.generate(name);
        String value = destBo.getName();
        int type = destBo.getType();
        int elements = destBo.getElements();

        boolean askTransfer = false;

        boolean cached = false;
        boolean locationsInCache = false;

        if (this.provider.isPersistentCEnabled()) {
            // Try if parameter is in cache
            if (WORKER_LOGGER_DEBUG) {
                WORKER_LOGGER.debug("   - fetching Binding object for persistent worker.");
                WORKER_LOGGER.debug("   - Checking if " + value + " is in binding cache.");
            }

            cached = BindingDataManager.isInBinding(value);
            // If is not cached and the worker is persistent
            if (!cached) {
                if (WORKER_LOGGER_DEBUG) {
                    WORKER_LOGGER.debug("   - Checking if " + value + " locations are cached.");
                }

                for (InvocationParamURI loc : param.getSources()) {
                    BindingObject bo = BindingObject.generate(loc.getPath());
                    if (loc.isHost(this.hostName) && BindingDataManager.isInBinding(bo.getName())) {
                        // The value we want is not directly cached, but one of it sources is
                        if (WORKER_LOGGER_DEBUG) {
                            WORKER_LOGGER
                                .debug("   - Parameter " + index + "(" + value + ") sources location found in cache.");
                        }
                        if (param.isPreserveSourceData()) {
                            if (WORKER_LOGGER_DEBUG) {
                                WORKER_LOGGER.debug(
                                    "   - Parameter " + index + "(" + value + ") preserves sources. CACHE-COPYING");
                                WORKER_LOGGER
                                    .debug("   - Parameters to issue the copy are: " + bo.getName() + " and " + value);
                            }
                            int res = BindingDataManager.copyCachedData(bo.getName(), value);
                            if (res != 0) {
                                WORKER_LOGGER
                                    .error("CACHE-COPY from " + bo.getName() + " to " + value + " has failed. ");
                                break;
                            }
                        } else {
                            if (WORKER_LOGGER_DEBUG) {
                                WORKER_LOGGER.debug(
                                    "   - Parameter " + index + "(" + value + ") overwrites sources. CACHE-MOVING");
                            }
                            int res = BindingDataManager.moveCachedData(bo.getName(), value);
                            if (res != 0) {
                                WORKER_LOGGER
                                    .error("CACHE-MOVE from " + bo.getName() + " to " + value + " has failed. ");
                                break;
                            }
                        }
                        locationsInCache = true;
                        break;
                    }
                }
            }
        } else {
            if (WORKER_LOGGER_DEBUG) {
                WORKER_LOGGER.debug("   - fetching Binding object for NOT persistent worker.");
            }
        }

        // TODO is it better to transfer again or to load from file??
        boolean existInHost = false;
        if (!locationsInCache) {
            // The condition can be read as:
            // If persistent and value is not renamed in cache ---> surely is persistent and not renamed or is not
            // persistent

            if (WORKER_LOGGER_DEBUG) {
                WORKER_LOGGER.debug("   - Checking if " + value + " is in host as file.");
            }

            for (InvocationParamURI loc : param.getSources()) {
                if (loc.isHost(this.hostName)) {
                    BindingObject bo = BindingObject.generate(loc.getPath());

                    if (WORKER_LOGGER_DEBUG) {
                        WORKER_LOGGER
                            .debug("   - Parameter " + index + "(" + param.getValue() + ") found at host with location "
                                + loc.getPath() + " Checking if id " + bo.getName() + " is in host...");
                    }

                    File inFile = new File(bo.getId());
                    if (!inFile.isAbsolute()) {
                        inFile = new File(this.baseFolder + File.separator + bo.getId());
                    }

                    String path = inFile.getAbsolutePath();
                    if (inFile.exists()) {
                        existInHost = true;

                        int res;
                        if (provider.isPersistentCEnabled()) {
                            // Load data from file into cache
                            res = BindingDataManager.loadFromFile(value, path, type, elements);
                            if (res != 0) {
                                existInHost = false;
                                WORKER_LOGGER.error("   - Error loading " + value + " from file " + path);
                            } else {
                                break;
                            }
                        } else {
                            // Copy or move file
                            File outFile = new File(value);
                            try {
                                if (param.isPreserveSourceData()) {
                                    Files.copy(inFile.toPath(), outFile.toPath());
                                    break;
                                } else {
                                    try {
                                        Files.move(inFile.toPath(), outFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
                                    } catch (AtomicMoveNotSupportedException amnse) {
                                        WORKER_LOGGER.warn("   - AtomicMoveNotSupportedException."
                                            + "File cannot be atomically moved." + " Trying to move without atomic");
                                        Files.move(inFile.toPath(), outFile.toPath());
                                    }
                                    break;
                                }
                            } catch (IOException e) {
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
            // Only if all three options failed, we must transfer the data
            if (WORKER_LOGGER_DEBUG) {
                WORKER_LOGGER.debug("   - The state of " + value + " in the worker is : \n" + "\t Cached: " + cached
                    + "\n" + "\t Renamed in cache:     " + locationsInCache + "\n" + "\t In host as a file:    "
                    + existInHost);
                WORKER_LOGGER.debug(
                    "   - Not possible to fetch     " + value + " in the current node, requesting for transfer.");
            }

            askTransfer = true;
        }

        // Request the transfer if needed
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
        synchronized (this.registry) {
            registry.put(finalRename, dr);
        }
        tt.fetchedValue(param.getDataMgmtId());
    }

    private void fetchFile(InvocationParam param, int index, FetchDataListener tt) {
        WORKER_LOGGER.debug("   - " + (String) param.getValue() + " registered as file.");
        final String originalName = param.getSourceDataId();
        final String finalName = param.getDataMgmtId();
        final String expectedFileLocation = param.getValue().toString();
        WORKER_LOGGER.debug("   - Checking if file " + (String) param.getValue() + " is being transferred.");
        if (provider.isTransferingData(param)) {
            WORKER_LOGGER.debug("- Parameter " + index + "(" + expectedFileLocation + ") is being trasferred.");
            // add to data resquest
            provider.askForTransfer(param, index, tt);
            return;
        }
        WORKER_LOGGER.debug("   - Checking if " + expectedFileLocation + " exists in worker");
        DataRegister originalRegister = getOriginalDataRegister(param);
        if (originalRegister.isLocal()) {
            WORKER_LOGGER.debug("   - Parameter " + index + "(" + expectedFileLocation + ") found at host.");

            Path tgtPath = Paths.get(expectedFileLocation);
            Set<String> files;
            final String[] filesArray;
            synchronized (originalRegister) {
                files = originalRegister.getFileLocations();
                if (files.contains(expectedFileLocation)) {
                    fetchedLocalParameter(param, index, tt);
                    return;
                }
                filesArray = files.toArray(new String[files.size()]);
            }

            FileOpsManager.composedOperationAsync(new Runnable() {

                @Override
                public void run() {
                    for (String path : filesArray) {
                        if (!expectedFileLocation.equals(path)) {
                            Path srcPath = Paths.get(path);
                            try {
                                if (WORKER_LOGGER_DEBUG) {
                                    WORKER_LOGGER.debug("   - Parameter " + index + "(" + expectedFileLocation + ") "
                                        + (param.isPreserveSourceData() ? "preserves sources. COPYING"
                                            : "erases sources. MOVING"));
                                    WORKER_LOGGER.debug("         Source: " + srcPath);// source);
                                    WORKER_LOGGER.debug("         Target: " + tgtPath);// target);
                                }

                                if (param.isPreserveSourceData()) {
                                    if (param.getType() == DataType.DIRECTORY_T) {
                                        FileOpsManager.copyDirSync(new File(path), new File(expectedFileLocation));
                                    } else {
                                        FileOpsManager.copySync(new File(path), new File(expectedFileLocation));
                                    }
                                } else {
                                    if (param.getType() == DataType.DIRECTORY_T) {
                                        FileOpsManager.moveDirSync(new File(path), new File(expectedFileLocation));
                                    } else {
                                        FileOpsManager.moveSync(new File(path), new File(expectedFileLocation));
                                    }
                                    originalRegister.removeFileLocation(path);
                                }
                                DataRegister dr = new DataRegister();
                                dr.addFileLocation(tgtPath.toString());
                                synchronized (registry) {
                                    registry.put(finalName, dr);
                                }
                                fetchedLocalParameter(param, index, tt);
                                return;
                            } catch (FileAlreadyExistsException e) {
                                WORKER_LOGGER
                                    .warn("WARN: File " + expectedFileLocation + " already exists avoiding copy");
                                WORKER_LOGGER.warn(" Registered locations for " + originalName + " :");
                                for (String file : files) {
                                    WORKER_LOGGER.warn(" * " + file);
                                }
                                fetchedLocalParameter(param, index, tt);
                                return;
                            } catch (IOException ioe) {
                                WORKER_LOGGER.error("IOException", ioe);
                                tt.errorFetchingValue(param.getDataMgmtId(), ioe);
                            }
                        } else {
                            fetchedLocalParameter(param, index, tt);
                            return;
                        }
                    }
                }
            });
        } else {
            WORKER_LOGGER.info(
                "- Parameter " + index + "(" + expectedFileLocation + ") does not exist, requesting data transfer");
            transferParameter(param, index, tt);
        }
    }

    @Override
    public void loadParam(InvocationParam param) throws UnloadableValueException {
        if (WORKER_LOGGER_DEBUG) {
            WORKER_LOGGER.debug("[Thread " + Thread.currentThread().getName() + " ] Loading parameter: " + param);
        }

        switch (param.getType()) {
            case OBJECT_T:
            case STREAM_T:
            case PSCO_T:
                // Fetch stage already set the value on the param, but we make sure to collect the last version
                loadObject(param);
                break;
            case COLLECTION_T: // value corresponds to the collection ID
            case DICT_COLLECTION_T: // value corresponds to the dictionary collection ID
            case FILE_T: // value already contains the path
            case EXTERNAL_STREAM_T: // value already contains the path
            case BINDING_OBJECT_T: // value corresponds to the ID of the object on the binding (already set)
            case EXTERNAL_PSCO_T: // value corresponds to the ID of the
                break;
            default:
                // Nothing to do since basic type parameters require no action
                break;
        }
    }

    private void loadObject(InvocationParam param) throws UnloadableValueException {
        String rename = param.getDataMgmtId();
        Object o = null;
        DataRegister register;
        synchronized (registry) {
            register = this.registry.get(rename);
        }
        if (WORKER_LOGGER_DEBUG) {
            WORKER_LOGGER.debug("[Thread " + Thread.currentThread().getName() + " ] Loading value: " + rename);
        }
        synchronized (register) {
            try {
                o = register.loadValue();
            } catch (ClassNotFoundException | IOException | NoSourcesException | StorageException e) {
                throw new UnloadableValueException(e);
            }
        }
        param.setValue(o);
    }

    @Override
    public void storeParam(InvocationParam param) {
        switch (param.getType()) {
            case OBJECT_T:
            case STREAM_T:
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
            case EXTERNAL_STREAM_T:
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
            register = this.registry.get(rename);
            if (register == null) {
                register = new DataRegister();
                this.registry.put(rename, register);
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
            register = this.registry.get(rename);
            if (register == null) {
                register = new DataRegister();
                this.registry.put(rename, register);
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
            register = this.registry.get(dataMgmtId);
        }
        try {
            synchronized (register) {
                o = register.loadValue();
            }
        } catch (IOException ioe) {
            WORKER_LOGGER.error("Error loading value", ioe);
        } catch (ClassNotFoundException cnfe) {
            WORKER_LOGGER.error("Error loading value", cnfe);
        } catch (NoSourcesException nse) {
            WORKER_LOGGER.error("Error loading value", nse);
        } catch (StorageException se) {
            WORKER_LOGGER.error("Error loading value", se);
        }
        return o;
    }

    /*
     * ****************************************************************************************************************
     * STORE METHODS
     ****************************************************************************************************************
     */
    private void askForTransfer(boolean askTransfer, InvocationParam param, int index, FetchDataListener tt) {
        if (askTransfer) {
            transferParameter(param, index, tt);
        } else {
            fetchedLocalParameter(param, index, tt);
        }
    }

    private void fetchedLocalParameter(InvocationParam param, int index, FetchDataListener tt) {
        WORKER_LOGGER.info("- Parameter " + index + "(" + (String) param.getValue() + ") already exists.");
        tt.fetchedValue(param.getDataMgmtId());
    }

    private void transferParameter(InvocationParam param, int index, FetchDataListener tt) {
        WORKER_LOGGER.info(
            "- Parameter " + index + "(" + (String) param.getValue() + ") does not exist, requesting data transfer");
        this.provider.askForTransfer(param, index, tt);
    }

}
