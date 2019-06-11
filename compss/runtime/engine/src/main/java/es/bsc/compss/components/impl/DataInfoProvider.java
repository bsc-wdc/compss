/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.components.impl;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.data.CollectionInfo;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataInfo;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.DataVersion;
import es.bsc.compss.types.data.FileInfo;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.ObjectInfo;
import es.bsc.compss.types.data.ResultFile;
import es.bsc.compss.types.data.StreamInfo;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.accessparams.AccessParams.AccessMode;
import es.bsc.compss.types.data.location.BindingObjectLocation;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.PersistentLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.BindingObjectTransferable;
import es.bsc.compss.types.data.operation.FileTransferable;
import es.bsc.compss.types.data.operation.ObjectTransferable;
import es.bsc.compss.types.data.operation.OneOpWithSemListener;
import es.bsc.compss.types.data.operation.ResultListener;
import es.bsc.compss.types.parameter.CollectionParameter;
import es.bsc.compss.types.request.ap.TransferBindingObjectRequest;
import es.bsc.compss.types.request.ap.TransferObjectRequest;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Serializer;
import es.bsc.compss.util.Tracer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StorageException;
import storage.StorageItf;


/**
 * Component to handle the specific data structures such as file names, versions, renamings and values.
 */
public class DataInfoProvider {

    // Constants definition
    private static final String RES_FILE_TRANSFER_ERR = "Error transferring result files";

    // Map: filename:host:path -> file identifier
    private TreeMap<String, Integer> nameToId;
    // Map: collectionName -> collection identifier
    private TreeMap<String, Integer> collectionToId;
    // Map: hash code -> object identifier
    private TreeMap<Integer, Integer> codeToId;
    // Map: file identifier -> file information
    private TreeMap<Integer, DataInfo> idToData;
    // Map: Object_Version_Renaming -> Object value
    private TreeMap<String, Object> renamingToValue; // TODO: Remove obsolete from here

    // Component logger - No need to configure, ProActive does
    private static final Logger LOGGER = LogManager.getLogger(Loggers.DIP_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();


    /**
     * New Data Info Provider instance.
     */
    public DataInfoProvider() {
        this.nameToId = new TreeMap<>();
        this.collectionToId = new TreeMap<>();
        this.codeToId = new TreeMap<>();
        this.idToData = new TreeMap<>();
        this.renamingToValue = new TreeMap<>();

        LOGGER.info("Initialization finished");
    }

    /**
     * DataAccess interface: registers a new data access.
     *
     * @param access Access Parameters.
     * @return The registered access Id.
     */
    public DataAccessId registerDataAccess(AccessParams access) {
        // The abstract method comes back to this class and executes the corresponding
        // registerFileAccess, registerObjectAccess, registerBindingObjectAccess or registerStreamAccess
        return access.register();
    }

    /**
     * DataAccess interface: registers a new file access.
     *
     * @param mode File Access Mode.
     * @param location File location.
     * @return The registered access Id.
     */
    public DataAccessId registerFileAccess(AccessMode mode, DataLocation location) {
        DataInfo fileInfo;
        String locationKey = location.getLocationKey();
        Integer fileId = this.nameToId.get(locationKey);

        // First access to this file
        if (fileId == null) {
            if (DEBUG) {
                LOGGER.debug("FIRST access to file " + location.getLocationKey());
            }

            // Update mappings
            fileInfo = new FileInfo(location);
            fileId = fileInfo.getDataId();
            this.nameToId.put(locationKey, fileId);
            this.idToData.put(fileId, fileInfo);

            // Register the initial location of the file
            if (mode != AccessMode.W) {
                DataInstanceId lastDID = fileInfo.getCurrentDataVersion().getDataInstanceId();
                String renaming = lastDID.getRenaming();
                Comm.registerLocation(renaming, location);
            }
        } else {
            // The file has already been accessed, all location are already registered
            if (DEBUG) {
                LOGGER.debug("Another access to file " + location.getLocationKey());
            }
            fileInfo = this.idToData.get(fileId);
        }

        // Version management
        return willAccess(mode, fileInfo);
    }

    /**
     * DataAccess interface: registers a new object access.
     *
     * @param mode Object access mode.
     * @param value Object value.
     * @param code Object hashcode.
     * @return The registered access Id.
     */
    public DataAccessId registerObjectAccess(AccessMode mode, Object value, int code) {
        DataInfo oInfo;
        Integer aoId = codeToId.get(code);

        // First access to this datum
        if (aoId == null) {
            if (DEBUG) {
                LOGGER.debug("FIRST access to object " + code);
            }

            // Update mappings
            oInfo = new ObjectInfo(code);
            aoId = oInfo.getDataId();
            this.codeToId.put(code, aoId);
            this.idToData.put(aoId, oInfo);

            // Serialize this first version of the object to a file
            DataInstanceId lastDID = oInfo.getCurrentDataVersion().getDataInstanceId();
            String renaming = lastDID.getRenaming();

            // Inform the File Transfer Manager about the new file containing the object
            if (mode != AccessMode.W) {
                Comm.registerValue(renaming, value);
            }
        } else {
            // The datum has already been accessed
            if (DEBUG) {
                LOGGER.debug("Another access to object " + code);
            }

            oInfo = this.idToData.get(aoId);
        }

        // Version management
        return willAccess(mode, oInfo);
    }

    /**
     * DataAccess interface: registers a new stream access.
     *
     * @param mode Stream access mode.
     * @param value Stream object value.
     * @param code Stream hashcode.
     * @return The registered access Id.
     */
    public DataAccessId registerStreamAccess(AccessMode mode, Object value, int code) {
        DataInfo oInfo;
        Integer aoId = this.codeToId.get(code);

        // First access to this datum
        if (aoId == null) {
            if (DEBUG) {
                LOGGER.debug("FIRST access to stream " + code);
            }

            // Update mappings
            oInfo = new StreamInfo(code);
            aoId = oInfo.getDataId();
            this.codeToId.put(code, aoId);
            this.idToData.put(aoId, oInfo);

            // Serialize this first version of the object to a file
            DataInstanceId lastDID = oInfo.getCurrentDataVersion().getDataInstanceId();
            String renaming = lastDID.getRenaming();

            // Inform the File Transfer Manager about the new file containing the object
            Comm.registerValue(renaming, value);
        } else {
            // The datum has already been accessed
            if (DEBUG) {
                LOGGER.debug("Another access to stream " + code);
            }

            oInfo = this.idToData.get(aoId);
        }

        // Version management
        return willAccess(mode, oInfo);
    }

    /**
     * DataAccess interface: registers a new file access.
     *
     * @param mode File Access Mode.
     * @param location File location.
     * @return The registered access Id.
     */
    public DataAccessId registerExternalStreamAccess(AccessMode mode, DataLocation location) {
        DataInfo externalStreamInfo;
        int locationKey = location.getLocationKey().hashCode();
        Integer externalStreamId = this.codeToId.get(locationKey);

        // First access to this file
        if (externalStreamId == null) {
            if (DEBUG) {
                LOGGER.debug("FIRST access to external stream " + locationKey);
            }

            // Update mappings
            externalStreamInfo = new StreamInfo(locationKey);
            externalStreamId = externalStreamInfo.getDataId();
            this.codeToId.put(locationKey, externalStreamId);
            this.idToData.put(externalStreamId, externalStreamInfo);

            // Register the initial location of the stream
            DataInstanceId lastDID = externalStreamInfo.getCurrentDataVersion().getDataInstanceId();
            String renaming = lastDID.getRenaming();
            Comm.registerLocation(renaming, location);
        } else {
            // The external stream has already been accessed, all location are already registered
            if (DEBUG) {
                LOGGER.debug("Another access to external stream " + locationKey);
            }
            externalStreamInfo = this.idToData.get(externalStreamId);
        }

        // Version management
        return willAccess(mode, externalStreamInfo);
    }

    /**
     * DataAccess interface: registers a new binding object access.
     *
     * @param mode Binding Object access mode.
     * @param bo Binding Object.
     * @param code Binding Object hashcode.
     * @return The registered access Id.
     */
    public DataAccessId registerBindingObjectAccess(AccessMode mode, BindingObject bo, int code) {
        DataInfo oInfo;

        Integer aoId = this.codeToId.get(code);

        // First access to this datum
        if (aoId == null) {
            if (DEBUG) {
                LOGGER.debug("FIRST access to external object " + code);
            }

            // Update mappings
            oInfo = new ObjectInfo(code);
            aoId = oInfo.getDataId();
            this.codeToId.put(code, aoId);
            this.idToData.put(aoId, oInfo);

            // Serialize this first version of the object to a file
            DataInstanceId lastDID = oInfo.getCurrentDataVersion().getDataInstanceId();
            String renaming = lastDID.getRenaming();

            // Inform the File Transfer Manager about the new file containing the object
            if (mode != AccessMode.W) {
                Comm.registerBindingObject(renaming, bo);
            }
        } else {
            // The datum has already been accessed
            if (DEBUG) {
                LOGGER.debug("Another access to external object " + code);
            }

            oInfo = this.idToData.get(aoId);
        }

        // Version management
        return willAccess(mode, oInfo);
    }

    /**
     * DataAccess interface: registers a new PSCO access.
     *
     * @param mode PSCO Access Mode.
     * @param pscoId PSCO Id.
     * @param code PSCO hashcode.
     * @return The registered access Id.
     */
    public DataAccessId registerExternalPSCOAccess(AccessMode mode, String pscoId, int code) {
        DataInfo oInfo;
        Integer aoId = this.codeToId.get(code);

        // First access to this datum
        if (aoId == null) {
            if (DEBUG) {
                LOGGER.debug("FIRST access to external object " + code);
            }

            // Update mappings
            oInfo = new ObjectInfo(code);
            aoId = oInfo.getDataId();
            this.codeToId.put(code, aoId);
            this.idToData.put(aoId, oInfo);

            // Serialize this first version of the object to a file
            DataInstanceId lastDID = oInfo.getCurrentDataVersion().getDataInstanceId();
            String renaming = lastDID.getRenaming();

            // Inform the File Transfer Manager about the new file containing the object
            if (mode != AccessMode.W) {
                Comm.registerExternalPSCO(renaming, pscoId);
            }
        } else {
            // The datum has already been accessed
            if (DEBUG) {
                LOGGER.debug("Another access to external object " + code);
            }

            oInfo = this.idToData.get(aoId);
        }

        // Version management
        return willAccess(mode, oInfo);
    }

    /**
     * Marks an access to a file as finished.
     * 
     * @param mode File Access Mode.
     * @param location File location.
     */
    public void finishFileAccess(AccessMode mode, DataLocation location) {
        DataInfo fileInfo;
        String locationKey = location.getLocationKey();
        Integer fileId = this.nameToId.get(locationKey);

        // First access to this file
        if (fileId == null) {
            LOGGER.warn("File " + location.getLocationKey() + " has not been accessed before");
            return;
        }
        fileInfo = this.idToData.get(fileId);
        DataAccessId daid = getAccess(mode, fileInfo);
        if (daid == null) {
            LOGGER.warn("File " + location.getLocationKey() + " has not been accessed before");
            return;
        }
        dataHasBeenAccessed(daid);

    }

    /**
     * Marks the access to a BindingObject as finished.
     * 
     * @param mode Binding Object Access Mode.
     * @param code Binding Object hashcode.
     */
    public void finishBindingObjectAccess(AccessMode mode, int code) {
        DataInfo boInfo;

        Integer aoId = this.codeToId.get(code);

        // First access to this file
        if (aoId == null) {
            LOGGER.warn("Binding Object " + code + " has not been accessed before");
            return;
        }
        boInfo = this.idToData.get(aoId);
        DataAccessId daid = getAccess(mode, boInfo);
        if (daid == null) {
            LOGGER.warn("Binding Object " + code + " has not been accessed before");
            return;
        }
        dataHasBeenAccessed(daid);

    }

    private DataAccessId willAccess(AccessMode mode, DataInfo di) {
        // Version management
        DataAccessId daId = null;
        switch (mode) {
            case C:
            case R:
                di.willBeRead();
                daId = new RAccessId(di.getCurrentDataVersion());
                if (DEBUG) {
                    StringBuilder sb = new StringBuilder("");
                    sb.append("Access:").append("\n");
                    sb.append("  * Type: R").append("\n");
                    sb.append("  * Read Datum: d").append(daId.getDataId()).append("v")
                            .append(((RAccessId) daId).getRVersionId()).append("\n");
                    LOGGER.debug(sb.toString());
                }
                break;

            case W:
                di.willBeWritten();
                daId = new WAccessId(di.getCurrentDataVersion());
                if (DEBUG) {
                    StringBuilder sb = new StringBuilder("");
                    sb.append("Access:").append("\n");
                    sb.append("  * Type: W").append("\n");
                    sb.append("  * Write Datum: d").append(daId.getDataId()).append("v")
                            .append(((WAccessId) daId).getWVersionId()).append("\n");
                    LOGGER.debug(sb.toString());
                }
                break;

            case RW:
                di.willBeRead();
                DataVersion readInstance = di.getCurrentDataVersion();
                di.willBeWritten();
                DataVersion writtenInstance = di.getCurrentDataVersion();
                daId = new RWAccessId(readInstance, writtenInstance);
                if (DEBUG) {
                    StringBuilder sb = new StringBuilder("");
                    sb.append("Access:").append("\n");
                    sb.append("  * Type: RW").append("\n");
                    sb.append("  * Read Datum: d").append(daId.getDataId()).append("v")
                            .append(((RWAccessId) daId).getRVersionId()).append("\n");
                    sb.append("  * Write Datum: d").append(daId.getDataId()).append("v")
                            .append(((RWAccessId) daId).getWVersionId()).append("\n");
                    LOGGER.debug(sb.toString());
                }
                break;
        }
        return daId;
    }

    private DataAccessId getAccess(AccessMode mode, DataInfo di) {
        // Version management
        DataAccessId daId = null;
        DataVersion currentInstance = di.getCurrentDataVersion();
        if (currentInstance != null) {
            switch (mode) {
                case C:
                case R:
                    daId = new RAccessId(currentInstance);
                    break;
                case W:
                    daId = new WAccessId(di.getCurrentDataVersion());
                    break;
                case RW:
                    DataVersion readInstance = di.getPreviousDataVersion();
                    if (readInstance != null) {
                        daId = new RWAccessId(readInstance, currentInstance);
                    } else {
                        LOGGER.warn("Previous instance for data" + di.getDataId() + " is null.");
                    }
                    break;
            }
        } else {
            LOGGER.warn("Current instance for data" + di.getDataId() + " is null.");
        }
        return daId;
    }

    /**
     * Removes the versions associated with the given DataAccessId {@code dAccId} to if the task was canceled or not.
     *
     * @param dAccId DataAccessId.
     */
    public void dataAccessHasBeenCanceled(DataAccessId dAccId) {
        Integer dataId = dAccId.getDataId();
        DataInfo di = this.idToData.get(dataId);
        Integer rVersionId;
        Integer wVersionId;
        boolean deleted = false;
        switch (dAccId.getDirection()) {
            case C:
            case R:
                rVersionId = ((RAccessId) dAccId).getReadDataInstance().getVersionId();
                deleted = di.versionHasBeenRead(rVersionId);
                break;
            case RW:
                rVersionId = ((RWAccessId) dAccId).getReadDataInstance().getVersionId();
                di.versionHasBeenRead(rVersionId);
                // read and write data version can be removed
                // di.canceledVersion(rVersionId);
                wVersionId = ((RWAccessId) dAccId).getWrittenDataInstance().getVersionId();
                di.canceledVersion(wVersionId);
                break;
            default:// case W:
                wVersionId = ((WAccessId) dAccId).getWrittenDataInstance().getVersionId();
                di.canceledVersion(wVersionId);
                break;
        }

        if (deleted) {
            // idToData.remove(dataId);
        }
    }

    /**
     * Marks that a given data {@code dAccId} has been accessed.
     *
     * @param dAccId DataAccessId.
     */
    public void dataHasBeenAccessed(DataAccessId dAccId) {
        Integer dataId = dAccId.getDataId();
        DataInfo di = this.idToData.get(dataId);
        Integer rVersionId;
        Integer wVersionId;
        boolean deleted = false;
        switch (dAccId.getDirection()) {
            case C:
            case R:
                rVersionId = ((RAccessId) dAccId).getReadDataInstance().getVersionId();
                deleted = di.versionHasBeenRead(rVersionId);
                break;
            case RW:
                rVersionId = ((RWAccessId) dAccId).getReadDataInstance().getVersionId();
                di.versionHasBeenRead(rVersionId);
                // read data version can be removed
                di.tryRemoveVersion(rVersionId);
                wVersionId = ((RWAccessId) dAccId).getWrittenDataInstance().getVersionId();
                deleted = di.versionHasBeenWritten(wVersionId);
                break;
            default:// case W:
                wVersionId = ((WAccessId) dAccId).getWrittenDataInstance().getVersionId();
                deleted = di.versionHasBeenWritten(wVersionId);
                break;
        }

        if (deleted) {
            // idToData.remove(dataId);
        }
    }

    /**
     * Returns whether a given location has been accessed or not.
     *
     * @param loc Location.
     * @return {@code true} if the location has been accessed, {@code false} otherwise.
     */
    public boolean alreadyAccessed(DataLocation loc) {
        LOGGER.debug("Check already accessed: " + loc.getLocationKey());
        String locationKey = loc.getLocationKey();
        Integer fileId = nameToId.get(locationKey);
        return fileId != null;
    }

    /**
     * DataInformation interface: returns the last renaming of a given data.
     *
     * @param code Object code.
     * @return Data renaming.
     */
    public String getLastRenaming(int code) {
        Integer aoId = this.codeToId.get(code);
        DataInfo oInfo = this.idToData.get(aoId);
        return oInfo.getCurrentDataVersion().getDataInstanceId().getRenaming();
    }

    /**
     * Returns the original location of a data id.
     *
     * @param fileId File Id.
     * @return Location of the original Data Id.
     */
    public DataLocation getOriginalLocation(int fileId) {
        FileInfo info = (FileInfo) this.idToData.get(fileId);
        return info.getOriginalLocation();
    }

    /**
     * Sets the value {@code value} to the renaming {@code renaming}.
     *
     * @param renaming Renaming.
     * @param value Object value.
     */
    public void setObjectVersionValue(String renaming, Object value) {
        this.renamingToValue.put(renaming, value);
        Comm.registerValue(renaming, value);
    }

    /**
     * Returns whether the dataInstanceId is registered in the master or not.
     *
     * @param dId Data Instance Id.
     * @return {@code true} if the renaming is registered in the master, {@code false} otherwise.
     */
    public boolean isHere(DataInstanceId dId) {
        return this.renamingToValue.get(dId.getRenaming()) != null;
    }

    /**
     * Returns the object associated to the given renaming {@code renaming}.
     *
     * @param renaming Object renaming.
     * @return Associated object value.
     */
    public Object getObject(String renaming) {
        return this.renamingToValue.get(renaming);
    }

    /**
     * Creates a new version with the same value.
     *
     * @param rRenaming Read renaming.
     * @param wRenaming Write renaming.
     */
    public void newVersionSameValue(String rRenaming, String wRenaming) {
        this.renamingToValue.put(wRenaming, this.renamingToValue.get(rRenaming));
    }

    /**
     * Returns the last data access to a given renaming.
     *
     * @param code Data code.
     * @return Data Instance Id with the last access.
     */
    public DataInstanceId getLastDataAccess(int code) {
        Integer aoId = this.codeToId.get(code);
        DataInfo oInfo = this.idToData.get(aoId);
        return oInfo.getCurrentDataVersion().getDataInstanceId();
    }

    /**
     * Returns the last version of all the specified data Ids {@code dataIds}.
     *
     * @param dataIds Data Ids.
     * @return A list of DataInstaceId containing the last version for each of the specified dataIds.
     */
    public List<DataInstanceId> getLastVersions(TreeSet<Integer> dataIds) {
        List<DataInstanceId> versionIds = new ArrayList<>(dataIds.size());
        for (Integer dataId : dataIds) {
            DataInfo dataInfo = this.idToData.get(dataId);
            if (dataInfo != null) {
                versionIds.add(dataInfo.getCurrentDataVersion().getDataInstanceId());
            } else {
                versionIds.add(null);
            }
        }
        return versionIds;
    }

    /**
     * Unblocks a dataId.
     *
     * @param dataId Data Id.
     */
    public void unblockDataId(Integer dataId) {
        DataInfo dataInfo = this.idToData.get(dataId);
        dataInfo.unblockDeletions();
    }

    /**
     * Waits until data is ready for its safe deletion.
     *
     * @param loc Data location.
     * @param semWait Waiting semaphore.
     * @return Number of permits.
     */
    public int waitForDataReadyToDelete(DataLocation loc, Semaphore semWait) {
        LOGGER.debug("Waiting for data to be ready for deletion: " + loc.getPath());
        String locationKey = loc.getLocationKey();

        Integer dataId = this.nameToId.get(locationKey);
        if (dataId == null) {
            LOGGER.debug("No data id found for this data location" + loc.getPath());
            semWait.release();
            return 0;
        }

        DataInfo dataInfo = this.idToData.get(dataId);
        int nPermits = dataInfo.waitForDataReadyToDelete(semWait);
        return nPermits;
    }

    /**
     * Gets the dataInfo of the location.
     * 
     * @param loc Location
     * @return DataInfo associated with the given location {@code loc}.
     */
    public DataInfo getLocationDataInfo(DataLocation loc) {
        String locationKey = loc.getLocationKey();
        if (this.nameToId.containsKey(locationKey)) {
            Integer dataId = this.nameToId.get(locationKey);
            DataInfo dataInfo = this.idToData.get(dataId);
            return dataInfo;
        }
        return null;
    }

    /**
     * Marks a data Id for deletion.
     *
     * @param loc Data location.
     * @return DataInfo associated with the given data.
     */
    public DataInfo deleteData(DataLocation loc) {
        LOGGER.debug("Deleting Data location: " + loc.getPath());
        String locationKey = loc.getLocationKey();
        Integer dataId = this.nameToId.get(locationKey);

        if (dataId == null) {
            LOGGER.debug("No data id found for this data location" + loc.getPath());
            return null;
        }

        DataInfo dataInfo = this.idToData.get(dataId);
        this.nameToId.remove(locationKey);
        if (dataInfo.delete()) {
            // idToData.remove(dataId);
        }
        return dataInfo;
    }

    /**
     * Deletes the data associated with the code.
     *
     * @param code Data code.
     * @return DataInfo associated with the given code.
     */
    public DataInfo deleteData(int code) {
        LOGGER.debug("Deleting Data associated with code: " + String.valueOf(code));

        Integer id = this.codeToId.get(code);
        DataInfo dataInfo = this.idToData.get(id);

        // We delete the data associated with all the versions of the same object
        dataInfo.delete();

        return dataInfo;
    }

    /**
     * Transfers the value of an object.
     *
     * @param toRequest Transfer object request.
     */
    public void transferObjectValue(TransferObjectRequest toRequest) {
        Semaphore sem = toRequest.getSemaphore();
        DataAccessId daId = toRequest.getDaId();
        RWAccessId rwaId = (RWAccessId) daId;
        String sourceName = rwaId.getReadDataInstance().getRenaming();
        // String targetName = rwaId.getWrittenDataInstance().getRenaming();
        if (DEBUG) {
            LOGGER.debug("Requesting getting object " + sourceName);
        }
        LogicalData ld = Comm.getData(sourceName);

        if (ld == null) {
            ErrorManager.error("Unregistered data " + sourceName);
            return;
        }

        if (ld.isInMemory()) {
            Object value = null;
            if (!rwaId.isPreserveSourceData()) {
                value = ld.getValue();
                // Clear value
                ld.removeValue();
            } else {
                try {
                    ld.writeToStorage();
                } catch (Exception e) {
                    ErrorManager.error("Exception writing object to file.", e);
                }
                for (DataLocation loc : ld.getLocations()) {
                    if (loc.getProtocol() != ProtocolType.OBJECT_URI) {
                        MultiURI mu = loc.getURIInHost(Comm.getAppHost());
                        String path = mu.getPath();
                        try {
                            value = Serializer.deserialize(path);
                            break;
                        } catch (IOException | ClassNotFoundException e) {
                            ErrorManager.error("Exception writing object to file.", e);
                        }
                    }
                }
            }
            // Set response
            toRequest.setResponse(value);
            toRequest.setTargetData(ld);
            sem.release();
        } else {
            if (DEBUG) {
                LOGGER.debug("Object " + sourceName + " not in memory. Requesting tranfers to "
                        + Comm.getAppHost().getName());
            }
            DataLocation targetLocation = null;
            String path = ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getTempDirPath() + sourceName;
            try {
                SimpleURI uri = new SimpleURI(path);
                targetLocation = DataLocation.createLocation(Comm.getAppHost(), uri);
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
            }
            toRequest.setTargetData(ld);
            Comm.getAppHost().getData(sourceName, targetLocation, new ObjectTransferable(),
                    new OneOpWithSemListener(sem));
        }

    }

    /**
     * Transfers the value of a binding object.
     *
     * @param toRequest Transfer binding object request.
     * @return Associated LogicalData to the obtained value.
     */
    public LogicalData transferBindingObject(TransferBindingObjectRequest toRequest) {
        DataAccessId daId = toRequest.getDaId();
        RAccessId rwaId = (RAccessId) daId;
        String sourceName = rwaId.getReadDataInstance().getRenaming();

        if (DEBUG) {
            LOGGER.debug("[DataInfoProvider] Requesting getting object " + sourceName);
        }
        LogicalData srcLd = Comm.getData(sourceName);
        if (DEBUG) {
            LOGGER.debug("[DataInfoProvider] Logical data for binding object is:" + srcLd);
        }
        if (srcLd == null) {
            ErrorManager.error("Unregistered data " + sourceName);
            return null;
        }
        if (DEBUG) {
            LOGGER.debug("Requesting tranfers binding object " + sourceName + " to " + Comm.getAppHost().getName());
        }

        Semaphore sem = toRequest.getSemaphore();
        BindingObject srcBO = BindingObject.generate(srcLd.getURIs().get(0).getPath());
        BindingObject tgtBO = new BindingObject(sourceName, srcBO.getType(), srcBO.getElements());
        LogicalData tgtLd = srcLd;
        DataLocation targetLocation = new BindingObjectLocation(Comm.getAppHost(), tgtBO);
        Transferable transfer = new BindingObjectTransferable(toRequest);

        Comm.getAppHost().getData(srcLd, targetLocation, tgtLd, transfer, new OneOpWithSemListener(sem));
        if (DEBUG) {
            LOGGER.debug(" Setting tgtName " + transfer.getDataTarget() + " in " + Comm.getAppHost().getName());
        }
        return srcLd;
    }

    /**
     * Blocks dataId and retrieves its result file.
     *
     * @param dataId Data Id.
     * @param listener Result listener.
     * @return The result file.
     */
    public ResultFile blockDataAndGetResultFile(int dataId, ResultListener listener) {
        DataInstanceId lastVersion;
        FileInfo fileInfo = (FileInfo) this.idToData.get(dataId);
        if (fileInfo.hasBeenCanceled()) {
            if (fileInfo != null && !fileInfo.isCurrentVersionToDelete()) { // If current version is to delete do not
                // transfer
                String[] splitPath = fileInfo.getOriginalLocation().getPath().split(File.separator);
                String origName = splitPath[splitPath.length - 1];
                if (origName.startsWith("compss-serialized-obj_")) {
                    // Do not transfer objects serialized by the bindings
                    if (DEBUG) {
                        LOGGER.debug("Discarding file " + origName + " as a result");
                    }
                    return null;
                }
                fileInfo.blockDeletions();

                lastVersion = fileInfo.getCurrentDataVersion().getDataInstanceId();

                ResultFile rf = new ResultFile(lastVersion, fileInfo.getOriginalLocation());

                DataInstanceId fId = rf.getFileInstanceId();
                String renaming = fId.getRenaming();

                // Look for the last available version
                while (renaming != null && !Comm.existsData(renaming)) {
                    renaming = DataInstanceId.previousVersionRenaming(renaming);
                }
                if (renaming == null) {
                    LOGGER.error(RES_FILE_TRANSFER_ERR + ": Cannot transfer file " + fId.getRenaming()
                            + " nor any of its previous versions");
                    return null;
                }

                // Check if data is a PSCO and must be consolidated
                for (DataLocation loc : Comm.getData(renaming).getLocations()) {
                    if (loc instanceof PersistentLocation) {
                        String pscoId = ((PersistentLocation) loc).getId();
                        if (Tracer.extraeEnabled()) {
                            Tracer.emitEvent(Tracer.Event.STORAGE_CONSOLIDATE.getId(),
                                    Tracer.Event.STORAGE_CONSOLIDATE.getType());
                        }
                        try {
                            StorageItf.consolidateVersion(pscoId);
                        } catch (StorageException e) {
                            LOGGER.error("Cannot consolidate PSCO " + pscoId, e);
                        } finally {
                            if (Tracer.extraeEnabled()) {
                                Tracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_CONSOLIDATE.getType());
                            }
                        }
                        LOGGER.debug("Returned because persistent object");
                        return rf;
                    }

                }

                // If no PSCO location is found, perform normal getData

                if (rf.getOriginalLocation().getProtocol() == ProtocolType.BINDING_URI) {
                    // Comm.getAppHost().getData(renaming, rf.getOriginalLocation(), new BindingObjectTransferable(),
                    // listener);
                    if (DEBUG) {
                        LOGGER.debug("Discarding data d" + dataId + " as a result beacuse it is a binding object");
                    }
                } else {
                    listener.addOperation();
                    Comm.getAppHost().getData(renaming, rf.getOriginalLocation(), new FileTransferable(), listener);
                }

                return rf;
            } else if (fileInfo != null && fileInfo.isCurrentVersionToDelete()) {
                if (DEBUG) {
                    String[] splitPath = fileInfo.getOriginalLocation().getPath().split(File.separator);
                    String origName = splitPath[splitPath.length - 1];
                    LOGGER.debug("Trying to delete file " + origName);
                }
                if (fileInfo.delete()) {
                    // idToData.remove(dataId);
                }
            }
        }
        return null;
    }

    /**
     * Shuts down the component.
     */
    public void shutdown() {
        // Nothing to do
    }

    /**
     * Registers the access to a collection.
     * 
     * @param am AccesMode.
     * @param cp CollectionParameter.
     * @return DataAccessId Representation of the access to the collection.
     */
    public DataAccessId registerCollectionAccess(AccessMode am, CollectionParameter cp) {
        String collectionId = cp.getCollectionId();
        Integer oId = this.collectionToId.get(collectionId);
        CollectionInfo cInfo;
        if (oId == null) {
            cInfo = new CollectionInfo(collectionId);
            oId = cInfo.getDataId();
            this.collectionToId.put(collectionId, oId);
            this.idToData.put(oId, cInfo);
            // Serialize this first version of the object to a file
            DataInstanceId lastDID = cInfo.getCurrentDataVersion().getDataInstanceId();
            String renaming = lastDID.getRenaming();
            // Inform the File Transfer Manager about the new file containing the object
            if (am != AccessMode.W) {
                LOGGER.debug(
                        "Collection " + cp.getCollectionId() + " contains " + cp.getParameters().size() + " accesses");
                // Null until the two-step transfer method is implemented
                Comm.registerCollection(renaming, null);
            }
        } else {
            cInfo = (CollectionInfo) this.idToData.get(oId);
        }
        return willAccess(am, cInfo);
    }
}
