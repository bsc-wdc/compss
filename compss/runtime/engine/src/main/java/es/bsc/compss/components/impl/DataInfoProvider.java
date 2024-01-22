/*
 *  Copyright 2002-2023 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.exceptions.CommException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.DataVersion;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.ResultFile;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.accessparams.AccessParams.AccessMode;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.FileInfo;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.PersistentLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.DirectoryTransferable;
import es.bsc.compss.types.data.operation.FileTransferable;
import es.bsc.compss.types.data.operation.ResultListener;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.request.exceptions.NonExistingValueException;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;

import java.io.File;
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

    // Map: file identifier -> file information
    private TreeMap<Integer, DataInfo> idToData;
    // Set: Object values available for main code
    private TreeSet<String> valuesOnMain; // TODO: Remove obsolete from here

    // Component logger - No need to configure, ProActive does
    private static final Logger LOGGER = LogManager.getLogger(Loggers.DIP_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();


    /**
     * New Data Info Provider instance.
     */
    public DataInfoProvider() {
        this.idToData = new TreeMap<>();
        this.valuesOnMain = new TreeSet<>();

        LOGGER.info("Initialization finished");
    }

    private DataInfo registerData(DataParams data) {
        DataInfo dInfo = data.createDataInfo();
        this.idToData.put(dInfo.getDataId(), dInfo);
        return dInfo;
    }

    private void deregisterData(DataInfo di) {
        int dataId = di.getDataId();
        idToData.remove(dataId);
    }

    /**
     * Registers the remote data.
     *
     * @param internalData local value
     * @param externalData Existing LogicalData to bind the value
     */
    public void registerRemoteDataSources(DataParams internalData, String externalData) {
        DataInfo dInfo = internalData.getDataInfo();
        if (dInfo == null) {
            if (DEBUG) {
                LOGGER.debug("Registering Remote data on DIP: " + internalData.getDescription());
            }
            dInfo = registerData(internalData);
        }
        if (externalData != null && dInfo != null) {
            String existingRename = dInfo.getCurrentDataVersion().getDataInstanceId().getRenaming();
            try {
                Comm.linkData(externalData, existingRename);
            } catch (CommException ce) {
                ErrorManager.error("Could not link the newly created data for " + internalData.getDescription()
                    + " with data " + externalData, ce);
            }
        }
    }

    /**
     * Obtains the last value produced for a data.
     *
     * @param internalData local value
     * @return last data produced for that value.
     */
    public LogicalData getDataLastVersion(DataParams internalData) {
        DataInfo dInfo = internalData.getDataInfo();
        if (dInfo != null) {
            return dInfo.getCurrentDataVersion().getDataInstanceId().getData();
        }
        return null;
    }

    /**
     * DataAccess interface: registers a new data access.
     *
     * @param access Access Parameters.
     * @return The registered access Id.
     * @throws ValueUnawareRuntimeException the runtime is not aware of the last value of the accessed data
     */
    public DataAccessId registerAccessToExistingData(AccessParams access) throws ValueUnawareRuntimeException {
        access.checkAccessValidity(this);
        return registerDataAccess(access);
    }

    /**
     * DataAccess interface: registers a new data access.
     *
     * @param access Access Parameters.
     * @return The registered access Id.
     */
    public DataAccessId registerDataAccess(AccessParams access) {
        DataInfo dInfo = access.getDataInfo();
        if (dInfo == null) {
            if (DEBUG) {
                LOGGER.debug("FIRST access to " + access.getDataDescription());
            }
            dInfo = registerData(access.getData());
            access.registeredAsFirstVersionForData(dInfo);
        } else {
            if (DEBUG) {
                LOGGER.debug("Another access to " + access.getDataDescription());
            }
        }

        DataAccessId daId = willAccess(access, dInfo);
        return daId;
    }

    /**
     * Marks the access from the main as finished.
     * 
     * @param access access being completed
     */
    public void finishDataAccess(AccessParams access, DataInstanceId generatedData) {
        if (generatedData != null && access.resultRemainOnMain()) {
            this.valuesOnMain.add(generatedData.getRenaming());
        }
        DataInfo dInfo = access.getDataInfo();
        // First access to this file
        if (dInfo == null) {
            LOGGER.warn(access.getDataDescription() + " has not been accessed before");
            return;
        }
        DataAccessId daid = getAccess(access.getMode(), dInfo);
        if (daid == null) {
            LOGGER.warn(access.getDataDescription() + " has not been accessed before");
            return;
        }
        dataHasBeenAccessed(daid);
    }

    private DataAccessId willAccess(AccessParams access, DataInfo di) {
        // Version management
        DataAccessId daId = null;
        switch (access.getMode()) {
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

            case CV:
            case RW:
                di.willBeRead();
                DataVersion readInstance = di.getCurrentDataVersion();
                di.willBeWritten();
                DataVersion writtenInstance = di.getCurrentDataVersion();
                if (readInstance != null) {
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
                } else {
                    ErrorManager.warn("Previous instance for data" + di.getDataId() + " is null.");
                }
                break;
        }
        access.externalRegister();
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
                case CV:
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
    public void dataAccessHasBeenCanceled(DataAccessId dAccId, boolean keepModified) {
        Integer dataId = dAccId.getDataId();
        DataInfo di = this.idToData.get(dataId);
        if (di != null) {
            Integer rVersionId;
            Integer wVersionId;
            boolean deleted = false;
            switch (dAccId.getDirection()) {
                case C:
                case R:
                    rVersionId = ((RAccessId) dAccId).getReadDataInstance().getVersionId();
                    deleted = di.canceledReadVersion(rVersionId);
                    break;
                case CV:
                case RW:
                    rVersionId = ((RWAccessId) dAccId).getReadDataInstance().getVersionId();
                    wVersionId = ((RWAccessId) dAccId).getWrittenDataInstance().getVersionId();
                    if (keepModified) {
                        di.versionHasBeenRead(rVersionId);
                        // read data version can be removed
                        di.tryRemoveVersion(rVersionId);
                        deleted = di.versionHasBeenWritten(wVersionId);
                    } else {
                        di.canceledReadVersion(rVersionId);
                        deleted = di.canceledWriteVersion(wVersionId);
                    }
                    break;
                default:// case W:
                    wVersionId = ((WAccessId) dAccId).getWrittenDataInstance().getVersionId();
                    deleted = di.canceledWriteVersion(wVersionId);
                    break;
            }

            if (deleted) {
                deregisterData(di);
            }
        } else {
            LOGGER.debug("Access of Data" + dAccId.getDataId() + " in Mode " + dAccId.getDirection().name()
                + " can not be cancelled because do not exist in DIP.");
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
        if (di != null) {
            Integer rVersionId = null;
            Integer wVersionId;
            boolean deleted = false;

            if (dAccId.isRead()) {
                rVersionId = ((DataAccessId.ReadingDataAccessId) dAccId).getReadDataInstance().getVersionId();
                deleted = di.versionHasBeenRead(rVersionId);
            }

            if (dAccId.isWrite()) {
                wVersionId = ((DataAccessId.WritingDataAccessId) dAccId).getWrittenDataInstance().getVersionId();
                if (rVersionId == null) {
                    rVersionId = wVersionId - 1;
                }
                di.tryRemoveVersion(rVersionId);
                deleted = di.versionHasBeenWritten(wVersionId);
            }

            if (deleted) {
                deregisterData(di);
            }
        } else {
            LOGGER.warn("Access of Data" + dAccId.getDataId() + " in Mode " + dAccId.getDirection().name()
                + "can not be mark as accessed because do not exist in DIP.");
        }
    }

    /**
     * Returns whether a given data has been accessed or not.
     *
     * @param data data whose last version is wanted to be obtained
     * @return {@code true} if the data has been accessed, {@code false} otherwise.
     */
    public boolean alreadyAccessed(DataParams data) {
        LOGGER.debug("Check already accessed: " + data.getDescription());
        DataInfo dInfo = data.getDataInfo();
        return dInfo != null;
    }

    /**
     * Returns whether the data is registered in the master or not.
     *
     * @param data Data Params.
     * @return {@code true} if the renaming is registered in the master, {@code false} otherwise.
     */
    public boolean isHere(DataParams data) {
        DataInfo oInfo = data.getDataInfo();
        DataInstanceId dId = oInfo.getCurrentDataVersion().getDataInstanceId();
        return this.valuesOnMain.contains(dId.getRenaming());
    }

    /**
     * Waits until data is ready for its safe deletion.
     *
     * @param data data to wait to be ready to delete
     * @param sem element to notify the operations completeness.
     * @throws ValueUnawareRuntimeException the runtime is not aware of the data
     * @throws NonExistingValueException the data to delete does not actually exist
     */
    public void waitForDataReadyToDelete(DataParams data, Semaphore sem)
        throws ValueUnawareRuntimeException, NonExistingValueException {
        LOGGER.debug("Waiting for data " + data.getDescription() + " to be ready for deletion");
        DataInfo dataInfo = data.getDataInfo();
        if (dataInfo == null) {
            if (DEBUG) {
                LOGGER.debug("No data found for data associated to " + data.getDescription());
            }
            throw new ValueUnawareRuntimeException();
        }
        dataInfo.waitForDataReadyToDelete(sem);
    }

    /**
     * Marks a data for deletion.
     *
     * @param data data to be deleted
     * @return DataInfo associated with the data to remove
     * @throws ValueUnawareRuntimeException the runtime is not aware of the data
     */
    public DataInfo deleteData(DataParams data) throws ValueUnawareRuntimeException {
        if (DEBUG) {
            LOGGER.debug("Deleting Data associated to " + data.getDescription());
        }

        DataInfo dataInfo = data.removeDataInfo();
        if (dataInfo == null) {
            if (DEBUG) {
                LOGGER.debug("No data found for data associated to " + data.getDescription());
            }
            throw new ValueUnawareRuntimeException();
        }
        // We delete the data associated with all the versions of the same object
        if (dataInfo.delete()) {
            deregisterData(dataInfo);
        }
        return dataInfo;
    }

    /**
     * Blocks fInfo and retrieves its result file.
     *
     * @param fInfo Data Id.
     * @param listener Result listener.
     * @return The result file.
     */
    public ResultFile blockDataAndGetResultFile(FileInfo fInfo, ResultListener listener) {
        int dataId = fInfo.getDataId();
        DataInstanceId lastVersion;
        if (DEBUG) {
            LOGGER.debug("Get Result file for data " + dataId);
        }
        if (fInfo.hasBeenCanceled()) {
            if (!fInfo.isCurrentVersionToDelete()) { // If current version is to delete do not
                // transfer
                String[] splitPath = fInfo.getOriginalLocation().getPath().split(File.separator);
                String origName = splitPath[splitPath.length - 1];
                if (origName.startsWith("compss-serialized-obj_")) {
                    // Do not transfer objects serialized by the bindings
                    if (DEBUG) {
                        LOGGER.debug("Discarding file " + origName + " as a result");
                    }
                    return null;
                }
                fInfo.blockDeletions();

                lastVersion = fInfo.getCurrentDataVersion().getDataInstanceId();

                ResultFile rf = new ResultFile(fInfo, lastVersion, fInfo.getOriginalLocation());

                String renaming = lastVersion.getRenaming();

                // Look for the last available version
                while (renaming != null && !Comm.existsData(renaming)) {
                    renaming = DataInstanceId.previousVersionRenaming(renaming);
                }
                if (renaming == null) {
                    LOGGER.error(RES_FILE_TRANSFER_ERR + ": Cannot transfer file " + lastVersion.getRenaming()
                        + " nor any of its previous versions");
                    return null;
                }

                LogicalData data = Comm.getData(renaming);
                // Check if data is a PSCO and must be consolidated
                for (DataLocation loc : data.getLocations()) {
                    if (loc instanceof PersistentLocation) {
                        String pscoId = ((PersistentLocation) loc).getId();
                        if (Tracer.isActivated()) {
                            Tracer.emitEvent(TraceEvent.STORAGE_CONSOLIDATE);
                        }
                        try {
                            StorageItf.consolidateVersion(pscoId);
                        } catch (StorageException e) {
                            LOGGER.error("Cannot consolidate PSCO " + pscoId, e);
                        } finally {
                            if (Tracer.isActivated()) {
                                Tracer.emitEventEnd(TraceEvent.STORAGE_CONSOLIDATE);
                            }
                        }
                        LOGGER.debug("Returned because persistent object");
                        return rf;
                    }

                }

                // If no PSCO location is found, perform normal getData
                if (rf.getOriginalLocation().getProtocol() == ProtocolType.BINDING_URI) {
                    // Comm.getAppHost().getData(data, rf.getOriginalLocation(), new
                    // BindingObjectTransferable(),
                    // listener);
                    if (DEBUG) {
                        LOGGER.debug("Discarding data d" + dataId + " as a result beacuse it is a binding object");
                    }
                } else {
                    if (rf.getOriginalLocation().getProtocol() == ProtocolType.DIR_URI) {
                        listener.addOperation();
                        Comm.getAppHost().getData(data, rf.getOriginalLocation(), new DirectoryTransferable(),
                            listener);
                    } else {
                        listener.addOperation();
                        Comm.getAppHost().getData(data, rf.getOriginalLocation(), new FileTransferable(), listener);
                    }
                }

                return rf;
            } else {
                if (fInfo.isCurrentVersionToDelete()) {
                    if (DEBUG) {
                        String[] splitPath = fInfo.getOriginalLocation().getPath().split(File.separator);
                        String origName = splitPath[splitPath.length - 1];
                        LOGGER.debug("Trying to delete file " + origName);
                    }
                    if (fInfo.delete()) {
                        deregisterData(fInfo);
                    }
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

}
