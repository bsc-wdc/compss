package es.bsc.compss.components.impl;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.PersistentLocation;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;
import java.io.File;
import java.io.IOException;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.*;
import es.bsc.compss.types.data.AccessParams.*;
import es.bsc.compss.types.data.DataAccessId.*;
import es.bsc.compss.types.data.operation.FileTransferable;
import es.bsc.compss.types.data.operation.ObjectTransferable;
import es.bsc.compss.types.data.operation.OneOpWithSemListener;
import es.bsc.compss.types.data.operation.ResultListener;
import es.bsc.compss.types.request.ap.TransferObjectRequest;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StorageException;
import storage.StorageItf;


/**
 * Component to handle the specific data structures such as file names, versions, renamings and values
 *
 */
public class DataInfoProvider {

    // Constants definition
    private static final String RES_FILE_TRANSFER_ERR = "Error transferring result files";

    // Map: filename:host:path -> file identifier
    private TreeMap<String, Integer> nameToId;
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
     * New Data Info Provider instance
     * 
     */
    public DataInfoProvider() {
        nameToId = new TreeMap<>();
        codeToId = new TreeMap<>();
        idToData = new TreeMap<>();
        renamingToValue = new TreeMap<>();

        LOGGER.info("Initialization finished");
    }

    /**
     * DataAccess interface: registers a new data access
     * 
     * @param access
     * @return
     */
    public DataAccessId registerDataAccess(AccessParams access) {
        if (access instanceof FileAccessParams) {
            FileAccessParams fAccess = (FileAccessParams) access;
            return registerFileAccess(fAccess.getMode(), fAccess.getLocation());
        } else {
            ObjectAccessParams oAccess = (ObjectAccessParams) access;
            return registerObjectAccess(oAccess.getMode(), oAccess.getValue(), oAccess.getCode());
        }
    }

    /**
     * DataAccess interface: registers a new file access
     * 
     * @param mode
     * @param location
     * @return
     */
    public DataAccessId registerFileAccess(AccessMode mode, DataLocation location) {
        DataInfo fileInfo;
        String locationKey = location.getLocationKey();
        Integer fileId = nameToId.get(locationKey);

        // First access to this file
        if (fileId == null) {
            if (DEBUG) {
                LOGGER.debug("FIRST access to " + location.getLocationKey());
            }
            // Update mappings
            fileInfo = new FileInfo(location);
            fileId = fileInfo.getDataId();
            nameToId.put(locationKey, fileId);
            idToData.put(fileId, fileInfo);

            // Register the initial location of the file
            if (mode != AccessMode.W) {
                Comm.registerLocation(fileInfo.getCurrentDataVersion().getDataInstanceId().getRenaming(), location);
            }
        } else {
            // The file has already been accessed, all location are already registered
            if (DEBUG) {
                LOGGER.debug("Another access to " + location.getLocationKey());
            }
            fileInfo = idToData.get(fileId);
        }

        // Version management
        return willAccess(mode, fileInfo);
    }
    
    public void finishFileAccess(AccessMode mode, DataLocation location) {
    	DataInfo fileInfo;
        String locationKey = location.getLocationKey();
        Integer fileId = nameToId.get(locationKey);

        // First access to this file
        if (fileId == null) {
               LOGGER.warn("File " + location.getLocationKey() + " has not been accessed before");
               return;
        }
        fileInfo = idToData.get(fileId);
        DataAccessId daid = getAccess(mode, fileInfo);
        if (daid == null){
        	LOGGER.warn("File " + location.getLocationKey() + " has not been accessed before");
            return;
        }
        dataHasBeenAccessed(daid);
               
    }
    	
    /**
     * DataAccess interface: registers a new object access
     * 
     * @param mode
     * @param value
     * @param code
     * @return
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
            codeToId.put(code, aoId);
            idToData.put(aoId, oInfo);

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

            oInfo = idToData.get(aoId);
        }

        // Version management
        return willAccess(mode, oInfo);
    }

    /**
     * DataAccess interface: registers a new object access
     * 
     * @param mode
     * @param value
     * @param code
     * @return
     */
    public DataAccessId registerExternalObjectAccess(AccessMode mode, String pscoId, int code) {
        DataInfo oInfo;
        Integer aoId = codeToId.get(code);

        // First access to this datum
        if (aoId == null) {
            if (DEBUG) {
                LOGGER.debug("FIRST access to external object " + code);
            }

            // Update mappings
            oInfo = new ObjectInfo(code);
            aoId = oInfo.getDataId();
            codeToId.put(code, aoId);
            idToData.put(aoId, oInfo);

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

            oInfo = idToData.get(aoId);
        }

        // Version management
        return willAccess(mode, oInfo);
    }

    private DataAccessId willAccess(AccessMode mode, DataInfo di) {
        // Version management
        DataAccessId daId = null;
        switch (mode) {
            case R:
                di.willBeRead();
                daId = new RAccessId(di.getCurrentDataVersion());
                if (DEBUG) {
                    StringBuilder sb = new StringBuilder("");
                    sb.append("Access:").append("\n");
                    sb.append("  * Type: R").append("\n");
                    sb.append("  * Read Datum: d").append(daId.getDataId()).append("v").append(((RAccessId) daId).getRVersionId())
                            .append("\n");
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
                    sb.append("  * Write Datum: d").append(daId.getDataId()).append("v").append(((WAccessId) daId).getWVersionId())
                            .append("\n");
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
                    sb.append("  * Read Datum: d").append(daId.getDataId()).append("v").append(((RWAccessId) daId).getRVersionId())
                            .append("\n");
                    sb.append("  * Write Datum: d").append(daId.getDataId()).append("v").append(((RWAccessId) daId).getWVersionId())
                            .append("\n");
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
        if (currentInstance!=null){
        	switch (mode) {
            	case R:
            		daId = new RAccessId(currentInstance);
            		break;

            	case W:
            		daId = new WAccessId(di.getCurrentDataVersion());
            		break;
            	case RW:
            		DataVersion readInstance = di.getPreviousDataVersion();
            		if (readInstance !=null){
            			daId = new RWAccessId(readInstance, currentInstance);
            		}else{
            			LOGGER.warn("Previous instance for data" + di.getDataId() + " is null." );
            		}
            		break;
        	}
        }else{
        	LOGGER.warn("Current instance for data" + di.getDataId() + " is null." );
        }
        return daId;
    }

    /**
     * Returns if a given data has been accessed or not
     * 
     * @param dAccId
     */
    public void dataHasBeenAccessed(DataAccessId dAccId) {
        Integer dataId = dAccId.getDataId();
        DataInfo di = idToData.get(dataId);
        Integer rVersionId;
        Integer wVersionId;
        boolean deleted = false;
        switch (dAccId.getDirection()) {
            case R:
                rVersionId = ((RAccessId) dAccId).getReadDataInstance().getVersionId();
                deleted = di.versionHasBeenRead(rVersionId);
                break;
            case RW:
                rVersionId = ((RWAccessId) dAccId).getReadDataInstance().getVersionId();
                di.versionHasBeenRead(rVersionId);
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
     * Returns if a given location has been accessed or not
     * 
     * @param loc
     * @return
     */
    public boolean alreadyAccessed(DataLocation loc) {
        LOGGER.debug("Check already accessed: " + loc.getLocationKey());
        String locationKey = loc.getLocationKey();
        Integer fileId = nameToId.get(locationKey);
        return fileId != null;
    }

    /**
     * DataInformation interface: returns the last renaming of a given data
     * 
     * @param code
     * @return
     */
    public String getLastRenaming(int code) {
        Integer aoId = codeToId.get(code);
        DataInfo oInfo = idToData.get(aoId);
        return oInfo.getCurrentDataVersion().getDataInstanceId().getRenaming();
    }

    /**
     * Returns the original location of a data id
     * 
     * @param fileId
     * @return
     */
    public DataLocation getOriginalLocation(int fileId) {
        FileInfo info = (FileInfo) idToData.get(fileId);
        return info.getOriginalLocation();
    }

    /**
     * Sets the value @value to the renaming @renaming
     * 
     * @param renaming
     * @param value
     */
    public void setObjectVersionValue(String renaming, Object value) {
        renamingToValue.put(renaming, value);
        Comm.registerValue(renaming, value);
    }

    /**
     * Returns if the dataInstanceId is registered in the master or not
     * 
     * @param dId
     * @return
     */
    public boolean isHere(DataInstanceId dId) {
        return renamingToValue.get(dId.getRenaming()) != null;
    }

    /**
     * Returns the object associated to the renaming @renaming
     * 
     * @param renaming
     * @return
     */
    public Object getObject(String renaming) {
        return renamingToValue.get(renaming);
    }

    /**
     * Creates a new version with the same value
     * 
     * @param rRenaming
     * @param wRenaming
     */
    public void newVersionSameValue(String rRenaming, String wRenaming) {
        renamingToValue.put(wRenaming, renamingToValue.get(rRenaming));
    }

    /**
     * Returns the last data access to a given renaming
     * 
     * @param code
     * @return
     */
    public DataInstanceId getLastDataAccess(int code) {
        Integer aoId = codeToId.get(code);
        DataInfo oInfo = idToData.get(aoId);
        return oInfo.getCurrentDataVersion().getDataInstanceId();
    }

    /**
     * Returns the last versions of all the specified data Ids
     * 
     * @param dataIds
     * @return
     */
    public List<DataInstanceId> getLastVersions(TreeSet<Integer> dataIds) {
        List<DataInstanceId> versionIds = new ArrayList<>(dataIds.size());
        for (Integer dataId : dataIds) {
            DataInfo dataInfo = idToData.get(dataId);
            if (dataInfo != null) {
                versionIds.add(dataInfo.getCurrentDataVersion().getDataInstanceId());
            } else {
                versionIds.add(null);
            }
        }
        return versionIds;
    }

    /**
     * Unblocks a dataId
     * 
     * @param dataId
     */
    public void unblockDataId(Integer dataId) {
        DataInfo dataInfo = idToData.get(dataId);
        dataInfo.unblockDeletions();
    }

    /**
     * Marks a data Id for deletion
     * 
     * @param loc
     * @return
     */
    public FileInfo deleteData(DataLocation loc) {
        LOGGER.debug("Deleting Data location: " + loc.getPath());
        String locationKey = loc.getLocationKey();
        Integer fileId = nameToId.get(locationKey);
        if (fileId == null) {
            return null;
        }
        FileInfo fileInfo = (FileInfo) idToData.get(fileId);
        // nameToId.remove(locationKey);
        if (fileInfo.delete()) {
            // idToData.remove(fileId);
        }
        return fileInfo;
    }

    /**
     * Transfers the value of an object
     * 
     * @param toRequest
     * @return
     */
    public LogicalData transferObjectValue(TransferObjectRequest toRequest) {
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
            return null;
        }

        if (ld.isInMemory()) {
            // Write to storage (if needed)
            try {
                ld.writeToStorage();
            } catch (IOException e) {
                ErrorManager.error("Exception writing object to file.", e);
            }

            // Clear value
            ld.removeValue();

            // Set response
            toRequest.setResponse(ld.getValue());
            toRequest.setTargetData(ld);
            toRequest.getSemaphore().release();
        } else {
            if (DEBUG) {
                LOGGER.debug("Object " + sourceName + " not in memory. Requesting tranfers to " + Comm.getAppHost().getName());
            }
            DataLocation targetLocation = null;
            String path = DataLocation.Protocol.FILE_URI.getSchema() + Comm.getAppHost().getTempDirPath() + sourceName;
            try {
                SimpleURI uri = new SimpleURI(path);
                targetLocation = DataLocation.createLocation(Comm.getAppHost(), uri);
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
            }

            Comm.getAppHost().getData(sourceName, targetLocation, new ObjectTransferable(), new OneOpWithSemListener(sem));
        }

        return ld;
    }

    /**
     * Blocks dataId and retrieves its result file
     * 
     * @param dataId
     * @param listener
     * @return
     */
    public ResultFile blockDataAndGetResultFile(int dataId, ResultListener listener) {
        DataInstanceId lastVersion;
        FileInfo fileInfo = (FileInfo) idToData.get(dataId);
        if (fileInfo != null && !fileInfo.isCurrentVersionToDelete()) { // If current version is to delete do not
                                                                        // transfer
            String[] splitPath = fileInfo.getOriginalLocation().getPath().split(File.separator);
            String origName = splitPath[splitPath.length - 1];
            if (origName.startsWith("compss-serialized-obj_")) { // Do not transfer objects serialized by the bindings
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
                LOGGER.error(RES_FILE_TRANSFER_ERR + ": Cannot transfer file " + fId.getRenaming() + " nor any of its previous versions");
                return null;
            }

            // Check if data is a PSCO and must be consolidated
            for (DataLocation loc : Comm.getData(renaming).getLocations()) {
                if (loc instanceof PersistentLocation) {
                    String pscoId = ((PersistentLocation) loc).getId();
                    if (Tracer.isActivated()) {
                        Tracer.emitEvent(Tracer.Event.STORAGE_CONSOLIDATE.getId(), Tracer.Event.STORAGE_CONSOLIDATE.getType());
                    }
                    try {
                        StorageItf.consolidateVersion(pscoId);
                    } catch (StorageException e) {
                        LOGGER.error("Cannot consolidate PSCO " + pscoId, e);
                    } finally {
                        if (Tracer.isActivated()) {
                            Tracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_CONSOLIDATE.getType());
                        }
                    }
                    LOGGER.debug("Returned because persistent object");
                    return rf;
                }

            }

            // If no PSCO location is found, perform normal getData
            listener.addOperation();
            Comm.getAppHost().getData(renaming, rf.getOriginalLocation(), new FileTransferable(), listener);
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

        return null;
    }

    /**
     * Shuts down the component
     * 
     */
    public void shutdown() {
        // Nothing to do
    }

}
