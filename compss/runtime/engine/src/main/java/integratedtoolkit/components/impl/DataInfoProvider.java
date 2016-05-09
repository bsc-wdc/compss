package integratedtoolkit.components.impl;

import integratedtoolkit.comm.Comm;
import integratedtoolkit.types.data.location.DataLocation;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.data.*;
import integratedtoolkit.types.data.AccessParams.*;
import integratedtoolkit.types.data.DataAccessId.*;
import integratedtoolkit.types.data.operation.FileTransferable;
import integratedtoolkit.types.data.operation.ObjectTransferable;
import integratedtoolkit.types.data.operation.OneOpWithSemListener;
import integratedtoolkit.types.data.operation.ResultListener;
import integratedtoolkit.types.request.ap.TransferObjectRequest;
import java.io.File;
import java.util.concurrent.Semaphore;

public class DataInfoProvider {

    // Constants definition
    private static final String RES_FILE_TRANSFER_ERR = "Error transferring result files";
    //private static final String SERIALIZATION_ERR = "Error serializing object to a file";

    // Map: filename:host:path -> file identifier
    private TreeMap<String, Integer> nameToId;
    // Map: hash code -> object identifier
    private TreeMap<Integer, Integer> codeToId;
    // Map: file identifier -> file information
    private TreeMap<Integer, DataInfo> idToData;
    // Map: Object_Version_Renaming -> Object value
    private TreeMap<String, Object> renamingToValue; // TODO: Remove obsolete from here

    // Component logger - No need to configure, ProActive does
    private static final Logger logger = Logger.getLogger(Loggers.DIP_COMP);
    private static final boolean debug = logger.isDebugEnabled();

    public DataInfoProvider() {
        nameToId = new TreeMap<String, Integer>();
        codeToId = new TreeMap<Integer, Integer>();
        idToData = new TreeMap<Integer, DataInfo>();
        renamingToValue = new TreeMap<String, Object>();

        logger.info("Initialization finished");
    }

    // DataAccess interface
    public DataAccessId registerDataAccess(AccessParams access) {
        if (access instanceof FileAccessParams) {
            FileAccessParams fAccess = (FileAccessParams) access;
            return registerFileAccess(fAccess.getMode(),
                    fAccess.getLocation(),
                    -1);
        } else {
            ObjectAccessParams oAccess = (ObjectAccessParams) access;
            return registerObjectAccess(oAccess.getMode(),
                    oAccess.getValue(),
                    oAccess.getCode(),
                    -1);
        }
    }

    public DataAccessId registerFileAccess(AccessMode mode, DataLocation location, int readerId) {
        DataInfo fileInfo;
        String locationKey = location.getLocationKey();
        Integer fileId = nameToId.get(locationKey);

        // First access to this file
        if (fileId == null) {
            if (debug) {
                logger.debug("FIRST access to " + location.getLocationKey());
            }
            // Update mappings
            fileInfo = new FileInfo(location);
            fileId = fileInfo.getDataId();
            nameToId.put(locationKey, fileId);
            idToData.put(fileId, fileInfo);

            // Register the initial location of the file
            if (mode != AccessMode.W) {
                Comm.registerLocation(fileInfo.getCurrentDataInstanceId().getRenaming(), location);
            }
        } else {
            // The file has already been accessed, all location are already registered
            if (debug) {
                logger.debug("Another access to " + location.getLocationKey());
            }
            fileInfo = idToData.get(fileId);
        }

        // Version management
        return willAccess(mode, fileInfo);
    }

    // Object access
    public DataAccessId registerObjectAccess(AccessMode mode, Object value, int code, int readerId) {
        DataInfo oInfo;
        Integer aoId = codeToId.get(code);

        // First access to this datum
        if (aoId == null) {
            if (debug) {
                logger.debug("FIRST access to object " + code);
            }

            // Update mappings
            oInfo = new ObjectInfo(code);
            aoId = oInfo.getDataId();
            codeToId.put(code, aoId);
            idToData.put(aoId, oInfo);

            // Serialize this first version of the object to a file
            DataInstanceId lastDID = oInfo.getCurrentDataInstanceId();
            String renaming = lastDID.getRenaming();

            // Inform the File Transfer Manager about the new file containing the object
            if (mode != AccessMode.W) {
                Comm.registerValue(renaming, value);
            }

        } else {// The datum has already been accessed
            if (debug) {
                logger.debug("Another access to object " + code);
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
                daId = new RAccessId(di.getCurrentDataInstanceId());
                if (debug) {
                    StringBuilder sb = new StringBuilder("");
                    sb.append("Access:").append("\n");
                    sb.append("  * Type: R").append("\n");
                    sb.append("  * Read Datum: d").append(daId.getDataId()).append("v").append(((RAccessId) daId).getRVersionId()).append("\n");
                    logger.debug(sb.toString());
                }
                break;

            case W:
                di.willBeWritten();
                daId = new WAccessId(di.getCurrentDataInstanceId());
                if (debug) {
                    StringBuilder sb = new StringBuilder("");
                    sb.append("Access:").append("\n");
                    sb.append("  * Type: W").append("\n");
                    sb.append("  * Write Datum: d").append(daId.getDataId()).append("v").append(((WAccessId) daId).getWVersionId()).append("\n");
                    logger.debug(sb.toString());
                }
                break;

            case RW:
                boolean preserveSourceData = di.isToBeRead();
                di.willBeRead();
                DataInstanceId readInstance = di.getCurrentDataInstanceId();
                di.willBeWritten();
                DataInstanceId writtenInstance = di.getCurrentDataInstanceId();
                daId = new RWAccessId(readInstance, writtenInstance, preserveSourceData);
                if (debug) {
                    StringBuilder sb = new StringBuilder("");
                    sb.append("Access:").append("\n");
                    sb.append("  * Type: RW").append("\n");
                    sb.append("  * Read Datum: d").append(daId.getDataId()).append("v").append(((RWAccessId) daId).getRVersionId()).append("\n");
                    sb.append("  * Write Datum: d").append(daId.getDataId()).append("v").append(((RWAccessId) daId).getWVersionId()).append("\n");
                    logger.debug(sb.toString());
                }
                break;
        }
        return daId;
    }

    public void dataHasBeenAccessed(DataAccessId dAccId) {
        Integer dataId = dAccId.getDataId();
        DataInfo di = idToData.get(dataId);
        Integer rVersionId = null;
        Integer wVersionId = null;
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
            idToData.remove(dataId);
        }
    }

    public boolean alreadyAccessed(DataLocation loc) {
        String locationKey = loc.getLocationKey();
        Integer fileId = nameToId.get(locationKey);
        return fileId != null;
    }

    // DataInformation interface
    public String getLastRenaming(int code) {
        Integer aoId = codeToId.get(code);
        DataInfo oInfo = idToData.get(aoId);
        return oInfo.getCurrentDataInstanceId().getRenaming();
    }

    public DataLocation getOriginalLocation(int fileId) {
        FileInfo info = (FileInfo) idToData.get(fileId);
        return info.getOriginalLocation();
    }

    public void setObjectVersionValue(String renaming, Object value) {
        renamingToValue.put(renaming, value);
        Comm.registerValue(renaming, value);
    }

    public boolean isHere(DataInstanceId dId) {
        return renamingToValue.get(dId.getRenaming()) != null;
    }

    public Object getObject(String renaming) {
        return renamingToValue.get(renaming);
    }

    public void newVersionSameValue(String rRenaming, String wRenaming) {
        renamingToValue.put(wRenaming, renamingToValue.get(rRenaming));
    }

    public DataInstanceId getLastDataAccess(int code) {
        Integer aoId = codeToId.get(code);
        DataInfo oInfo = idToData.get(aoId);
        return oInfo.getCurrentDataInstanceId();
    }

    public List<DataInstanceId> getLastVersions(TreeSet<Integer> dataIds) {
        List<DataInstanceId> versionIds = new ArrayList<DataInstanceId>(dataIds.size());
        for (Integer dataId : dataIds) {
            DataInfo dataInfo = idToData.get(dataId);
            if (dataInfo != null) {
                versionIds.add(dataInfo.getCurrentDataInstanceId());
            } else {
                versionIds.add(null);
            }
        }
        return versionIds;
    }

    public void unblockDataId(Integer dataId) {
        DataInfo dataInfo = idToData.get(dataId);
        dataInfo.unblockDeletions();
    }

    public FileInfo deleteData(DataLocation loc) {
        String locationKey = loc.getLocationKey();
        Integer fileId = nameToId.get(locationKey);
        if (fileId == null) {
            return null;
        }
        FileInfo fileInfo = (FileInfo) idToData.get(fileId);
        nameToId.remove(locationKey);
        if (fileInfo.delete()) {
            idToData.remove(fileId);
        }
        return fileInfo;
    }

    public void transferObjectValue(TransferObjectRequest toRequest) {
        Semaphore sem = toRequest.getSemaphore();
        DataAccessId daId = toRequest.getDaId();
        RWAccessId rwaId = (RWAccessId) daId;

        String sourceName = rwaId.getReadDataInstance().getRenaming();
        //String targetName = rwaId.getWrittenDataInstance().getRenaming();

        LogicalData ld = Comm.getData(sourceName);

        if (ld.isInMemory()) {
            if (!ld.isOnFile()) { // Only if there are no readers
                try {
                    ld.writeToFileAndRemoveValue();
                } catch (Exception e) {
                    logger.fatal("Exception writing object to file.", e);
                }
            } else {
                Comm.clearValue(sourceName);
            }
            toRequest.setResponse(ld.getValue());
            toRequest.getSemaphore().release();
        } else {
            DataLocation targetLocation = DataLocation.getLocation(Comm.appHost, Comm.appHost.getTempDirPath() + sourceName);
            Comm.appHost.getData(sourceName, targetLocation, new ObjectTransferable(), new OneOpWithSemListener(sem));
        }
    }

    public ResultFile blockDataAndGetResultFile(int dataId, ResultListener listener) {
        DataInstanceId lastVersion;
        FileInfo fileInfo = (FileInfo) idToData.get(dataId);
        if (fileInfo != null) {
            String[] splitPath = fileInfo.getOriginalLocation().getPath().split(File.separator);
            String origName = splitPath[splitPath.length - 1];
            if (origName.startsWith("compss-serialized-obj_")) { // Do not transfer objects serialized by the bindings
                if (debug) {
                    logger.debug("Discarding file " + origName + " as a result");
                }
                return null;
            }
            fileInfo.blockDeletions();
            lastVersion = fileInfo.getCurrentDataInstanceId();
            ResultFile rf = new ResultFile(lastVersion, fileInfo.getOriginalLocation());

            DataInstanceId fId = rf.getFileInstanceId();
            String renaming = fId.getRenaming();

            // Look for the last available version
            while (renaming != null && !Comm.existsData(renaming)) {
                renaming = DataInstanceId.previousVersionRenaming(renaming);
            }
            if (renaming == null) {
                logger.error(RES_FILE_TRANSFER_ERR + ": Cannot transfer file " + fId.getRenaming() + " nor any of its previous versions");
                return null;
            }

            listener.addOperation();
            Comm.appHost.getData(renaming, rf.getOriginalLocation(), new FileTransferable(), listener);
            return rf;
        }
        return null;
    }

    public void shutdown() {
        //Nothing to do
    }

}
