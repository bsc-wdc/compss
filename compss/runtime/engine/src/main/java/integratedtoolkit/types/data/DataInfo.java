package integratedtoolkit.types.data;

import java.util.LinkedList;
import java.util.TreeMap;


// Information about a datum and its versions
public abstract class DataInfo {

    private static final int FIRST_FILE_ID = 1;
    private static final int FIRST_VERSION_ID = 1;

    protected static int nextDataId = FIRST_FILE_ID;
    // Data identifier
    protected int dataId;

    // Current version
    protected DataVersion currentVersion;
    // Data and version identifier management
    protected int currentVersionId;

    // Versions of the datum
    // Map: version identifier -> version
    protected TreeMap<Integer, DataVersion> versions;
    //private boolean toDelete;

    protected int deletionBlocks;
    protected final LinkedList<DataVersion> pendingDeletions;


    public DataInfo() {
        this.dataId = nextDataId++;
        this.versions = new TreeMap<>();
        this.currentVersionId = FIRST_VERSION_ID;
        this.currentVersion = new DataVersion(dataId, 1);
        this.versions.put(currentVersionId, currentVersion);
        this.deletionBlocks = 0;
        this.pendingDeletions = new LinkedList<>();
    }

    public int getDataId() {
        return dataId;
    }

    public int getCurrentVersionId() {
        return currentVersionId;
    }

    public DataInstanceId getCurrentDataInstanceId() {
        return currentVersion.getDataInstanceId();
    }
    
    public DataInstanceId getPreviousDataInstanceId() {
    	DataVersion dv =versions.get(currentVersionId-1);
    	if (dv!=null){
    		return dv.getDataInstanceId();
    	}else{
    		return null;
    	}
    }


    public void willBeRead() {
        currentVersion.willBeRead();
    }

    public boolean isToBeRead() {
        return currentVersion.hasPendingLectures();
    }

    public boolean versionHasBeenRead(int versionId) {
        DataVersion readVersion = versions.get(versionId);
        if (readVersion.hasBeenRead()) {
            versions.remove(versionId);
            //return (this.toDelete && versions.size() == 0);
            return (versions.size() == 0);
        }
        return false;
    }

    public void willBeWritten() {
        currentVersionId++;
        DataVersion newVersion = new DataVersion(dataId, currentVersionId);
        newVersion.willBeWritten();
        versions.put(currentVersionId, newVersion);
        currentVersion = newVersion;
    }

    public boolean versionHasBeenWritten(int versionId) {
        DataVersion writtenVersion = versions.get(versionId);
        if (writtenVersion.hasBeenWritten()) {
            versions.remove(versionId);
            //return (this.toDelete && versions.size() == 0);
            return (versions.size() == 0);
        }
        return false;
    }

    public void blockDeletions() {
        deletionBlocks++;
    }

    public boolean unblockDeletions() {
        deletionBlocks--;
        if (deletionBlocks == 0) {
            for (DataVersion version : pendingDeletions) {
                if (version.delete()) {
                    versions.remove(version.getDataInstanceId().getVersionId());
                }
            }
            if (versions.size() == 0) {
                return true;
            }
        }
        return false;
    }

    public boolean delete() {
        //this.toDelete = true;
        if (deletionBlocks > 0) {
            for (DataVersion version : versions.values()) {
                pendingDeletions.add(version);
            }
        } else {
            LinkedList<Integer> removedVersions = new LinkedList<>();
            for (DataVersion version : versions.values()) {
                if (version.delete()) {
                    removedVersions.add(version.getDataInstanceId().getVersionId());
                }
            }
            for (int versionId : removedVersions) {
                versions.remove(versionId);
            }
            if (versions.size() == 0) {
                return true;
            }
        }
        return false;
    }
    
    public boolean isCurrentVersionToDelete(){
    	return currentVersion.isToDelete();
    }
    
    
}
