package es.bsc.compss.types.data;


public class DataVersion {

    private final DataInstanceId dataInstanceId;
    private int readers;
    private int writters;
    private boolean toDelete;


    public DataVersion(int dataId, int versionId) {
        this.readers = 0;
        this.dataInstanceId = new DataInstanceId(dataId, versionId);
        this.writters = 0;
        this.toDelete = false;
    }

    public DataInstanceId getDataInstanceId() {
        return this.dataInstanceId;
    }

    public void willBeRead() {
        readers++;
    }

    public void willBeWritten() {
        writters++;
    }

    public boolean hasPendingLectures() {
        return readers > 0;
    }
    
    public boolean isOnlyReader() {
    	return readers > 1;
    }

    public boolean hasBeenRead() {
        readers--;
        return checkDeletion();
    }

    public boolean hasBeenWritten() {
        writters--;
        return checkDeletion();
    }

    public boolean delete() {
        toDelete = true;
        if (readers == 0 && writters == 0) {
            return true;
        }
        return false;
    }

    private boolean checkDeletion() {
        if (toDelete // deletion requested
                && writters == 0 // version has been generated
                && readers == 0 // version has been read
        ) {
            return true;
        }
        return false;
    }

    public boolean isToDelete() {
        return toDelete;
    }

}
