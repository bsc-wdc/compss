package integratedtoolkit.types.data;

import integratedtoolkit.comm.Comm;


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
        Comm.registerData(dataInstanceId.getRenaming());
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
            Comm.removeData(dataInstanceId.getRenaming());
            return true;
        }
        return false;
    }

    private boolean checkDeletion() {
        if (toDelete // deletion requested
                && writters == 0 // version has been generated
                && readers == 0 // version has been read
        ) {
            Comm.removeData(dataInstanceId.getRenaming());
            return true;
        }
        return false;
    }

    public boolean isToDelete() {
        return toDelete;
    }

}
