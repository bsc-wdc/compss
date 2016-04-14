package integratedtoolkit.types.data;

import integratedtoolkit.comm.Comm;
import integratedtoolkit.types.data.AccessParams.AccessMode;
import integratedtoolkit.types.data.DataAccessId.RAccessId;
import integratedtoolkit.types.data.DataAccessId.RWAccessId;
import integratedtoolkit.types.data.DataAccessId.WAccessId;
import java.util.LinkedList;
import java.util.TreeMap;
import org.apache.log4j.Logger;


// Information about a datum and its versions
public abstract class DataInfo {

    private static final int FIRST_FILE_ID = 1;
    private static final int FIRST_VERSION_ID = 1;
    // Data identifier
    protected int dataId;
    // Versions of the datum
    // Map: version identifier -> version
    protected TreeMap<Integer, Version> versions;
    // Current version
    protected Version currentVersion;
    // Data and version identifier management
    protected static int nextDataId;
    protected int currentVersionId;
    private int readers;
    protected boolean toDelete;

    public static void init() {
        nextDataId = FIRST_FILE_ID;
    }

    public DataInfo() {
        this.dataId = nextDataId++;
        this.versions = new TreeMap<Integer, Version>();
        this.currentVersionId = FIRST_VERSION_ID;
        this.currentVersion = new Version(dataId, 1);
        this.versions.put(currentVersionId, currentVersion);
        readers = 0;
    }

    public int getDataId() {
        return dataId;
    }

    public int getLastVersionId() {
        return currentVersionId;
    }

    public DataInstanceId getLastDataInstanceId() {
        return currentVersion.dataInstanceId;
    }

    public LinkedList<DataInstanceId> getAllDataInstances() {
        LinkedList<DataInstanceId> renamings = new LinkedList<DataInstanceId>();
        for (Version version : this.versions.values()) {
            renamings.add(version.dataInstanceId);
        }
        return renamings;
    }

    public int getNumberOfVersions() {
        return versions.size();
    }

    public int getReaders() {
        return readers;
    }

    public Integer getReadersForVersion(int versionId) {
        return versions.get(versionId).getReaders();
    }

    /*public Map<Integer, Integer> getMethodReadersForVersion(int versionId) {
     return versions.get(versionId).getMethodReaders();
     }*/
    public void addVersion() {
        currentVersionId++;
        Version newVersion = new Version(dataId, currentVersionId);
        versions.put(currentVersionId, newVersion);
        currentVersion = newVersion;
    }

    public int willBeRead(int methodId) {
        int i = currentVersion.willBeRead(methodId);
        readers++;
        return i;
    }

    public Integer versionHasBeenRead(int versionId, int methodId) {
        int i = versions.get(versionId).hasBeenRead(methodId);
        readers--;
        return i;
    }

    public void removeVersion(int versionId) {
        versions.remove(versionId);
    }

    public DataAccessId manageAccess(AccessMode mode, int readerId, boolean debug, Logger logger) {
        // Version management
        DataAccessId daId = null;
        switch (mode) {
            case R:
                willBeRead(readerId);
                daId = new RAccessId(getLastDataInstanceId());
                if (debug) {
                	StringBuilder sb = new StringBuilder("");
                	sb.append("Access:").append("\n");
                    sb.append("  * Type: R").append("\n");
                    sb.append("  * Read Datum: d").append(daId.getDataId()).append("v").append(((RAccessId) daId).getRVersionId()).append("\n");
                    logger.debug(sb.toString());
                }
                break;

            case W:
                addVersion();
                daId = new WAccessId(getLastDataInstanceId());
                if (debug) {
                	StringBuilder sb = new StringBuilder("");
                	sb.append("Access:").append("\n");
                    sb.append("  * Type: W").append("\n");
                    sb.append("  * Write Datum: d").append(daId.getDataId()).append("v").append(((WAccessId) daId).getWVersionId()).append("\n");
                    logger.debug(sb.toString());
                }
                break;

            case RW:
                int versionReaders = willBeRead(readerId);
                DataInstanceId readInstance = getLastDataInstanceId();
                addVersion();
                DataInstanceId writtenInstance = getLastDataInstanceId();
                boolean preserveSourceData = (versionReaders > 1);
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

    public boolean isToDelete() {
        return toDelete;
    }

    public void setToDelete(boolean deleted) {
        this.toDelete = deleted;
    }

    protected class Version {

        //private HashMap<Integer, Integer> readMethod;
        private int readers;
        private DataInstanceId dataInstanceId;

        public Version(int dataId, int versionId) {
            //this.readMethod = new HashMap<Integer, Integer>();
            this.readers = 0;
            this.dataInstanceId = new DataInstanceId(dataId, versionId);
            Comm.registerData(dataInstanceId.getRenaming());
        }

        public int willBeRead(int methodId) {
            /*Integer actual = readMethod.get(methodId);
             if (actual == null) {
             actual = 0;
             }
             readMethod.put(methodId, actual + 1);*/
            readers++;
            return readers;
        }

        public int hasBeenRead(int methodId) {
            //readMethod.put(methodId, readMethod.get(methodId) - 1);
            readers--;
            return readers;
        }

        public int getReaders() {
            return readers;
        }

        /*public Map<Integer, Integer> getMethodReaders() {
         return readMethod;
         }*/
    }
}
