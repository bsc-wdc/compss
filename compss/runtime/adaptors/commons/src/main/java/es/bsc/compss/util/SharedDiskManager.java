package es.bsc.compss.util;

import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.resources.Resource;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;


/**
 * The Shared Disk Manager is an utility to manage the disk shared by many resources. It keeps information about which
 * disk are mounted in a machine, the path where they are mounted and which files are present on the disk.
 */
public class SharedDiskManager {

    /**
     * Relation shared disk name --> worker names where it is mounted
     */
    private static final Map<String, List<Resource>> shared2Machines = new HashMap<>();

    /**
     * Relation resource name --> Shared disks contained
     */
    private static final Map<Resource, Machine> machine2Shareds = new HashMap<>();

    /**
     * LogicalData stored in any sharedDisk
     */
    private static final Map<String, Set<LogicalData>> sharedDisk2SharedFiles = new TreeMap<>();


    /**
     * Adds a new resource to be managed
     *
     * @param host
     *            Resource
     */
    public static synchronized void addMachine(Resource host) {
        Machine m = new Machine();
        machine2Shareds.put(host, m);
    }

    /**
     * Links a shared disk with a resource
     *
     * @param diskName
     *            shared disk identifier
     * @param mountpoint
     *            path where the shared disk is mounted
     * @param host
     *            containing resource
     */
    public static synchronized void addSharedToMachine(String diskName, String mountpoint, Resource host) {
        machine2Shareds.get(host).addSharedDisk(diskName, mountpoint);
        List<Resource> machines = shared2Machines.get(diskName);
        if (machines == null) {
            machines = new LinkedList<Resource>();
            shared2Machines.put(diskName, machines);
        }
        synchronized(machines){
        	machines.add(host);
        }
    }

    /**
     * Gets the name of a shared disk which contains the files in a resource path
     *
     * @param host
     *            Name of the resource
     * @param path
     *            File path contained by the disk
     * @return null if there is no shared disk containing that file path on the resource. The shared disk identifier
     *         containing that file path.
     */
    public static synchronized String getSharedName(Resource host, String path) {
        Machine m = machine2Shareds.get(host);
        if (m == null) {
            return null;
        }
        return m.getSharedName(path);
    }

    /**
     * Returns a string describing the current state of the shared disk configuration and the files contained on them
     *
     * @return description of the current state of the shared disk configuration and the files contained on them
     */
    public synchronized static String getSharedStatus() {
        StringBuilder sb = new StringBuilder("Shared disk in machines:\n");
        for (Entry<String, List<Resource>> e : shared2Machines.entrySet()) {
            sb.append(e.getKey()).append("--> {");
            for (int i = 0; i < e.getValue().size(); i++) {
                sb.append(e.getValue().get(i).getName()).append(", ");
            }
            sb.append("}\n");
        }

        sb.append("Machines :\n");
        for (Entry<Resource, Machine> e : machine2Shareds.entrySet()) {
            sb.append(e.getKey().getName()).append("--> {");
            for (Entry<String, String> me : e.getValue().name2Mountpoint.entrySet()) {
                sb.append(me.getKey()).append("@").append(me.getValue()).append(", ");
            }
            sb.append("}\n");
        }

        return sb.toString();
    }

    /**
     * Returns a list with all the name of all the shared disks mounted on a resource
     *
     * @param host
     *            resource
     * @return a list with all the name of all the shared disks mounted on a resource
     */
    public static synchronized List<String> getAllSharedNames(Resource host) {
        Machine m = machine2Shareds.get(host);
        if (m == null) {
            return new LinkedList<String>();
        }
        return m.getAllSharedNames();
    }

    /**
     * Returns the mountpoint of a shared disk in a resource
     *
     * @param host
     *            resource
     * @param sharedDisk
     *            shared disk name
     * @return mountpoint of the shared disk in the resource
     */
    public static synchronized String getMounpoint(Resource host, String sharedDisk) {
        Machine m = machine2Shareds.get(host);
        if (m == null) {
            return null;
        }
        return m.getPath(sharedDisk);
    }

    /**
     * Returns a list of machines with a shared disk mounted
     *
     * @param diskName
     *            name of the shared disk we are looking for
     * @return list of machines with a shared disk mounted
     */
    public static synchronized List<Resource> getAllMachinesfromDisk(String diskName) {
        return shared2Machines.get(diskName);
    }

    /**
     * Removes all the information of a resource
     *
     * @param host
     *            Machine to remove
     *
     * @return returns the correlation diskName->mountpoint
     */
    public static synchronized Map<String, String> terminate(Resource host) {
        Machine m;
        m = machine2Shareds.remove(host);
        if (m != null) {
            for (String sharedName : m.allShared) {
            	List<Resource> machines = shared2Machines.get(sharedName);
            	synchronized(machines){
            		machines.remove(host);
            	}
            }
        }
        if (m != null) {
            return m.name2Mountpoint;
        } else {
            return new HashMap<String, String>();
        }
    }

    /**
     * Adds a LogicalData to a diskName
     * 
     * @param diskName
     * @param ld
     */
    public static synchronized void addLogicalData(String diskName, LogicalData ld) {
        Set<LogicalData> lds = null;
        if (sharedDisk2SharedFiles.containsKey(diskName)) {
            lds = sharedDisk2SharedFiles.get(diskName);
        } else {
            lds = new HashSet<>();
        }
        lds.add(ld);
        sharedDisk2SharedFiles.put(diskName, lds);
    }

    /**
     * Removes all the obsolete logical data appearances in the given shared disk. It doesn't have any effect if the
     * diskName or the logicalData don't exist
     * 
     * @param r
     * @param obsolete
     */
    public static synchronized void removeLogicalData(String diskName, LogicalData obsolete) {
        Set<LogicalData> lds = sharedDisk2SharedFiles.get(diskName);
        if (lds != null) {
            lds.remove(obsolete);
        }
    }

    /**
     * Recovers all the data of a given sharedDisk
     * 
     * @param diskName
     * @return
     */
    public static synchronized Set<LogicalData> getAllSharedFiles(String diskName) {
        Set<LogicalData> lds = sharedDisk2SharedFiles.get(diskName);
        return lds;
    }


    private static class Machine {

        private List<String> allShared;
        private HashMap<String, String> mountpoint2Name;
        private HashMap<String, String> name2Mountpoint;


        public Machine() {
            allShared = new LinkedList<>();
            mountpoint2Name = new HashMap<>();
            name2Mountpoint = new HashMap<>();
        }

        public void addSharedDisk(String diskName, String mountpoint) {
            allShared.add(diskName);
            if (!mountpoint.endsWith(File.separator)) {
                mountpoint += File.separator;
            }
            mountpoint2Name.put(mountpoint, diskName);
            name2Mountpoint.put(diskName, mountpoint);
        }

        public String getSharedName(String path) {
            if (path == null) {
                return null;
            }
            for (Entry<String, String> e : mountpoint2Name.entrySet()) {
                if (path.startsWith(e.getKey())) {
                    return e.getValue();
                }
            }
            return null;
        }

        public String getPath(String sharedDisk) {
            return name2Mountpoint.get(sharedDisk);
        }

        public List<String> getAllSharedNames() {
            return allShared;
        }
    }

}
