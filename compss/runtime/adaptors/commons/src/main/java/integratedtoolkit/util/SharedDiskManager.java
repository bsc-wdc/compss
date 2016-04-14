package integratedtoolkit.util;

import integratedtoolkit.types.resources.Resource;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;


/**
 * The Shared Disk Manager is an utility to manage the disk shared by many
 * resources. It keeps information about which disk are mounted in a machine,
 * the path where they are mounted and which files are present on the disk.
 */
public class SharedDiskManager {

    /**
     * Relation shared disk name --> worker names where it is mounted
     */
    private static HashMap<String, LinkedList<Resource>> shared2Machines = new HashMap<String, LinkedList<Resource>>();
    /**
     * Relation resource name --> Shared disks contained
     */
    private static HashMap<Resource, Machine> machine2Shareds = new HashMap<Resource, Machine>();

    /**
     * Adds a new resource to be managed
     *
     * @param host Resource
     */
    public static void addMachine(Resource host) {
        Machine m = new Machine(host);
        synchronized (machine2Shareds) {
            machine2Shareds.put(host, m);
        }
    }

    /**
     * Links a shared disk with a resource
     *
     * @param diskName shared disk identifier
     * @param mountpoint path where the shared disk is mounted
     * @param host containing resource
     */
    public static void addSharedToMachine(String diskName, String mountpoint, Resource host) {
        synchronized (machine2Shareds) {
            machine2Shareds.get(host).addSharedDisk(diskName, mountpoint);
            LinkedList<Resource> machines = shared2Machines.get(diskName);
            if (machines == null) {
                machines = new LinkedList<Resource>();
                shared2Machines.put(diskName, machines);
            }
            machines.add(host);
        }
    }

    /**
     * Gets the name of a shared disk which contains the files in a resource
     * path
     *
     * @param host Name of the resource
     * @param path File path contained by the disk
     * @return null if there is no shared disk containing that file path on the
     * resource. The shared disk identifier containing that file path.
     */
    public static String getSharedName(Resource host, String path) {
        Machine m = machine2Shareds.get(host);
        if (m == null) {
            return null;
        }
        return m.getSharedName(path);
    }

    /**
     * Returns a string describing the current state of the shared disk
     * configuration and the files contained on them
     *
     * @return description of the current state of the shared disk configuration
     * and the files contained on them
     */
    public static String getSharedStatus() {
        StringBuilder sb = new StringBuilder("Shared disk in machines:\n");
        for (Entry<String, LinkedList<Resource>> e : shared2Machines.entrySet()) {
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
     * Returns a list with all the name of all the shared disks mounted on a
     * resource
     *
     * @param host resource
     * @return a list with all the name of all the shared disks mounted on a
     * resource
     */
    public static LinkedList<String> getAllSharedNames(Resource host) {
        Machine m = machine2Shareds.get(host);
        if (m == null) {
            return new LinkedList<String>();
        }
        return m.getAllSharedNames();
    }

    /**
     * Returns the mountpoint of a shared disk in a resource
     *
     * @param host resource
     * @param sharedDisk shared disk name
     * @return mountpoint of the shared disk in the resource
     */
    public static String getMounpoint(Resource host, String sharedDisk) {
        Machine m = machine2Shareds.get(host);
        if (m == null) {
            return null;
        }
        return m.getPath(sharedDisk);
    }

    /**
     * Returns a list of machines with a shared disk mounted
     *
     * @param diskName name of the shared disk we are looking for
     * @return list of machines with a shared disk mounted
     */
    public static LinkedList<Resource> getAllMachinesfromDisk(String diskName) {
        return shared2Machines.get(diskName);
    }

    /**
     * Removes all the information of a resource
     *
     * @param host Machine to remove
     *
     * @return returns the correlation diskName->mountpoint
     */
    public static HashMap<String, String> terminate(Resource host) {
        Machine m;
        synchronized (machine2Shareds) {
            m = machine2Shareds.remove(host);
            if (m!= null){
            	for (String sharedName : m.allShared) {
            		shared2Machines.get(sharedName).remove(host);
            	}
            }
        }
        if (m!=null){
        	return m.name2Mountpoint;
        }else{
        	return new HashMap<String, String>();
        }
    }
}

class Machine {

    Resource resource;
    LinkedList<String> allShared;
    HashMap<String, String> mountpoint2Name;
    HashMap<String, String> name2Mountpoint;

    public Machine(Resource resource) {
        this.resource = resource;
        allShared = new LinkedList<String>();
        mountpoint2Name = new HashMap<String, String>();
        name2Mountpoint = new HashMap<String, String>();
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

    public String getPath(LinkedList<String> sharedDisks) {
        for (int i = 0; i < sharedDisks.size(); i++) {
            String path = name2Mountpoint.get(sharedDisks.get(i));
            if (path != null) {
                return path;
            }
        }
        return null;
    }

    public LinkedList<String> getAllSharedNames() {
        return allShared;
    }
}
