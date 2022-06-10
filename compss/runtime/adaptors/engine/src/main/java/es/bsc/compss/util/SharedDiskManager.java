/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
     * Relation shared disk name --> worker names where it is mounted.
     */
    private static final Map<String, List<Resource>> SHARED_TO_MACHINES = new HashMap<>();

    /**
     * Relation resource name --> Shared disks contained.
     */
    private static final Map<Resource, Machine> MACHINE_TO_SHARED = new HashMap<>();

    /**
     * LogicalData stored in any sharedDisk.
     */
    private static final Map<String, Set<LogicalData>> SHARED_DISK_TO_SHARED_FILES = new TreeMap<>();


    /**
     * Adds a new resource to be managed.
     *
     * @param host Resource
     */
    public static synchronized void addMachine(Resource host) {
        Machine m = new Machine();
        MACHINE_TO_SHARED.put(host, m);
    }

    /**
     * Links a shared disk with a resource.
     *
     * @param diskName shared disk identifier
     * @param mountpoint path where the shared disk is mounted
     * @param host containing resource
     */
    public static synchronized void addSharedToMachine(String diskName, String mountpoint, Resource host) {
        Machine resource = MACHINE_TO_SHARED.get(host);
        if (resource != null) {
            resource.addSharedDisk(diskName, mountpoint);
            List<Resource> machines = SHARED_TO_MACHINES.get(diskName);
            if (machines == null) {
                machines = new LinkedList<>();
                SHARED_TO_MACHINES.put(diskName, machines);
            }
            synchronized (machines) {
                machines.add(host);
            }
        } else {
            ErrorManager.warn("Host " + host.getName() + " not registered as machine in the Shared disk manager.");
        }
    }

    /**
     * Gets the name of a shared disk which contains the files in a resource path.
     *
     * @param host Name of the resource
     * @param path File path contained by the disk
     * @return null if there is no shared disk containing that file path on the resource. The shared disk identifier
     *         containing that file path.
     */
    public static synchronized String getSharedName(Resource host, String path) {
        Machine m = MACHINE_TO_SHARED.get(host);
        if (m == null) {
            return null;
        }
        return m.getSharedName(path);
    }

    /**
     * Returns a string describing the current state of the shared disk configuration and the files contained on them.
     *
     * @return description of the current state of the shared disk configuration and the files contained on them.
     */
    public static synchronized String getSharedStatus() {
        StringBuilder sb = new StringBuilder("Shared disk in machines:\n");
        for (Entry<String, List<Resource>> e : SHARED_TO_MACHINES.entrySet()) {
            sb.append(e.getKey()).append("--> {");
            for (int i = 0; i < e.getValue().size(); i++) {
                sb.append(e.getValue().get(i).getName()).append(", ");
            }
            sb.append("}\n");
        }

        sb.append("Machines :\n");
        for (Entry<Resource, Machine> e : MACHINE_TO_SHARED.entrySet()) {
            sb.append(e.getKey().getName()).append("--> {");
            for (Entry<String, String> me : e.getValue().name2Mountpoint.entrySet()) {
                sb.append(me.getKey()).append("@").append(me.getValue()).append(", ");
            }
            sb.append("}\n");
        }

        return sb.toString();
    }

    /**
     * Returns a list with all the name of all the shared disks mounted on a resource.
     *
     * @param host resource
     * @return a list with all the name of all the shared disks mounted on a resource
     */
    public static synchronized List<String> getAllSharedNames(Resource host) {
        Machine m = MACHINE_TO_SHARED.get(host);
        if (m == null) {
            return new LinkedList<>();
        }
        return m.getAllSharedNames();
    }

    /**
     * Returns the mountpoint of a shared disk in a resource.
     *
     * @param host resource
     * @param sharedDisk shared disk name
     * @return mountpoint of the shared disk in the resource
     */
    public static synchronized String getMounpoint(Resource host, String sharedDisk) {
        Machine m = MACHINE_TO_SHARED.get(host);
        if (m == null) {
            return null;
        }
        return m.getPath(sharedDisk);
    }

    /**
     * Returns a list of machines with a shared disk mounted.
     *
     * @param diskName name of the shared disk we are looking for
     * @return list of machines with a shared disk mounted
     */
    public static synchronized List<Resource> getAllMachinesfromDisk(String diskName) {
        return SHARED_TO_MACHINES.get(diskName);
    }

    /**
     * Removes all the information of a resource.
     *
     * @param host Machine to remove
     * @return returns the correlation diskName->mountpoint
     */
    public static synchronized Map<String, String> terminate(Resource host) {
        Machine m;
        m = MACHINE_TO_SHARED.remove(host);
        if (m != null) {
            for (String sharedName : m.allShared) {
                List<Resource> machines = SHARED_TO_MACHINES.get(sharedName);
                synchronized (machines) {
                    machines.remove(host);
                }
            }
        }
        if (m != null) {
            return m.name2Mountpoint;
        } else {
            return new HashMap<>();
        }
    }

    /**
     * Adds a LogicalData to a diskName.
     *
     * @param diskName Disk name
     * @param ld Logical data
     */
    public static synchronized void addLogicalData(String diskName, LogicalData ld) {
        Set<LogicalData> lds = SHARED_DISK_TO_SHARED_FILES.get(diskName);
        if (lds == null) {
            lds = new HashSet<>();
            SHARED_DISK_TO_SHARED_FILES.put(diskName, lds);
        }
        synchronized (lds) {
            lds.add(ld);
        }
    }

    /**
     * Removes all the obsolete logical data appearances in the given shared disk. It doesn't have any effect if the
     * diskName or the logicalData don't exist
     *
     * @param diskName Shared disk name
     * @param obsolete obsoleted logical data
     */
    public static synchronized void removeLogicalData(String diskName, LogicalData obsolete) {
        Set<LogicalData> lds = SHARED_DISK_TO_SHARED_FILES.get(diskName);
        if (lds != null) {
            synchronized (lds) {
                lds.remove(obsolete);
            }
        }
    }

    /**
     * Recovers all the data of a given sharedDisk.
     *
     * @param diskName Shared Disk name
     * @return
     */
    public static synchronized Set<LogicalData> getAllSharedFiles(String diskName) {
        Set<LogicalData> lds = SHARED_DISK_TO_SHARED_FILES.get(diskName);
        return lds;
    }


    private static class Machine {

        private final List<String> allShared;
        private final HashMap<String, String> mountpoint2Name;
        private final HashMap<String, String> name2Mountpoint;


        public Machine() {
            allShared = new LinkedList<>();
            mountpoint2Name = new HashMap<>();
            name2Mountpoint = new HashMap<>();
        }

        public void addSharedDisk(String diskName, String mountpoint) {
            if (!allShared.contains(diskName)) {
                allShared.add(diskName);
            }
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
