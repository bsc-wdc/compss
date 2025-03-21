/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.data.location;

import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.resources.Resource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


/**
 * Class representing a shared disk.
 */
public class SharedDisk {

    private static Map<String, SharedDisk> ALL_DISKS = new TreeMap<>();


    /**
     * Constructs a new Shared disk with the given name.
     * 
     * @param name name of the disk
     */
    public static SharedDisk createDisk(String name) {
        SharedDisk disk;
        synchronized (ALL_DISKS) {
            disk = ALL_DISKS.get(name);
            if (disk == null) {
                disk = new SharedDisk(name);
                ALL_DISKS.put(name, disk);
            }
        }
        return disk;
    }


    private final String name;
    private final List<Resource> resources;
    private final Map<Resource, String> mountpoints;

    /**
     * LogicalData stored in any sharedDisk.
     */
    private final Set<LogicalData> sharedFiles;


    private SharedDisk(String name) {
        this.name = name;
        this.resources = new LinkedList<>();
        this.mountpoints = new HashMap<>();
        this.sharedFiles = new HashSet<>();
    }

    /**
     * Returns the name of the disk.
     * 
     * @return name of the disk
     */
    public String getName() {
        return name;
    }

    /**
     * Sets up a new host where the shared disk is mounted.
     * 
     * @param host host where it was mounted
     * @param path mountpoint within the host
     */
    public synchronized void addMountpoint(Resource host, String path) {
        this.resources.add(host);
        this.mountpoints.put(host, path);
    }

    /**
     * Returns the mountpoint of the disk in a certain resource.
     * 
     * @param r resource to query
     * @return mountpoint of the disk in resource r.
     */
    public synchronized String getMountpoint(Resource r) {
        return this.mountpoints.get(r);
    }

    /**
     * Removes the mountpoint for a resource fromthe disk.
     *
     * @param r resource to query
     * @return mountpoint of the disk in resource r.
     */
    public synchronized String removeMountpoint(Resource r) {
        this.resources.remove(r);
        return this.mountpoints.remove(r);
    }

    /**
     * Returns a list of machines with the shared disk mounted.
     *
     * @return list of machines with the shared disk mounted
     */
    public List<Resource> getAllResources() {
        return this.resources;
    }

    public Map<Resource, String> getAllMountpoints() {
        return this.mountpoints;
    }

    /**
     * Adds a LogicalData to the disk.
     *
     * @param ld Logical data
     */
    public void addLogicalData(LogicalData ld) {
        synchronized (this.sharedFiles) {
            this.sharedFiles.add(ld);
        }
    }

    /**
     * Removes all the obsolete logical data appearances in the shared disk. It doesn't have any effect if the
     * logicalData don't exist
     *
     * @param obsolete obsoleted logical data
     */
    public void removeLogicalData(LogicalData obsolete) {
        synchronized (this.sharedFiles) {
            this.sharedFiles.remove(obsolete);
        }
    }

    /**
     * Recovers all the data of a given sharedDisk.
     *
     * @return all files in the shared disk
     */
    public Set<LogicalData> getAllSharedFiles() {
        return this.sharedFiles;
    }
}
