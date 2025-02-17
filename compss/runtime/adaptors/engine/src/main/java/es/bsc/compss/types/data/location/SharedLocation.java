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

import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.uri.MultiURI;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class SharedLocation extends DataLocation {

    private final String diskName;
    private final SharedDisk disk;
    private String path;
    private final ProtocolType protocol;


    /**
     * Shared location constructor.
     * 
     * @param protocol Protocol
     * @param sharedDisk Shared disk name/identifier
     * @param path Path from mount point
     */
    public SharedLocation(ProtocolType protocol, String sharedDisk, String path) {
        this.diskName = sharedDisk;
        this.disk = SharedDisk.createDisk(sharedDisk);
        this.path = path;
        this.protocol = protocol;
    }

    /**
     * Shared location constructor.
     *
     * @param protocol Protocol
     * @param sharedDisk Shared disk
     * @param path Path from mount point
     */
    public SharedLocation(ProtocolType protocol, SharedDisk sharedDisk, String path) {
        this.disk = sharedDisk;
        this.diskName = sharedDisk.getName();
        this.path = path;
        this.protocol = protocol;
    }

    @Override
    public MultiURI getURIInHost(Resource host) {
        String diskPath = this.disk.getMountpoint(host);
        if (diskPath == null) {
            return null;
        }
        if (!diskPath.endsWith(File.separator)) {
            diskPath = diskPath + File.separator;
        }

        return new MultiURI(this.protocol, host, diskPath + this.path);
    }

    @Override
    public LocationType getType() {
        return LocationType.SHARED;
    }

    @Override
    public ProtocolType getProtocol() {
        return this.protocol;
    }

    @Override
    public List<MultiURI> getURIs() {
        List<MultiURI> uris = new LinkedList<>();

        Map<Resource, String> resList = this.disk.getAllMountpoints();
        if (resList == null) {
            return uris;
        }
        Resource[] resources;
        String[] mountpoints;
        int size = 0;
        synchronized (resList) {
            if (resList.isEmpty()) {
                return uris;
            }

            size = resList.size();
            resources = new Resource[size];
            mountpoints = new String[size];
            int idx = 0;
            for (Map.Entry<Resource, String> e : resList.entrySet()) {
                resources[idx] = e.getKey();
                mountpoints[idx] = e.getValue();
                idx++;
            }

        }
        for (int i = 0; i < size; i++) {
            Resource host = resources[i];
            String diskPath = mountpoints[i];
            if (!diskPath.endsWith(File.separator)) {
                diskPath = diskPath + File.separator;
            }

            uris.add(new MultiURI(this.protocol, host, diskPath + this.path));
        }

        return uris;
    }

    @Override
    public List<Resource> getHosts() {
        return this.disk.getAllResources();
    }

    @Override
    public boolean isTarget(DataLocation target) {
        String targetDisk;
        String targetPath;
        if (target.getType().equals(LocationType.PRIVATE)) {
            PrivateLocation privateLoc = (PrivateLocation) target;
            targetDisk = null; // TODO: extract from URI
            targetPath = privateLoc.getPath();
        } else {
            SharedLocation sharedloc = (SharedLocation) target;
            targetDisk = sharedloc.diskName;
            targetPath = sharedloc.path;
        }
        return (targetDisk != null && targetDisk.contentEquals(this.diskName) && targetPath.contentEquals(targetPath));
    }

    @Override
    public SharedDisk getSharedDisk() {
        return this.disk;
    }

    @Override
    public String getPath() {
        return this.path;
    }

    @Override
    public String getLocationKey() {
        return this.path + ":shared:" + this.diskName;
    }

    @Override
    public int compareTo(DataLocation o) {
        if (o == null) {
            throw new NullPointerException();
        }
        if (o.getClass() != SharedLocation.class) {
            return (this.getClass().getName()).compareTo(SharedLocation.class.toString());
        } else {
            SharedLocation sl = (SharedLocation) o;
            int compare = this.diskName.compareTo(sl.diskName);
            if (compare == 0) {
                compare = this.path.compareTo(sl.path);
            }
            return compare;
        }
    }

    @Override
    public String toString() {
        return "shared://" + this.diskName + File.separator + this.path;
    }

    @Override
    public void modifyPath(String path) {
        this.path = path;

    }

}
