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
package es.bsc.compss.types.data.location;

import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.util.SharedDiskManager;

import java.io.File;
import java.util.LinkedList;
import java.util.List;


public class SharedLocation extends DataLocation {

    private final String diskName;
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
        this.path = path;
        this.protocol = protocol;
    }

    @Override
    public MultiURI getURIInHost(Resource host) {
        String diskPath = SharedDiskManager.getMounpoint(host, this.diskName);
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
        List<Resource> resList = SharedDiskManager.getAllMachinesfromDisk(this.diskName);
        Resource[] resources;
        synchronized (resList) {
            resources = resList.toArray(new Resource[resList.size()]);
        }
        for (Resource host : resources) {
            String diskPath = SharedDiskManager.getMounpoint(host, this.diskName);
            if (!diskPath.endsWith(File.separator)) {
                diskPath = diskPath + File.separator;
            }

            uris.add(new MultiURI(this.protocol, host, diskPath + this.path));
        }

        return uris;
    }

    @Override
    public List<Resource> getHosts() {
        return SharedDiskManager.getAllMachinesfromDisk(this.diskName);
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
    public String getSharedDisk() {
        return this.diskName;
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
        return "shared:" + this.diskName + File.separator + this.path;
    }

    @Override
    public void modifyPath(String path) {
        this.path = path;

    }

}
