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
package es.bsc.compss.agent.types;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Data value location on an Agent.
 */
public class SharedRemoteDataLocation implements RemoteDataLocation {

    public static class Mountpoint implements Externalizable {

        private Resource<?, ?> resource;
        private String path;


        public Mountpoint() {
        }

        public Mountpoint(Resource<?, ?> r, String path) {
            this.resource = r;
            this.path = path;
        }

        public Resource<?, ?> getResource() {
            return resource;
        }

        public String getPath() {
            return path;
        }

        @Override
        public void writeExternal(ObjectOutput oo) throws IOException {
            oo.writeObject(this.resource);
            oo.writeUTF(this.path);
        }

        @Override
        public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
            this.resource = (Resource<?, ?>) oi.readObject();
            this.path = oi.readUTF();
        }

        @Override
        public String toString() {
            return "{\"resource\":" + (this.resource == null ? "null" : this.resource) + "," + " \"path\":" + this.path
                + "}";
        }
    }


    private String diskName;

    private String pathOnDisk;

    private Mountpoint[] mountpoints;


    /**
     * Constructor with no parameters for Externalizable serialization.
     */
    public SharedRemoteDataLocation() {
        this.diskName = null;
        this.pathOnDisk = null;
        this.mountpoints = new Mountpoint[0];
    }

    /**
     * Constructs a new instance of a shared remote location based on the diskname and the path of the file in it.
     * 
     * @param sharedDisk name of the disk
     * @param pathOnDisk path of data in the sahred disk
     * @param mountpoints mountpoints of the shared disk for all known resources
     */
    public SharedRemoteDataLocation(String sharedDisk, String pathOnDisk, Mountpoint[] mountpoints) {
        this.diskName = sharedDisk;
        this.pathOnDisk = pathOnDisk;
        this.mountpoints = mountpoints;
    }

    @Override
    public Type getType() {
        return Type.SHARED;
    }

    public String getDiskName() {
        return diskName;
    }

    public String getPathOnDisk() {
        return pathOnDisk;
    }

    /**
     * Returns an array with all the known private locations for that shared disk.
     * 
     * @return all the known locations for that shared disk as private locations
     */
    public PrivateRemoteDataLocation[] getAllPrivateLocations() {
        PrivateRemoteDataLocation[] locs = new PrivateRemoteDataLocation[this.mountpoints.length];
        for (int i = 0; i < this.mountpoints.length; i++) {
            Mountpoint m = this.mountpoints[i];
            locs[i] = new PrivateRemoteDataLocation(m.resource, m.path + this.pathOnDisk);
        }
        return locs;
    }

    /**
     * Returns an array with the mountPoint for all the known nodes that have the shared disk mounted.
     * 
     * @return mountPoint for all the known nodes that have the shared disk mounted
     */
    public Mountpoint[] getMountpoints() {
        return mountpoints;
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        oo.writeUTF(this.diskName);
        oo.writeUTF(this.pathOnDisk);
        oo.writeInt(this.mountpoints.length);
        for (Mountpoint m : this.mountpoints) {
            oo.writeObject(m);
        }
    }

    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        this.diskName = oi.readUTF();
        this.pathOnDisk = oi.readUTF();
        int numMountpoints = oi.readInt();
        this.mountpoints = new Mountpoint[numMountpoints];
        for (int i = 0; i < numMountpoints; i++) {
            this.mountpoints[i] = (Mountpoint) oi.readObject();
        }
    }

    @Override
    public String toString() {
        StringBuilder desc = new StringBuilder(
            "{\"disk\":\"" + this.diskName + "\"," + " \"path\":\"" + this.pathOnDisk + "\"," + "\"mountpoints\":[");

        for (Mountpoint m : this.mountpoints) {
            desc.append(m.toString() + ",");
        }
        desc.append("]" + "}");
        return desc.toString();
    }
}
