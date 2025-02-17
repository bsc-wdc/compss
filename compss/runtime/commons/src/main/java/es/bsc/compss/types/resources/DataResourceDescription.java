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
package es.bsc.compss.types.resources;

import es.bsc.compss.types.implementations.Implementation;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Data Node representation.
 */
public class DataResourceDescription extends ResourceDescription {

    // Unassigned values
    // !!!!!!!!!! WARNING: Coherent with constraints class
    public static final String UNASSIGNED_STR = "[unassigned]";
    public static final int UNASSIGNED_INT = -1;
    public static final float UNASSIGNED_FLOAT = (float) -1.0;

    // Required DataNode information
    private final String host;
    private final String path;
    // Optional information: Storage
    protected float storageSize = UNASSIGNED_FLOAT;
    protected String storageType = UNASSIGNED_STR;
    protected int storageBW = UNASSIGNED_INT;


    /**
     * New empty data Resource Description.
     */
    public DataResourceDescription() {
        // Only for externalization
        super();
        this.host = "";
        this.path = "";
    }

    /**
     * New Data Resource description with host {@code host} and path {@code path}.
     *
     * @param host Resource host.
     * @param path Resource path.
     */
    public DataResourceDescription(String host, String path) {
        super();
        this.host = host;
        this.path = path;
    }

    /**
     * Clone method for Data Resource Description.
     *
     * @param clone DataResourceDescription to clone.
     */
    public DataResourceDescription(DataResourceDescription clone) {
        super(clone);

        this.host = clone.host;
        this.path = clone.path;
        this.storageSize = clone.storageSize;
        this.storageType = clone.storageType;
    }

    @Override
    public DataResourceDescription copy() {
        return new DataResourceDescription(this);
    }

    /**
     * Returns the host.
     *
     * @return The host.
     */
    public String getHost() {
        return this.host;
    }

    /**
     * Returns the path.
     *
     * @return The path.
     */
    public String getPath() {
        return this.path;
    }

    /**
     * Returns the storage size.
     *
     * @return The storage size.
     */
    public float getStorageSize() {
        return this.storageSize;
    }

    /**
     * Sets a new storage size.
     *
     * @param storageSize New storage size.
     */
    public void setStorageSize(float storageSize) {
        if (storageSize > (float) 0.0) {
            this.storageSize = storageSize;
        }
    }

    /**
     * Returns the storage type.
     *
     * @return The storage type.
     */
    public String getStorageType() {
        return this.storageType;
    }

    /**
     * Sets a new storage type.
     *
     * @param storageType New storage type.
     */
    public void setStorageType(String storageType) {
        if (storageType != null) {
            this.storageType = storageType;
        }
    }

    /**
     * Sets a new storage bandwidth.
     *
     * @param storageBW New storage bandwidth.
     */
    public void setStorageBW(int storageBW) {
        if (storageBW != -1) {
            this.storageBW = storageBW;
        }
    }

    /**
     * Returns storage bandwidth.
     */
    public int getStorageBW() {
        return this.storageBW;
    }

    @Override
    public boolean canHost(Implementation impl) {
        // DataNodes can not run any implementation
        return false;
    }

    @Override
    public void mimic(ResourceDescription rd) {
        DataResourceDescription drd = (DataResourceDescription) rd;
        this.storageSize = drd.storageSize;
        this.storageType = drd.storageType;
    }

    @Override
    public void increase(ResourceDescription rd) {
        // A DataNode cannot be increased nor decreased, nothing to do
    }

    @Override
    public void reduce(ResourceDescription rd) {
        // A DataNode cannot be increased nor decreased, nothing to do
    }

    @Override
    public boolean canHostDynamic(Implementation impl) {
        // DataNodes can not run any implementation
        return false;
    }

    @Override
    public ResourceDescription getDynamicCommons(ResourceDescription constraints) {
        // There is nothing common between two dataNodes
        return null;
    }

    @Override
    public void increaseDynamic(ResourceDescription rd) {
        // A DataNode cannot be increased nor decreased, nothing to do
    }

    @Override
    public ResourceDescription reduceDynamic(ResourceDescription rd) {
        // A DataNode cannot be increased nor decreased, nothing to do
        return null;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // Nothing to serialize since it is never used
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        // Nothing to serialize since it is never used
    }

    @Override
    public boolean isDynamicUseless() {
        // A DataNode cannot be useless
        return false;
    }

    @Override
    public boolean isDynamicConsuming() {
        // A DataNode cannot be useless
        return false;
    }

    @Override
    public String toString() {
        return "{" + "\"host\":\"" + this.host + "\"" + "\"path\":\"" + this.path + "\"" + "}";
    }

    @Override
    public String getDynamicDescription() {
        return "{}";
    }

    @Override
    public boolean usesCPUs() {
        return false;
    }
}
