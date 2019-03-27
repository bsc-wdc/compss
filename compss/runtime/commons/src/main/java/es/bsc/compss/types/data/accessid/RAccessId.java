/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.data.accessid;

import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.DataVersion;


public class RAccessId extends DataAccessId {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    // File version read
    // private DataInstanceId readDataInstance;
    private DataVersion readDataVersion;
    // Source data preservation flag
    private boolean preserveSourceData = true;


    /**
     * Creates a new Read Access Id for serialization.
     */
    public RAccessId() {
        // For serialization
    }

    /**
     * Creates a new Read Access Id for data id {@code dataId} and version {@code rVersionId}.
     * 
     * @param dataId Data id.
     * @param rVersionId Read version id.
     */
    public RAccessId(int dataId, int rVersionId) {
        this.readDataVersion = new DataVersion(dataId, rVersionId);
    }

    /**
     * Sets a new data version.
     * 
     * @param rdv New data version.
     */
    public RAccessId(DataVersion rdv) {
        this.readDataVersion = rdv;
    }

    @Override
    public int getDataId() {
        return this.readDataVersion.getDataInstanceId().getDataId();
    }

    @Override
    public Direction getDirection() {
        return Direction.R;
    }

    /**
     * Returns the read data instance.
     * 
     * @return The read data instance.
     */
    public DataInstanceId getReadDataInstance() {
        return this.readDataVersion.getDataInstanceId();
    }

    /**
     * Returns the read version id.
     * 
     * @return The read version id.
     */
    public int getRVersionId() {
        return this.readDataVersion.getDataInstanceId().getVersionId();
    }

    /**
     * Returns whether the source data must be preserved or not.
     * 
     * @return {@code true} if the source data must be preserved, {@code false} otherwise.
     */
    public boolean isPreserveSourceData() {
        return this.preserveSourceData;
    }

    @Override
    public String toString() {
        return "Read data: " + this.readDataVersion.getDataInstanceId()
                + (this.preserveSourceData ? ", Preserved" : ", Erased");
    }

}
