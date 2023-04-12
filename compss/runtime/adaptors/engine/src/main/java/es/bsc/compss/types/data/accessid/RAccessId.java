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
package es.bsc.compss.types.data.accessid;

import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.DataVersion;


public class RAccessId implements ReadingDataAccessId {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    // File version read
    // private DataInstanceId readDataInstance;
    private DataVersion readDataVersion;


    /**
     * Creates a new Read Access Id for serialization.
     */
    public RAccessId() {
        // For serialization
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

    @Override
    public boolean isPreserveSourceData() {
        return this.readDataVersion.hasMoreReaders() || this.readDataVersion.getDataInstanceId().getVersionId() == 1;
    }

    @Override
    public boolean isRead() {
        return true;
    }

    @Override
    public DataVersion getReadDataVersion() {
        return this.readDataVersion;
    }

    @Override
    public DataInstanceId getReadDataInstance() {
        return this.readDataVersion.getDataInstanceId();
    }

    @Override
    public int getRVersionId() {
        return this.readDataVersion.getDataInstanceId().getVersionId();
    }

    @Override
    public boolean isWrite() {
        return false;
    }

    @Override
    public String toString() {
        return "Read data: " + this.readDataVersion.getDataInstanceId()
            + (isPreserveSourceData() ? ", Preserved" : ", Erased");
    }

    @Override
    public DataAccessId consolidateValidVersions() {
        if (!this.readDataVersion.isValid()) {
            DataVersion validR = this.readDataVersion.getPreviousValidPredecessor();
            if (validR != null) {
                return new RAccessId(validR);
            } else {
                return null;
            }
        }
        return this;
    }

}
