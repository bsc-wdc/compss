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
import es.bsc.compss.types.data.DataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.DataVersion;


public class RWAccessId implements ReadingDataAccessId, WritingDataAccessId {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    // File version read
    private DataVersion readDataVersion;
    // File version written
    private DataVersion writtenDataVersion;


    /**
     * Creates a new ReadWrite Access Id for serialization.
     */
    public RWAccessId() {
        // For serialization
    }

    /**
     * Creates a new ReadWrite Access Id with read version {@code rdv} and write version {@code wdv}.
     * 
     * @param rdv Read version.
     * @param wdv Write version.
     */
    public RWAccessId(DataVersion rdv, DataVersion wdv) {
        this.readDataVersion = rdv;
        this.writtenDataVersion = wdv;
    }

    @Override
    public int getDataId() {
        return this.readDataVersion.getDataInstanceId().getDataId();
    }

    @Override
    public Direction getDirection() {
        return Direction.RW;
    }

    @Override
    public boolean isPreserveSourceData() {
        return this.readDataVersion.hasMoreReaders();
    }

    @Override
    public boolean isRead() {
        return true;
    }

    @Override
    public int getRVersionId() {
        return this.readDataVersion.getDataInstanceId().getVersionId();
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
    public boolean isWrite() {
        return true;
    }

    @Override
    public DataVersion getWrittenDataVersion() {
        return this.writtenDataVersion;
    }

    @Override
    public DataInstanceId getWrittenDataInstance() {
        return this.writtenDataVersion.getDataInstanceId();
    }

    @Override
    public int getWVersionId() {
        return this.writtenDataVersion.getDataInstanceId().getVersionId();
    }

    @Override
    public String toString() {
        return "Read data: " + this.readDataVersion.getDataInstanceId() + ", Written data: "
            + this.writtenDataVersion.getDataInstanceId() + (isPreserveSourceData() ? ", Preserved" : ", Erased");
    }

    @Override
    public DataAccessId consolidateValidVersions() {
        if (!this.readDataVersion.isValid()) {
            DataVersion validR = this.readDataVersion.getPreviousValidPredecessor();
            if (validR != null) {
                return new RWAccessId(validR, this.writtenDataVersion);
            } else {
                return null;
            }
        }
        return this;
    }

}
