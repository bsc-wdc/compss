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
package es.bsc.compss.types.data.accessid;

import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.DataVersion;


public class RWAccessId extends EngineDataAccessId implements ReadingDataAccessId, WritingDataAccessId {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    // Data version read
    private DataVersion readDataVersion;
    // Data version written
    private DataVersion writtenDataVersion;


    /**
     * Creates a new ReadWrite Access Id.
     */
    public RWAccessId() {
        super();
        // To enact placeholder RW access for commutative accesses.
    }

    /**
     * Creates a new ReadWrite Access Id with read version {@code rdv} and write version {@code wdv}.
     *
     * @param data data being accessed.
     * @param rdv Read version.
     * @param wdv Write version.
     */
    public RWAccessId(DataInfo data, DataVersion rdv, DataVersion wdv) {
        super(data);
        this.readDataVersion = rdv;
        this.writtenDataVersion = wdv;
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
    public EngineDataInstanceId getReadDataInstance() {
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
    public EngineDataInstanceId getWrittenDataInstance() {
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
    public EngineDataAccessId consolidateValidVersions() {
        if (!this.readDataVersion.isValid()) {
            DataVersion validR = this.readDataVersion.getPreviousValidPredecessor();
            if (validR != null) {
                return new RWAccessId(this.getAccessedDataInfo(), validR, this.writtenDataVersion);
            } else {
                return null;
            }
        }
        return this;
    }

    @Override
    public String toDebugString() {
        int dataId = this.getDataId();
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"type\":\"RW\",");
        sb.append("\"read_datum\":\"d").append(dataId).append("v").append(this.getRVersionId()).append("\"");
        sb.append("\"write_datum\":\"d").append(dataId).append("v").append(this.getWVersionId()).append("\"");
        sb.append("}");
        return sb.toString();
    }

}
