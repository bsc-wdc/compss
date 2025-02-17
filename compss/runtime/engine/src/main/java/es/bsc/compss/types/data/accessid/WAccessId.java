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
import es.bsc.compss.types.data.accessid.EngineDataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.DataVersion;


public class WAccessId extends EngineDataAccessId implements WritingDataAccessId {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    // File version written
    private DataVersion writtenDataVersion;


    /**
     * Creates a new ReadWrite Access Id.
     */
    public WAccessId() {
        super();
        // To enact placeholder RW access for commutative accesses.
    }

    /**
     * Creates a new WriteAccessId with the given data version.
     *
     * @param data data being accessed.
     * @param wdv Write version.
     */
    public WAccessId(DataInfo data, DataVersion wdv) {
        super(data);
        this.writtenDataVersion = wdv;
    }

    @Override
    public Direction getDirection() {
        return Direction.W;
    }

    @Override
    public boolean isPreserveSourceData() {
        return false;
    }

    @Override
    public boolean isRead() {
        return false;
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
        return "Written data: " + this.writtenDataVersion.getDataInstanceId();
    }

    @Override
    public EngineDataAccessId consolidateValidVersions() {
        return this;
    }

    @Override
    public String toDebugString() {
        int dataId = this.getDataId();
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"type\":\"W\",");
        sb.append("\"write_datum\":\"d").append(dataId).append("v").append(this.getWVersionId()).append("\"");
        sb.append("}");
        return sb.toString();
    }

}
