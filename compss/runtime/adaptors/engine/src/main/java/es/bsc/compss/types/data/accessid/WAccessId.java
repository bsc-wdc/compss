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
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.DataVersion;


public class WAccessId extends DataAccessId {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    // File version written
    private DataVersion writtenDataVersion;


    /**
     * Creates a new Write Access Id for serialization.
     */
    public WAccessId() {
        // For serialization
    }

    /**
     * Creates a new Write Access Id for data id {@code dataId} and version {@code wVersionId}.
     * 
     * @param dataId Data id.
     * @param wVersionId Write version id.
     */
    public WAccessId(int dataId, int wVersionId) {
        this.writtenDataVersion = new DataVersion(dataId, wVersionId);
    }

    /**
     * Creates a new WriteAccessId with the given data version.
     * 
     * @param wdv Write version.
     */
    public WAccessId(DataVersion wdv) {
        this.writtenDataVersion = wdv;
    }

    @Override
    public int getDataId() {
        return this.writtenDataVersion.getDataInstanceId().getDataId();
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
    public boolean isWrite() {
        return true;
    }

    /**
     * Returns the written data version.
     * 
     * @return data version written
     */
    public DataVersion getWrittenDataVersion() {
        return this.writtenDataVersion;
    }

    /**
     * Returns the written data instance.
     *
     * @return The written data instance.
     */
    public DataInstanceId getWrittenDataInstance() {
        return this.writtenDataVersion.getDataInstanceId();
    }

    /**
     * Returns the write version id.
     * 
     * @return The write version id.
     */
    public int getWVersionId() {
        return this.writtenDataVersion.getDataInstanceId().getVersionId();
    }

    @Override
    public String toString() {
        return "Written data: " + this.writtenDataVersion.getDataInstanceId();
    }

}
