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

import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.DataVersion;


public abstract class EngineDataAccessId implements DataAccessId {

    public interface ReadingDataAccessId extends DataAccessId.ReadingDataAccessId {

        @Override
        EngineDataInstanceId getReadDataInstance();

        /**
         * Returns the data version read by the access.
         *
         * @return data version read
         */
        DataVersion getReadDataVersion();
    }

    public interface WritingDataAccessId extends DataAccessId.WritingDataAccessId {

        @Override
        EngineDataInstanceId getWrittenDataInstance();

        /**
         * Returns the written data version.
         *
         * @return data version written
         */
        DataVersion getWrittenDataVersion();
    }


    private DataInfo data;


    protected EngineDataAccessId() {
        // To enact placeholder RW access for commutative accesses.
    }

    protected EngineDataAccessId(DataInfo data) {
        this.data = data;
    }

    public DataInfo getAccessedDataInfo() {
        return this.data;
    }

    public int getDataId() {
        return this.data.getDataId();
    }

    public final void commit() {
        this.data.committedAccess(this);
    }

    public final void cancel(boolean keepModified) {
        this.data.cancelledAccess(this, keepModified);
    }

    /**
     * Returns a new DataAccess where the no-longer-valid versions are replaced by valid ones.
     *
     * @return new DataAccess with valid versions.
     */
    public abstract EngineDataAccessId consolidateValidVersions();

    public abstract String toDebugString();
}
