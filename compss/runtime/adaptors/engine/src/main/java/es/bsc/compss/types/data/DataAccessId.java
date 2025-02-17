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
package es.bsc.compss.types.data;

import java.io.Serializable;


public interface DataAccessId extends Serializable {

    /**
     * Data Direction.
     */
    public static enum Direction {
        R, // Read
        RW, // Read and write
        W, // Write
        C, // Concurrent
        CV // Commutative
    }


    /**
     * Returns the data Id.
     * 
     * @return The data Id.
     */
    int getDataId();

    /**
     * Returns the data direction.
     * 
     * @return The data direction.
     */
    Direction getDirection();

    /**
     * Returns whether the data access will read or not.
     * 
     * @return {@code true} if the data access will read, {@code false} otherwise.
     */
    boolean isRead();

    /**
     * Returns whether the data access will write or not.
     * 
     * @return {@code true} if the data access will write, {@code false} otherwise.
     */
    boolean isWrite();

    /**
     * Returns whether the source data must be preserved or not.
     * 
     * @return {@code true} if the source data must be preserved, {@code false} otherwise.
     */
    boolean isPreserveSourceData();


    interface ReadingDataAccessId extends DataAccessId {

        /**
         * Returns the read data instance.
         *
         * @return The read data instance.
         */
        public DataInstanceId getReadDataInstance();

        /**
         * Returns the read version id.
         *
         * @return The read version id.
         */
        public int getRVersionId();
    }

    interface WritingDataAccessId extends DataAccessId {

        /**
         * Returns the written data instance.
         *
         * @return The written data instance.
         */
        public DataInstanceId getWrittenDataInstance();

        /**
         * Returns the write version id.
         *
         * @return The write version id.
         */
        public int getWVersionId();
    }
}
