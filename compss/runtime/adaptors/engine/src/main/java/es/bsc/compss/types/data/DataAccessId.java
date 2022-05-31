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
package es.bsc.compss.types.data;

import java.io.Serializable;


public abstract class DataAccessId implements Serializable {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;


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
    public abstract int getDataId();

    /**
     * Returns the data direction.
     * 
     * @return The data direction.
     */
    public abstract Direction getDirection();

    /**
     * Returns whether the data access will write or not.
     * 
     * @return {@code true} if the data access will write, {@code false} otherwise.
     */
    public abstract boolean isWrite();

    /**
     * Returns whether the source data must be preserved or not.
     * 
     * @return {@code true} if the source data must be preserved, {@code false} otherwise.
     */
    public abstract boolean isPreserveSourceData();
}
