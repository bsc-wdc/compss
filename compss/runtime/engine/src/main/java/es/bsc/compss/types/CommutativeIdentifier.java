/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types;

public class CommutativeIdentifier implements Comparable<CommutativeIdentifier> {

    private final int coreId;
    private final int dataId;


    /**
     * Creates a new Commutative group identifier for the given core and data.
     * 
     * @param coreId Associated coreElement Id.
     * @param dataId Associated data Id.
     */
    public CommutativeIdentifier(int coreId, int dataId) {
        this.coreId = coreId;
        this.dataId = dataId;
    }

    /**
     * Returns the associated coreElement Id.
     *
     * @return The associated coreElement Id.
     */
    public int getCoreId() {
        return this.coreId;
    }

    /**
     * Returns the associated data Id.
     *
     * @return The associated data Id.
     */
    public int getDataId() {
        return this.dataId;
    }

    @Override
    public int compareTo(CommutativeIdentifier comId) {
        if (comId == null) {
            throw new NullPointerException();
        }
        return (this.coreId == comId.getCoreId() && this.dataId == comId.getDataId()) ? 1 : 0;
    }

    @Override
    public String toString() {
        return this.dataId + "_" + this.coreId;
    }
}
