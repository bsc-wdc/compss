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
package es.bsc.compss.types.data;

public class DataVersion {

    private final DataInstanceId dataInstanceId;
    private int readers;
    private int writters;
    private boolean toDelete;
    private boolean used; // The version has been read or written


    /**
     * Creates a new DataVersion instance.
     * 
     * @param dataId Data Id.
     * @param versionId Version Id.
     */
    public DataVersion(int dataId, int versionId) {
        this.readers = 0;
        this.dataInstanceId = new DataInstanceId(dataId, versionId);
        this.writters = 0;
        this.toDelete = false;
        this.used = false;
    }

    /**
     * Returns the associated data instance.
     * 
     * @return The associated data instance.
     */
    public DataInstanceId getDataInstanceId() {
        return this.dataInstanceId;
    }

    /**
     * Marks a read access on the data version.
     */
    public void willBeRead() {
        this.readers++;
    }

    /**
     * Marks a write access on the data version.
     */
    public void willBeWritten() {
        this.writters++;
    }

    /**
     * Returns whether the data version has pending lectures or not.
     * 
     * @return {@code true} if the data version has pending lectures, {@code false} otherwise.
     */
    public boolean hasPendingLectures() {
        return this.readers > 0;
    }

    /**
     * Returns whether the data version has more than one reader or not.
     * 
     * @return @{code true} if the data version has more than one reader, {@code false} otherwise.
     */
    public boolean hasMoreReaders() {
        return this.readers > 1;
    }

    /**
     * Marks the data version as read and returns whether it can be deleted or not.
     * 
     * @return {@code true} if the data can be deleted, {@code false} otherwise.
     */
    public boolean hasBeenRead() {
        this.readers--;
        return checkDeletion();
    }

    /**
     * Marks the data version as written and returns whether it can be deleted or not.
     * 
     * @return {@code true} if the data can be deleted, {@code false} otherwise.
     */
    public boolean hasBeenWritten() {
        this.writters--;
        return checkDeletion();
    }

    /**
     * Returns whether the data can be deleted or not.
     * 
     * @return {@code true} if the data can be deleted, {@code false} otherwise.
     */
    public boolean markToDelete() {
        this.toDelete = true;
        if (this.readers == 0 && this.writters == 0) {
            return true;
        }
        return false;
    }

    /**
     * Whether the data version is marked for deletion or not.
     * 
     * @return {@code true} if the data version is marked for deletion, {@code false} otherwise.
     */
    public boolean isToDelete() {
        return this.toDelete;
    }

    /**
     * Internally checks if the data version can be deleted and is marked for deletion.
     * 
     * @return {@code true} if the data version can be deleted and is marked for deletion, {@code false} otherwise.
     */
    private boolean checkDeletion() {
        if (this.toDelete // deletion requested
                && this.writters == 0 // version has been generated
                && this.readers == 0 // version has been read
        ) {
            return true;
        }
        return false;
    }

    /**
     * Marks the version as used.
     */
    public void versionUsed() {
        this.used = true;
    }

    /**
     * Returns whether the version has been used or not.
     * 
     * @return {@code true} if the version has been used, {@code false} otherwise.
     */
    public boolean hasBeenUsed() {
        return this.used;
    }
}
