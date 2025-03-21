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
package es.bsc.compss.types.data.info;

import es.bsc.compss.types.data.EngineDataInstanceId;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;


public class DataVersion {

    private final EngineDataInstanceId dataInstanceId;
    private int readers;
    private int writers;
    private boolean toDelete;
    private boolean used; // The version has been read or written
    private boolean semUsed;
    private boolean canceled;
    private boolean valid;
    private List<Semaphore> semReaders;

    private DataVersion prevValidVersion;
    private DataVersion nextValidVersion;
    private boolean valueOnMain;


    /**
     * Creates a new DataVersion instance.
     *
     * @param dataId Data Id.
     * @param versionId Version Id.
     * @param predecessor Previous version of the same data
     */
    public DataVersion(int dataId, int versionId, DataVersion predecessor) {
        this.dataInstanceId = new EngineDataInstanceId(dataId, versionId, this);
        this.valueOnMain = false;
        this.writers = 0;
        this.toDelete = false;
        this.used = false;
        this.canceled = false;
        this.semReaders = new LinkedList<>();
        this.semUsed = false;
        this.valid = true;
        this.prevValidVersion = null;
        if (predecessor != null) {
            if (predecessor.isValid()) {
                this.prevValidVersion = predecessor;
                this.prevValidVersion.nextValidVersion = this;
            } else {
                predecessor = predecessor.getPreviousValidPredecessor();
                if (predecessor != null) {
                    this.prevValidVersion = predecessor;
                    this.prevValidVersion.nextValidVersion = this;
                }
            }
        }
    }

    /**
     * Returns the associated data instance.
     *
     * @return The associated data instance.
     */
    public EngineDataInstanceId getDataInstanceId() {
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
        this.writers++;
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
        if (readers == 0 && this.semUsed == true) {
            for (Semaphore s : semReaders) {
                s.release();
            }
        }
        return this.toDelete && checkDeletion();
    }

    /**
     * Marks the data version as written and returns whether it can be deleted or not.
     *
     * @return {@code true} if the data can be deleted, {@code false} otherwise.
     */
    public boolean hasBeenWritten() {
        this.writers--;
        return this.toDelete && checkDeletion();
    }

    /**
     * Returns whether the data can be deleted or not.
     *
     * @return {@code true} if the data can be deleted, {@code false} otherwise.
     */
    public boolean markToDelete() {
        this.toDelete = true;
        return checkDeletion();
    }

    public void unmarkToDelete() {
        this.toDelete = false;
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
        if (this.writers == 0 // version has been generated
            && this.readers == 0 // version has been read
        ) {
            invalidate();
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
     * Marks the version as cancelled.
     */
    public void versionCancelled() {
        this.canceled = true;
        invalidate();
    }

    /**
     * Marks the version as invalid.
     */
    public void invalidate() {
        if (this.valid) {
            this.valid = false;
            if (this.nextValidVersion != null) {
                this.nextValidVersion.prevValidVersion = this.prevValidVersion;
            }
            if (this.prevValidVersion != null) {
                this.prevValidVersion.nextValidVersion = this.nextValidVersion;
            }
        }
    }

    /**
     * Returns whether the version is valid or not.
     *
     * @return {@code false} if the version is valid, {@code true} otherwise.
     */
    public boolean isValid() {
        return this.valid;
    }

    /**
     * Returns the closer previous version that has not been cancelled.
     *
     * @return closer previous version that has not been cancelled.
     */
    public DataVersion getPreviousValidPredecessor() {
        if (this.prevValidVersion != null && !this.prevValidVersion.isValid()) {
            return this.prevValidVersion.getPreviousValidPredecessor();
        }
        return this.prevValidVersion;
    }

    /**
     * Returns whether the version has been used or not.
     *
     * @return {@code true} if the version has been used, {@code false} otherwise.
     */
    public boolean hasBeenUsed() {
        return this.used;
    }

    /**
     * Returns whether the version has been cancelled or not.
     *
     * @return {@code true} if the version has been cancelled, {@code false} otherwise.
     */
    public boolean hasBeenCancelled() {
        return this.canceled;
    }

    /**
     * Adds a Semaphore to the Semaphore list.
     *
     * @param semWait Semaphore to be added to the list
     * @return True if semaphore is add. Otherwise false.
     */
    public boolean addSemaphore(Semaphore semWait) {
        if (readers != 0) {
            this.semUsed = true;
            this.semReaders.add(semWait);
            return true;
        }
        return false;
    }

    /**
     * Returns whether the value of the data version has been sent to the main.
     * 
     * @return {@literal true} if it was sent; {@literal false}, otherwise
     */
    public boolean isValueOnMain() {
        return this.valueOnMain;
    }

    /**
     * Marks the version value as sent to the main.
     */
    public void valueOnMain() {
        this.valueOnMain = true;
    }
}
