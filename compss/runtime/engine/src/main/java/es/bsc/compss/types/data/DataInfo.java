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

import es.bsc.compss.comm.Comm;

import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;


// Information about a datum and its versions
public abstract class DataInfo {

    private static final int FIRST_FILE_ID = 1;
    private static final int FIRST_VERSION_ID = 1;

    protected static int nextDataId = FIRST_FILE_ID;
    // Data identifier
    protected int dataId;

    // Current version
    protected DataVersion currentVersion;
    // Data and version identifier management
    protected int currentVersionId;

    // Versions of the datum
    // Map: version identifier -> version
    protected TreeMap<Integer, DataVersion> versions;
    // private boolean toDelete;

    protected int deletionBlocks;
    protected final LinkedList<DataVersion> pendingDeletions;
    protected final LinkedList<Integer> canceledVersions;

    protected Boolean canceled;


    /**
     * Creates a new DataInfo instance with and registers a new LogicalData.
     */
    public DataInfo() {
        this.dataId = nextDataId++;
        this.versions = new TreeMap<>();
        this.currentVersionId = FIRST_VERSION_ID;
        this.currentVersion = new DataVersion(dataId, 1);
        Comm.registerData(currentVersion.getDataInstanceId().getRenaming());
        this.versions.put(currentVersionId, currentVersion);
        this.deletionBlocks = 0;
        this.pendingDeletions = new LinkedList<>();
        this.canceledVersions = new LinkedList<>();
        this.canceled = false;
    }

    /**
     * Creates a new DataInfo instance for an already existing LogicalData.
     *
     * @param data data being accessed
     */
    public DataInfo(String data) {
        this.dataId = nextDataId++;
        this.versions = new TreeMap<>();
        this.currentVersionId = FIRST_VERSION_ID;
        this.currentVersion = new DataVersion(dataId, 1, data);
        this.versions.put(currentVersionId, currentVersion);
        this.deletionBlocks = 0;
        this.pendingDeletions = new LinkedList<>();
        this.canceledVersions = new LinkedList<>();
        this.canceled = false;
    }

    /**
     * Returns the data Id.
     *
     * @return The data Id.
     */
    public final int getDataId() {
        return this.dataId;
    }

    /**
     * Returns the current version Id.
     *
     * @return The current version Id.
     */
    public final int getCurrentVersionId() {
        return this.currentVersionId;
    }

    /**
     * Returns the current data version.
     *
     * @return The current data version.
     */
    public final DataVersion getCurrentDataVersion() {
        return this.currentVersion;
    }

    /**
     * Returns the previous data version.
     *
     * @return The previous data version.
     */
    public final DataVersion getPreviousDataVersion() {
        return this.versions.get(this.currentVersionId - 1);
    }

    /**
     * Marks the data to be read.
     */
    public final void willBeRead() {
        this.currentVersion.versionUsed();
        this.currentVersion.willBeRead();
    }

    /**
     * Returns whether the data is expected to be read or not.
     *
     * @return {@code true} if there are pending reads to the data, {@code false} otherwise.
     */
    public final boolean isToBeRead() {
        return this.currentVersion.hasPendingLectures();
    }

    /**
     * Returns whether the data has been cancelled or not.
     *
     * @return {@code true} if the data has been cancelled, {@code false} otherwise.
     */
    public final boolean hasBeenCanceled() {
        return this.currentVersion.hasBeenUsed();
    }

    /**
     * Returns whether the specified version {@code versionId} has been read or not.
     *
     * @param versionId Version Id.
     * @return {@code true} if the version Id has no pending reads, {@code false} otherwise.
     */
    public final boolean versionHasBeenRead(int versionId) {
        DataVersion readVersion = this.versions.get(versionId);
        if (readVersion.hasBeenRead()) {
            Comm.removeData(readVersion.getDataInstanceId().getRenaming());
            this.versions.remove(versionId);
            // return (this.toDelete && versions.size() == 0);
            return this.versions.isEmpty();
        }
        return false;
    }

    /**
     * Marks the data to be written.
     */
    public void willBeWritten() {
        int oldVersionId = this.currentVersionId;
        this.currentVersionId++;
        DataVersion newVersion = new DataVersion(this.dataId, this.currentVersionId);
        Comm.registerData(newVersion.getDataInstanceId().getRenaming());
        newVersion.willBeWritten();
        DataVersion oldVersion = this.currentVersion;
        this.versions.put(this.currentVersionId, newVersion);
        this.currentVersion = newVersion;
        if (oldVersion != null) {
            if (oldVersion.markToDelete()) {
                Comm.removeData(oldVersion.getDataInstanceId().getRenaming());
                this.versions.remove(oldVersionId);
            }
        }
        this.currentVersion.versionUsed();
    }

    /**
     * Returns whether the data has already been written or not.
     *
     * @param versionId Version Id.
     * @return {@code true} if the data has been written, {@code false} otherwise.
     */
    public final boolean versionHasBeenWritten(int versionId) {
        DataVersion writtenVersion = versions.get(versionId);
        if (writtenVersion.hasBeenWritten()) {
            Comm.removeData(writtenVersion.getDataInstanceId().getRenaming());
            this.versions.remove(versionId);
            // return (this.toDelete && versions.size() == 0);
            return this.versions.isEmpty();
        }
        return false;
    }

    /**
     * Increases the number of deletion blocks.
     */
    public final void blockDeletions() {
        this.deletionBlocks++;
    }

    /**
     * Decreases the number of deletion blocks and returns whether all the pending deletions are completed or not.
     *
     * @return {@code true} if all the pending deletions have been removed, {@code false} otherwise.
     */
    public final boolean unblockDeletions() {
        this.deletionBlocks--;
        if (this.deletionBlocks == 0) {
            for (DataVersion version : this.pendingDeletions) {
                if (version.markToDelete()) {
                    Comm.removeData(version.getDataInstanceId().getRenaming());
                    this.versions.remove(version.getDataInstanceId().getVersionId());
                }
            }
            if (this.versions.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Delete DataInfo (can be overwritten by implementations).
     *
     * @return {@code true} if all the versions have been removed, {@code false} otherwise.
     */
    public boolean delete(boolean noReuse) {
        if (this.deletionBlocks > 0) {
            this.pendingDeletions.addAll(this.versions.values());
        } else {
            LinkedList<Integer> removedVersions = new LinkedList<>();
            for (DataVersion version : this.versions.values()) {
                String sourceName = version.getDataInstanceId().getRenaming();
                if (version.markToDelete()) {
                    LogicalData ld = Comm.getData(sourceName);
                    if (ld != null) {
                        ld.removeValue();
                    }
                    Comm.removeData(sourceName);
                    removedVersions.add(version.getDataInstanceId().getVersionId());
                }
            }
            for (int versionId : removedVersions) {
                this.versions.remove(versionId);
            }
            if (this.versions.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Waits for the data to be ready to be deleted.
     *
     * @param semWait Semaphore.
     * @return
     */
    public abstract int waitForDataReadyToDelete(Semaphore semWait);

    /**
     * Returns whether the current version is marked to deleted or not.
     *
     * @return {@code true} if the current version must be deleted, {@code false} otherwise.
     */
    public final boolean isCurrentVersionToDelete() {
        return this.currentVersion.isToDelete();
    }

    /**
     * Returns the first data version.
     *
     * @return The first data version.
     */
    public final DataVersion getFirstVersion() {
        return versions.get(1);
    }

    /**
     * Tries to remove the given version {@code versionId}.
     *
     * @param versionId Version Id.
     */
    public final void tryRemoveVersion(Integer versionId) {
        DataVersion readVersion = this.versions.get(versionId);

        if (readVersion != null && readVersion.markToDelete()) {
            Comm.removeData(readVersion.getDataInstanceId().getRenaming());
            this.versions.remove(versionId);
        }

    }

    /**
     * Cancels the given version {@code versionId}.
     *
     * @param versionId Version Id.
     */
    public final void canceledVersion(Integer versionId) {
        this.canceledVersions.add(versionId);
        if (versionId == currentVersionId) {
            Integer lastVersion = this.currentVersionId;
            while (this.canceledVersions.contains(lastVersion)) {
                tryRemoveVersion(lastVersion);
                lastVersion = lastVersion - 1;
            }
            this.currentVersionId = lastVersion;
            this.currentVersion = this.versions.get(this.currentVersionId);
        }
    }
}
