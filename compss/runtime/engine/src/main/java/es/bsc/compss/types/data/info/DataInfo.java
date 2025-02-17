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

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams.AccessMode;
import es.bsc.compss.types.data.params.DataOwner;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.parameter.impl.DependencyParameter;
import es.bsc.compss.types.request.ap.RegisterDataAccessRequest;
import es.bsc.compss.types.request.exceptions.NonExistingValueException;

import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


// Information about a datum and its versions
public abstract class DataInfo<T extends DataParams> {

    // CONSTANTS
    private static final int FIRST_FILE_ID = 1;
    private static final int FIRST_VERSION_ID = 1;

    // Component logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TP_COMP);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static int nextDataId = FIRST_FILE_ID;

    // Data identifier
    protected final int dataId;
    // Generating application
    protected final T params;
    protected final DataOwner owner;

    // Current version
    protected DataVersion currentVersion;
    // Data and version identifier management
    private int currentVersionId;

    // Versions of the datum
    // Map: version identifier -> version
    private TreeMap<Integer, DataVersion> versions;

    private int deletionBlocks;
    private final LinkedList<DataVersion> pendingDeletions;
    private final LinkedList<Integer> canceledVersions;

    private boolean deleted;


    /**
     * Creates a new DataInfo instance with and registers a new LogicalData.
     *
     * @param data description of the data related to the info
     * @param owner owner of the data being created
     */
    public DataInfo(T data, DataOwner owner) {
        this.dataId = nextDataId++;
        this.params = data;
        this.owner = owner;
        this.versions = new TreeMap<>();
        this.currentVersionId = FIRST_VERSION_ID;
        this.currentVersion = new DataVersion(dataId, 1, null);
        this.versions.put(currentVersionId, currentVersion);
        this.deletionBlocks = 0;
        this.pendingDeletions = new LinkedList<>();
        this.canceledVersions = new LinkedList<>();
        this.deleted = false;
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
     * Returns the description of the data related to the info.
     *
     * @return description of the data related to the info
     */
    public final T getParams() {
        return params;
    }

    /**
     * Returns the owner of the data.
     *
     * @return owner of the data
     */
    public DataOwner getOwner() {
        return owner;
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
     * Registers a new version for the data.
     */
    protected void newVersion() {
        this.currentVersionId++;
        DataVersion validPred = currentVersion;
        if (validPred.hasBeenCancelled()) {
            validPred = validPred.getPreviousValidPredecessor();
        }
        DataVersion newVersion = new DataVersion(this.dataId, this.currentVersionId, validPred);
        this.versions.put(this.currentVersionId, newVersion);
        this.currentVersion = newVersion;
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
     * Reconstruct the last access to the data as if were of a given mode.
     *
     * @param mode mode being access
     */
    public final EngineDataAccessId getLastAccess(AccessMode mode) {
        // Version management
        EngineDataAccessId daId = null;
        if (this.currentVersion != null) {
            switch (mode) {
                case C:
                case R:
                    daId = new RAccessId(this, this.currentVersion);
                    break;
                case W:
                    daId = new WAccessId(this, this.currentVersion);
                    break;
                case CV:
                case RW:
                    DataVersion readInstance = this.versions.get(this.currentVersionId - 1);
                    if (readInstance != null) {
                        daId = new RWAccessId(this, readInstance, this.currentVersion);
                    } else {
                        LOGGER.warn("Previous instance for data" + this.dataId + " is null.");
                    }
                    break;
            }
        } else {
            LOGGER.warn("Current instance for data" + this.dataId + " is null.");
        }
        return daId;
    }

    /*
     * Registers a future lecture of the current version of the data.
     */
    protected void currentVersionWillBeRead() {
        this.currentVersion.willBeRead();
        this.currentVersion.versionUsed();
    }

    /*
     * Registers a future writing of the current version of the data.
     */
    protected void currentVersionWillBeWritten() {
        this.currentVersion.willBeWritten();
        this.currentVersion.versionUsed();
    }

    /**
     * Registers a new access to the data.
     *
     * @param mode access mode of the operation performed on the data
     * @return description of the access performed
     */
    public abstract EngineDataAccessId willAccess(AccessMode mode);

    /**
     * Tries to remove the given version {@code versionId}.
     *
     * @param versionId Version Id.
     */
    private void tryRemoveVersion(Integer versionId) {
        DataVersion version = this.versions.get(versionId);
        if (version != null && version.markToDelete()) {
            version.getDataInstanceId().delete();
            this.versions.remove(versionId);
        }
    }

    /**
     * Marks that a given version {@code dAccId} has been accessed.
     *
     * @param dAccId DataAccessId.
     */
    public void committedAccess(EngineDataAccessId dAccId) {
        Integer rVersionId = null;
        Integer wVersionId;

        if (dAccId.isRead()) {
            rVersionId = ((ReadingDataAccessId) dAccId).getReadDataInstance().getVersionId();
            this.versionHasBeenRead(rVersionId);
        }

        if (dAccId.isWrite()) {
            wVersionId = ((WritingDataAccessId) dAccId).getWrittenDataInstance().getVersionId();
            if (rVersionId == null) {
                rVersionId = wVersionId - 1;
            }
            this.tryRemoveVersion(rVersionId);
            this.versionHasBeenWritten(wVersionId);
        }
    }

    /**
     * Returns whether the specified version {@code versionId} has been read or not.
     *
     * @param versionId Version Id.
     */
    private void versionHasBeenRead(int versionId) {
        DataVersion readVersion = this.versions.get(versionId);
        if (readVersion.hasBeenRead()) {
            readVersion.getDataInstanceId().delete();
            this.versions.remove(versionId);
        }
    }

    /**
     * Returns whether the data has already been written or not.
     *
     * @param versionId Version Id.
     */
    private void versionHasBeenWritten(int versionId) {
        DataVersion writtenVersion = versions.get(versionId);
        if (writtenVersion.hasBeenWritten()) {
            writtenVersion.getDataInstanceId().delete();
            this.versions.remove(versionId);
        }
    }

    /**
     * Returns whether the data has been used or not.
     *
     * @return {@code true} if the data has been used, {@code false} otherwise.
     */
    public final boolean isCurrentVersionBeenUsed() {
        return this.currentVersion.hasBeenUsed();
    }

    /**
     * Removes the versions associated with the given DataAccessId {@code dAccId} to if the task was canceled or not.
     *
     * @param dAccId DataAccessId.
     * @param keepModified {@literal true}, if the value resulting from the access should be kept
     */
    public void cancelledAccess(EngineDataAccessId dAccId, boolean keepModified) {
        Integer rVersionId;
        Integer wVersionId;
        switch (dAccId.getDirection()) {
            case C:
            case R:
                rVersionId = ((RAccessId) dAccId).getReadDataInstance().getVersionId();
                this.canceledReadVersion(rVersionId);
                break;
            case CV:
            case RW:
                rVersionId = ((RWAccessId) dAccId).getReadDataInstance().getVersionId();
                wVersionId = ((RWAccessId) dAccId).getWrittenDataInstance().getVersionId();
                if (keepModified) {
                    this.versionHasBeenRead(rVersionId);
                    // read data version can be removed
                    this.tryRemoveVersion(rVersionId);
                    this.versionHasBeenWritten(wVersionId);
                } else {
                    this.canceledReadVersion(rVersionId);
                    this.canceledWriteVersion(wVersionId);
                }
                break;
            default:// case W:
                wVersionId = ((WAccessId) dAccId).getWrittenDataInstance().getVersionId();
                this.canceledWriteVersion(wVersionId);
                break;
        }
    }

    /**
     * Cancels the given read version {@code versionId}.
     *
     * @param versionId Version Id.
     */
    private void canceledReadVersion(Integer versionId) {
        DataVersion readVersion = this.versions.get(versionId);
        if (!deleted && readVersion.isToDelete() && readVersion.hasBeenUsed()) {
            readVersion.unmarkToDelete();
        }
        if (readVersion.hasBeenRead()) {
            readVersion.getDataInstanceId().delete();
            this.versions.remove(versionId);
        }
    }

    /**
     * Cancels the given version {@code versionId}.
     *
     * @param versionId Version Id.
     */
    private void canceledWriteVersion(Integer versionId) {
        DataVersion version = this.versions.get(versionId);
        version.versionCancelled();
        this.canceledVersions.add(versionId);
        if (versionId == currentVersionId) {
            Integer lastVersion = this.currentVersionId;
            while (this.canceledVersions.contains(lastVersion)) {
                tryRemoveVersion(lastVersion);
                lastVersion = lastVersion - 1;
            }
            if (lastVersion > 1) {
                this.currentVersionId = lastVersion;
                this.currentVersion = this.versions.get(this.currentVersionId);
            } else if (lastVersion == 1) {
                DataVersion firstVersion = this.getFirstVersion();
                if (firstVersion != null && firstVersion.hasBeenUsed()) {
                    this.currentVersionId = lastVersion;
                    this.currentVersion = firstVersion;
                }
            }
        }
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
                    version.getDataInstanceId().delete();
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
     */
    public void delete() {
        this.deleted = true;
        if (this.deletionBlocks > 0) {
            this.pendingDeletions.addAll(this.versions.values());
        } else {
            LinkedList<Integer> removedVersions = new LinkedList<>();
            for (DataVersion version : this.versions.values()) {
                if (version.markToDelete()) {
                    version.getDataInstanceId().delete();
                    removedVersions.add(version.getDataInstanceId().getVersionId());
                }
            }
            for (int versionId : removedVersions) {
                this.versions.remove(versionId);
            }
        }
    }

    /**
     * Waits for the data to be ready to be deleted.
     *
     * @param sem Semaphore.
     * @throws NonExistingValueException the data to delete does not actually exist
     */
    public abstract void waitForDataReadyToDelete(Semaphore sem) throws NonExistingValueException;

    /**
     * Returns whether the current version is marked to deleted or not.
     *
     * @return {@code true} if the current version must be deleted, {@code false} otherwise.
     */
    public final boolean isCurrentVersionToDelete() {
        return this.currentVersion.isToDelete();
    }

    /**
     * Registers a task reading the data value.
     *
     * @param t task reading the value
     * @param dp parameter corresponding to the data value
     * @param isConcurrent {@literal true} if the reading was due to a concuerrent access; {@literal false} otherwise.
     * @return {@literal true}, if an edge has been printed; {@literal false}, otherwise.
     */
    public abstract boolean readValue(Task t, DependencyParameter dp, boolean isConcurrent);

    /**
     * Registers a task writting on the data value.
     *
     * @param t task writting the value
     * @param dp parameter corresponding to the data value
     * @param isConcurrent {@literal true} if the writting was due to a concuerrent access; {@literal false} otherwise.
     */
    public abstract void writeValue(Task t, DependencyParameter dp, boolean isConcurrent);

    /**
     * Registers an access from the application main code to the value.
     *
     * @param rdar Request to access the data value
     * @param access data access description with instances
     */
    public abstract void mainAccess(RegisterDataAccessRequest rdar, EngineDataAccessId access);

    /**
     * Registers a data producer as completed.
     *
     * @param task Data Producer
     */
    public abstract void completedProducer(AbstractTask task);

    /**
     * Obtains the task/task group producing the last version of the data.
     * 
     * @return the task/task group producing the last version of the data
     */
    public abstract AbstractTask getLastVersionProducer();

    /**
     * Returns the last Tasks producing the value.
     *
     * @return last tasks generating the value.
     */
    public abstract List<AbstractTask> getDataWriters();
}
