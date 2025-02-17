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

import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.listener.SafeCopyListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.LocationType;
import es.bsc.compss.types.data.operation.copy.Copy;
import es.bsc.compss.types.data.params.DataOwner;
import es.bsc.compss.types.data.params.FileData;
import es.bsc.compss.types.request.exceptions.NonExistingValueException;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.FileOpsManager;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class FileInfo extends StandardDataInfo<FileData> {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);


    /**
     * Creates a new FileInfo instance for a given file.
     * 
     * @param file description of the file related to the info
     * @param owner owner of the fileInfo being created
     */
    public FileInfo(FileData file, DataOwner owner) {
        super(file, owner);
        String locKey = file.getLocationKey();
        owner.registerFileData(locKey, this);
    }

    /**
     * Returns the original file location.
     * 
     * @return The original file location.
     */
    public DataLocation getOriginalLocation() {
        return this.getParams().getLocation();
    }

    @Override
    public void waitForDataReadyToDelete(Semaphore sem) throws NonExistingValueException {
        LOGGER.debug("[FileInfo] Deleting file of data " + this.getDataId());
        DataVersion firstVersion = this.getFirstVersion();
        if (firstVersion != null && firstVersion.isValid()) {
            LogicalData ld = firstVersion.getDataInstanceId().getData();
            if (ld != null) {
                for (DataLocation loc : ld.getLocations()) {
                    MultiURI uri = loc.getURIInHost(Comm.getAppHost());
                    if (uri != null) {
                        if (loc.equals(getOriginalLocation())) {
                            DeletionListener listener = new DeletionListener(sem, firstVersion, loc, uri);
                            if (loc.getType() != LocationType.SHARED) {
                                waitForEndingCopies(ld, listener);
                            } else {
                                listener.addPendingOperation();
                                // Add a semaphore to notify if all readers to finish
                                if (!firstVersion.addSemaphore(listener)) {
                                    LOGGER.debug("[FileInfo] Readers for first version of " + this.getDataId()
                                        + " finished. Nothing to do. Releasing semaphore.");
                                    listener.release();
                                }
                            }
                            listener.enable();
                            return;
                        }
                    }
                }
                throw new NonExistingValueException();
            }
        } else {
            throw new NonExistingValueException();
        }
    }

    private void waitForEndingCopies(LogicalData ld, DeletionListener listener) {
        Collection<Copy> copiesInProgress = ld.getCopiesInProgress();
        if (copiesInProgress != null && !copiesInProgress.isEmpty()) {
            // length copiesInProgress son els permits
            for (Copy copy : copiesInProgress) {
                if (copy.getSourceData().equals(ld)) {
                    LOGGER.debug("[FileInfo] Waiting for copy of data " + ld.getName() + " to finish...");
                    SafeCopyListener currentCopylistener = new SafeCopyListener(listener);
                    listener.addPendingOperation();
                    copy.addEventListener(currentCopylistener);
                    currentCopylistener.addOperation();
                    currentCopylistener.enable();
                }
            }
        }
    }


    private static class DeletionListener extends Semaphore {

        private final Semaphore sem;
        private final DataVersion version;
        private final DataLocation loc;
        private final MultiURI uri;

        private int missingOperations = 1; // Starts with 1 for enabling


        public DeletionListener(Semaphore sem, DataVersion version, DataLocation location, MultiURI uri) {
            super(0);
            this.sem = sem;
            this.version = version;
            this.loc = location;
            this.uri = uri;
        }

        public void addPendingOperation() {
            missingOperations++;
        }

        public void enable() {
            missingOperations--;
            if (this.missingOperations == 0) {
                completed();
            }
        }

        @Override
        public void release() {
            missingOperations--;
            if (this.missingOperations == 0) {
                completed();
            }
        }

        public void completed() {
            EngineDataInstanceId daId = version.getDataInstanceId();
            LogicalData ld = daId.getData();
            // The number of readers can only be higher than 0 in local disks. The overhead of moving should be low.
            if (version.hasPendingLectures()) {
                String rename = daId.getRenaming();
                moveToWorkingDir(loc, uri, rename);
            } else {
                ld.removeLocation(loc);
            }
            sem.release();
        }

        private void moveToWorkingDir(DataLocation loc, MultiURI uri, String renaming) {
            String newPath = Comm.getAppHost().getWorkingDirectory() + File.separator + renaming;
            LOGGER.debug("[FileInfo] Modifying path in location " + loc + " with new path " + newPath);
            loc.modifyPath(newPath);
            try {
                LOGGER.debug("[FileInfo] Moving " + uri.getPath() + " to " + newPath);
                FileOpsManager.moveSync(new File(uri.getPath()), new File(newPath));
            } catch (IOException e) {
                ErrorManager
                    .warn("File " + uri.getPath() + " cannot be moved to " + newPath + "Reason: " + e.getMessage());
            }
        }

    }

}
