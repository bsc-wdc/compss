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

import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.data.listener.SafeCopyListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.LocationType;
import es.bsc.compss.types.data.operation.copy.Copy;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.FileOpsManager;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class FileInfo extends DataInfo {

    // Original name and location of the file
    private final DataLocation origLocation;
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);


    /**
     * Creates a new FileInfo instance with the given location {@code loc}.
     * 
     * @param app application generating the data
     * @param loc File location.
     */
    public FileInfo(Application app, DataLocation loc) {
        super(app);
        this.origLocation = loc;
    }

    /**
     * Returns the original file location.
     * 
     * @return The original file location.
     */
    public DataLocation getOriginalLocation() {
        return this.origLocation;
    }

    @Override
    public int waitForDataReadyToDelete(Semaphore semWait) {
        int nPermits = 1;
        LOGGER.debug("[FileInfo] Deleting file of data " + this.getDataId());
        DataVersion firstVersion = this.getFirstVersion();
        if (firstVersion != null) {
            LogicalData ld = firstVersion.getDataInstanceId().getData();
            if (ld != null) {
                for (DataLocation loc : ld.getLocations()) {
                    MultiURI uri = loc.getURIInHost(Comm.getAppHost());
                    if (uri != null) {
                        if (loc.equals(this.origLocation)) {
                            if (loc.getType() != LocationType.SHARED) {
                                nPermits = waitForEndingCopies(ld, loc, semWait);
                            } else {
                                // Add a semaphore to notify if all readers to finish
                                if (!firstVersion.addSemaphore(semWait)) {
                                    LOGGER.debug("[FileInfo] Readers for first version of " + this.getDataId()
                                        + " finished. Nothing to do. Releasing semaphore.");
                                    semWait.release();
                                }
                            }
                            return nPermits;
                        }
                    }
                }
                LOGGER.debug("[FileInfo] No location in " + this.getDataId()
                    + " equal to original. Nothing to do. Releasing semaphore.");
                semWait.release();
            }
        } else {
            LOGGER.debug("[FileInfo] First version of data " + this.getDataId()
                + " is null. Nothing to do. Releasing semaphore.");
            semWait.release();
        }
        return nPermits;
    }

    @Override
    public boolean delete(boolean noReuse) {
        LOGGER.debug("[FileInfo] Deleting file of data " + this.getDataId());
        if (!noReuse) {
            DataVersion firstVersion = this.getFirstVersion();
            if (firstVersion != null) {
                LogicalData ld = firstVersion.getDataInstanceId().getData();
                if (ld != null) {
                    for (DataLocation loc : ld.getLocations()) {
                        MultiURI uri = loc.getURIInHost(Comm.getAppHost());
                        if (uri != null
                            && uri.getPath().equals(getOriginalLocation().getURIInHost(Comm.getAppHost()).getPath())) {
                            String newPath = Comm.getAppHost().getTempDirPath() + File.separator
                                + firstVersion.getDataInstanceId().getRenaming();
                            LOGGER.debug("[FileInfo] Modifying path in location " + loc + " with new path " + newPath);
                            loc.modifyPath(newPath);
                            try {
                                LOGGER.debug("[FileInfo] Moving " + uri.getPath() + " to " + newPath);
                                FileOpsManager.moveSync(new File(uri.getPath()), new File(newPath));
                            } catch (IOException e) {
                                ErrorManager.warn("File " + uri.getPath() + " cannot be moved to " + newPath
                                    + "Reason: " + e.getMessage());
                            }
                        }
                    }
                }

            } else {
                LOGGER.debug("[FileInfo] First Version is null. Nothing to delete");
            }
        }
        return super.delete(noReuse);
    }

    private int waitForEndingCopies(LogicalData ld, DataLocation loc, Semaphore semWait) {
        Collection<Copy> copiesInProgress = ld.getCopiesInProgress();
        int nPermits = 1;
        if (copiesInProgress != null && !copiesInProgress.isEmpty()) {
            // length copiesInProgress son els permits
            nPermits = copiesInProgress.size();
            for (Copy copy : copiesInProgress) {
                if (copy.getSourceData().equals(ld)) {
                    LOGGER.debug("[FileInfo] Waiting for copy of data " + ld.getName() + " to finish...");
                    SafeCopyListener currentCopylistener = new SafeCopyListener(semWait);
                    copy.addEventListener(currentCopylistener);
                    currentCopylistener.addOperation();
                    currentCopylistener.enable();
                    // Copy.waitForCopyTofinish(copy, Comm.getAppHost().getNode());
                }
            }
        } else {
            semWait.release();
        }
        return nPermits;
    }

}
