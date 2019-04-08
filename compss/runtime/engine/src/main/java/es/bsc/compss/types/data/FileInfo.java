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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.listener.SafeCopyListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.DataLocation.Type;
import es.bsc.compss.types.data.operation.copy.Copy;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.util.ErrorManager;


public class FileInfo extends DataInfo {

    // Original name and location of the file
    private final DataLocation origLocation;
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);


    public FileInfo(DataLocation loc) {
        super();
        this.origLocation = loc;
    }

    public DataLocation getOriginalLocation() {
        return origLocation;
    }
    
    @Override
    public int waitForDataReadyToDelete(Semaphore semWait) {
        int nPermits=1;
        LOGGER.debug("[FileInfo] Deleting file of data " + this.getDataId());
        DataVersion firstVersion = this.getFirstVersion();
        if (firstVersion != null) {
            LogicalData ld = Comm.getData(firstVersion.getDataInstanceId().getRenaming());
            if (ld != null) {
                for (DataLocation loc : ld.getLocations()) {                
                    MultiURI uri = loc.getURIInHost(Comm.getAppHost());
                    if (uri != null) {
                        if (loc.equals(origLocation)) {
                            if (loc.getType() != Type.SHARED) {
                                nPermits = waitForEndingCopies(ld, loc, semWait);
                            } else {
                                waitForLecturers(firstVersion,ld,loc,semWait);
                            }
                        }
                    }
                }
                if (ld.getLocations().size() == 0) {
                    semWait.release();
                }
            } 
        }  else {
            semWait.release();  
        }
        return nPermits;
    }
    
    private void waitForLecturers(DataVersion firstVersion, LogicalData ld, DataLocation loc, Semaphore semWait) {
        firstVersion.addSemaphore(semWait);
    }
    
    @Override
    public boolean delete() {
        LOGGER.debug("[FileInfo] Deleting file of data " + this.getDataId());
          DataVersion firstVersion = this.getFirstVersion();
        if (firstVersion != null) {
            LogicalData ld = Comm.getData(firstVersion.getDataInstanceId().getRenaming());
            if (ld != null) {
                for (DataLocation loc : ld.getLocations()) {
                    MultiURI uri = loc.getURIInHost(Comm.getAppHost());
                    if (uri != null) {
                        String newPath = Comm.getAppHost().getTempDirPath() + File.separator
                                + firstVersion.getDataInstanceId().getRenaming();
                        LOGGER.debug("[FileInfo] Modifying path in location " + loc + " with new path " + newPath);
                        loc.modifyPath(newPath);
                        try {
                            LOGGER.debug("[FileInfo] Moving " + uri.getPath() + " to " + newPath);
                            Files.move(new File(uri.getPath()).toPath(), new File(newPath).toPath(),
                                    StandardCopyOption.REPLACE_EXISTING);
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
        return super.delete();
    }

    private int waitForEndingCopies(LogicalData ld, DataLocation loc, Semaphore semWait) {
        Collection<Copy> copiesInProgress = ld.getCopiesInProgress();
        int nPermits = 1;
        if (copiesInProgress != null && !copiesInProgress.isEmpty()) {
            //length copiesInProgress son els permits
            nPermits = copiesInProgress.size();
            for (Copy copy : copiesInProgress) {
                if (copy.getSourceData().equals(ld)) {
                    LOGGER.debug("[FileInfo] Waiting for copy of data " + ld.getName() + " to finish...");
                    SafeCopyListener currentCopylistener = new SafeCopyListener(semWait);
                    copy.addEventListener(currentCopylistener);
                    currentCopylistener.addOperation();
                    currentCopylistener.enable();
//                    Copy.waitForCopyTofinish(copy, Comm.getAppHost().getNode());
                }
            }
        } else {
            semWait.release();
        }
        return nPermits;
    }

}
