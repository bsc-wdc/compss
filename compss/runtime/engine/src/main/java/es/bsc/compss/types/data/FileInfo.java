/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.location.DataLocation;
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
    public boolean delete() {
        LOGGER.debug("[FileInfo] Deleting file of data " + this.getDataId());
        DataVersion firstVersion = this.getFirstVersion();
        if (firstVersion != null) {
            LogicalData ld = Comm.getData(firstVersion.getDataInstanceId().getRenaming());
            if (ld != null) {

                for (DataLocation loc : ld.getLocations()) {
                    MultiURI uri = loc.getURIInHost(Comm.getAppHost());
                    if (uri != null) {
                        if (loc.equals(origLocation)) {
                            waitForEndingCopies(ld, loc);
                            String newPath = Comm.getAppHost().getTempDirPath() + File.separator
                                    + firstVersion.getDataInstanceId().getRenaming();
                            LOGGER.debug("[FileInfo] Modifying path in location " + loc + " with new path " + newPath);
                            loc.modifyPath(newPath);
                            try {
                                LOGGER.debug("[FileInfo] Moving " + uri.getPath() + " to " + newPath);
                                Files.move(new File(uri.getPath()).toPath(), new File(newPath).toPath(),
                                        StandardCopyOption.REPLACE_EXISTING);
                            } catch (IOException e) {
                                ErrorManager.warn("File " + uri.getPath() + " cannot be moved to " + newPath + "Reason: " + e.getMessage());
                            }
                        }

                    }
                }
            }
        } else {
            LOGGER.debug("[FileInfo] First Version is null. Nothing to delete");
        }
        return super.delete();
    }

    private void waitForEndingCopies(LogicalData ld, DataLocation loc) {
        ld.lockHostRemoval();
        Collection<Copy> copiesInProgress = ld.getCopiesInProgress();
        if (copiesInProgress != null && !copiesInProgress.isEmpty()) {
            for (Copy copy : copiesInProgress) {
                if (copy.getSourceData().equals(ld)) {
                    LOGGER.debug("[FileInfo] Waiting for copy of data " + ld.getName() + " to finish...");
                    Copy.waitForCopyTofinish(copy, Comm.getAppHost().getNode());
                }
            }
        }
        ld.releaseHostRemoval();

    }

}
