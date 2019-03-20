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
package es.bsc.compss.types.request.ap;

import java.io.File;
import java.util.concurrent.Semaphore;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.data.FileInfo;
import es.bsc.compss.types.data.location.DataLocation;


public class DeleteFileRequest extends APRequest {

    private final DataLocation loc;
    private final Semaphore sem;


    public DeleteFileRequest(DataLocation loc, Semaphore sem) {
        this.loc = loc;
        this.sem = sem;
    }

    public DataLocation getLocation() {
        return loc;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        LOGGER.info("[DeleteFileRequest] Notify data delete to DIP...");
        FileInfo fileInfo = (FileInfo) dip.deleteData(loc);
        if (fileInfo == null) {
            // File is not used by any task, we can erase it
            // Retrieve the first valid URI location (private locations have only 1, shared locations may have more)
            String filePath = loc.getURIs().get(0).getPath();
            File f = new File(filePath);
            if (f.exists()) {
                if (f.delete()) {
                    LOGGER.info("[DeleteFileRequest] File " + filePath + " deleted");
                } else {
                    LOGGER.error("[DeleteFileRequest] Error on deleting file " + filePath);
                }
            }

        } else { // file is involved in some task execution
            // File Won't be read by any future task or from the main code.
            // Remove it from the dependency analysis and the files to be transferred back
            LOGGER.info("[DeleteFileRequest] Deleting Data in Task Analyser");
            ta.deleteData(fileInfo);
        }
        sem.release();
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.DELETE_FILE;
    }

}
