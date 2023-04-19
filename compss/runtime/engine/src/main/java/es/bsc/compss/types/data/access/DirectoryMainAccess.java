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
package es.bsc.compss.types.data.access;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.DataParams.DirectoryData;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessparams.DirectoryAccessParams;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.DirectoryTransferable;
import es.bsc.compss.types.data.operation.OneOpWithSemListener;
import es.bsc.compss.types.uri.SimpleURI;
import java.util.concurrent.Semaphore;


/**
 * Handling of an access from the main code to a directory.
 */
public class DirectoryMainAccess extends FileMainAccess<DirectoryData, DirectoryAccessParams> {

    /**
     * Creates a new DirectoryMainAccess instance with the given mode {@code mode} and for the given file location
     * {@code loc}.
     *
     * @param app Id of the application accessing the file.
     * @param dir operation performed.
     * @param loc File location.
     * @return new DirectoryMainAccess instance
     */
    public static final DirectoryMainAccess constructDMA(Application app, Direction dir, DataLocation loc) {
        DirectoryAccessParams dap = DirectoryAccessParams.constructDAP(app, dir, loc);
        return new DirectoryMainAccess(dap);
    }

    private DirectoryMainAccess(DirectoryAccessParams p) {
        super(p);
    }

    @Override
    public DataLocation getUnavailableValueResponse() {
        return this.createDirLocation("null");
    }

    /**
     * Fetches the last version of the directory.
     *
     * @param daId Data Access Id.
     * @return Location of the transferred open directory.
     */
    @Override
    public DataLocation fetch(DataAccessId daId) {
        // Get target information
        DataInstanceId targetFile;
        if (daId.isWrite()) {
            DataAccessId.WritingDataAccessId waId = (DataAccessId.WritingDataAccessId) daId;
            targetFile = waId.getWrittenDataInstance();

        } else {
            // Read only mode
            RAccessId raId = (RAccessId) daId;
            targetFile = raId.getReadDataInstance();
        }
        String targetName = targetFile.getRenaming();
        String targetPath = Comm.getAppHost().getWorkingDirectory() + targetName;
        LOGGER.debug("Openning directory " + targetName + " at " + targetPath);

        // Create location
        DataLocation targetLocation = createDirLocation(targetPath);
        if (daId.isRead()) {
            LOGGER.debug("Asking for transfer");
            DataAccessId.ReadingDataAccessId rdaId = (DataAccessId.ReadingDataAccessId) daId;
            LogicalData srcData = rdaId.getReadDataInstance().getData();
            DirectoryTransferable dt = new DirectoryTransferable(daId.isPreserveSourceData());
            Semaphore sem = new Semaphore(0);
            OneOpWithSemListener listener = new OneOpWithSemListener(sem);
            if (daId.isWrite()) {
                Comm.getAppHost().getData(srcData, targetName, (LogicalData) null, dt, listener);
            } else {
                Comm.getAppHost().getData(srcData, dt, listener);
            }
            sem.acquireUninterruptibly();
            String finalPath = dt.getDataTarget();
            return createDirLocation(finalPath);
        } else {
            LOGGER.debug("Write only mode. Auto-release");
            Comm.registerLocation(targetName, targetLocation);
            // Register target location
            LOGGER.debug("Setting target location to " + targetLocation);
            return targetLocation;
        }
    }

    private DataLocation createDirLocation(String localPath) {
        SimpleURI targetURI = new SimpleURI(ProtocolType.DIR_URI.getSchema() + localPath);
        return createLocalLocation(targetURI);
    }

}
