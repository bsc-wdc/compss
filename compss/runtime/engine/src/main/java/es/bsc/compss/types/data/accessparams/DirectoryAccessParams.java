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
package es.bsc.compss.types.data.accessparams;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessparams.DataParams.DirectoryData;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.data.operation.DirectoryTransferable;
import es.bsc.compss.types.data.operation.OneOpWithSemListener;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import java.io.IOException;
import java.util.concurrent.Semaphore;


public class DirectoryAccessParams extends FileAccessParams<DirectoryData> {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Creates a new FileAccessParams instance with the given mode {@code mode} and for the given file location
     * {@code loc}.
     *
     * @param app Id of the application accessing the file.
     * @param dir operation performed.
     * @param loc File location.
     * @return new FileAccessParams instance
     */
    public static final DirectoryAccessParams constructDAP(Application app, Direction dir, DataLocation loc) {
        DirectoryData dd = new DirectoryData(app, loc);
        return new DirectoryAccessParams(dd, dir);
    }

    private DirectoryAccessParams(DirectoryData data, Direction dir) {
        super(data, dir);
    }

    /**
     * Fetches the last version of the directory.
     *
     * @param daId Data Access Id.
     * @return Location of the transferred open directory.
     */
    @Override
    public DataLocation fetchForOpen(DataAccessId daId) {
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
        DataLocation targetLocation = null;
        try {
            SimpleURI targetURI = new SimpleURI(ProtocolType.DIR_URI.getSchema() + targetPath);
            targetLocation = DataLocation.createLocation(Comm.getAppHost(), targetURI);
        } catch (IOException ioe) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, ioe);
        }
        if (daId.isRead()) {
            LOGGER.debug("Asking for transfer");
            ReadingDataAccessId rdaId = (ReadingDataAccessId) daId;
            LogicalData srcData = rdaId.getReadDataInstance().getData();
            DirectoryTransferable dt = new DirectoryTransferable(daId.isPreserveSourceData());
            Semaphore sem = new Semaphore(0);
            CopyListener listener = new CopyListener(dt, sem);
            if (daId.isWrite()) {
                Comm.getAppHost().getData(srcData, targetName, (LogicalData) null, dt, listener);
            } else {
                Comm.getAppHost().getData(srcData, dt, listener);
            }
            sem.acquireUninterruptibly();
            return listener.getResult();
        } else {
            LOGGER.debug("Write only mode. Auto-release");
            Comm.registerLocation(targetName, targetLocation);
            // Register target location
            LOGGER.debug("Setting target location to " + targetLocation);
            return targetLocation;
        }
    }


    private class CopyListener extends OneOpWithSemListener {

        private final DirectoryTransferable reason;
        private DataLocation targetLocation;


        public CopyListener(DirectoryTransferable reason, Semaphore sem) {
            super(sem);
            this.reason = reason;
        }

        @Override
        public void notifyEnd(DataOperation fOp) {
            String targetPath = this.reason.getDataTarget();
            try {
                SimpleURI targetURI = new SimpleURI(ProtocolType.DIR_URI.getSchema() + targetPath);
                targetLocation = DataLocation.createLocation(Comm.getAppHost(), targetURI);
            } catch (IOException ioe) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, ioe);
            }

            super.notifyEnd(fOp);
        }

        private DataLocation getResult() {
            return this.targetLocation;
        }
    }

}
