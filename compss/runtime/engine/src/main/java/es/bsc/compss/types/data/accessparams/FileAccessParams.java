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
import es.bsc.compss.types.data.DataInfo;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.DataVersion;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessparams.DataParams.FileData;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.data.operation.FileTransferable;
import es.bsc.compss.types.data.operation.OneOpWithSemListener;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import java.io.IOException;
import java.util.concurrent.Semaphore;


public class FileAccessParams<D extends FileData> extends AccessParams<D> {

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
    public static final FileAccessParams constructFAP(Application app, Direction dir, DataLocation loc) {
        FileData fd = new FileData(app, loc);
        return new FileAccessParams(fd, dir);
    }

    protected FileAccessParams(D data, Direction dir) {
        super(data, dir);
    }

    /**
     * Returns the file location.
     * 
     * @return The file location.
     */
    public final DataLocation getLocation() {
        return this.data.getLocation();
    }

    @Override
    public void registeredAsFirstVersionForData(DataInfo dInfo) {
        DataVersion dv = dInfo.getCurrentDataVersion();
        if (mode != AccessMode.W) {
            DataInstanceId lastDID = dv.getDataInstanceId();
            String renaming = lastDID.getRenaming();
            Comm.registerLocation(renaming, this.getLocation());
        } else {
            dv.invalidate();
        }
    }

    @Override
    public void externalRegister() {
        // Do nothing. No need to register the access anywhere.
    }

    /**
     * Fetches the last version of the file.
     *
     * @param daId Data Access Id.
     * @return Location of the transferred open file.
     */
    public DataLocation fetchForOpen(DataAccessId daId, String destDir) {
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

        LOGGER.debug("Openning file " + targetName);

        String pscoId = Comm.getData(targetName).getPscoId();
        if (pscoId == null && daId.isRead()) {
            LOGGER.debug("Asking for transfer");
            ReadingDataAccessId rdaId = (ReadingDataAccessId) daId;
            LogicalData srcData = rdaId.getReadDataInstance().getData();
            Semaphore sem = new Semaphore(0);
            CopyListener listener;
            if (destDir != null) {
                String targetPath = destDir + targetName;
                DataLocation targetLocation = createFileLocation(targetPath);
                FileTransferable ft = new FileTransferable();
                listener = new CopyListener(ft, sem);
                Comm.getAppHost().getData(srcData, targetLocation, (LogicalData) null, ft, listener);
            } else {
                if (rdaId.isWrite()) {
                    FileTransferable ft = new FileTransferable(daId.isPreserveSourceData());
                    listener = new CopyListener(ft, sem);
                    Comm.getAppHost().getData(srcData, targetName, (LogicalData) null, ft, listener);
                } else {
                    FileTransferable ft = new FileTransferable();
                    listener = new CopyListener(ft, sem);
                    Comm.getAppHost().getData(srcData, ft, listener);
                }
            }
            sem.acquireUninterruptibly();
            return listener.getResult();
        } else {
            LOGGER.debug("Auto-release");
            // Create location
            DataLocation targetLocation;
            if (pscoId != null) {
                targetLocation = createPSCOLocation(pscoId);
            } else {
                String targetPath = Comm.getAppHost().getWorkingDirectory() + targetName;
                targetLocation = createFileLocation(targetPath);
            }
            Comm.registerLocation(targetName, targetLocation);
            // Register target location
            LOGGER.debug("Setting target location to " + targetLocation);
            return targetLocation;
        }
    }

    private DataLocation createPSCOLocation(String pscoId) {
        SimpleURI targetURI = new SimpleURI(ProtocolType.PERSISTENT_URI.getSchema() + pscoId);
        return createLocation(targetURI);
    }

    private DataLocation createFileLocation(String localPath) {
        SimpleURI targetURI = new SimpleURI(ProtocolType.FILE_URI.getSchema() + localPath);
        return createLocation(targetURI);
    }

    private static DataLocation createLocation(SimpleURI targetURI) {
        DataLocation targetLocation = null;
        try {
            targetLocation = DataLocation.createLocation(Comm.getAppHost(), targetURI);
        } catch (IOException ioe) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetURI, ioe);
        }
        return targetLocation;
    }

    @Override
    public String toString() {
        return "[" + this.getApp() + ", " + this.mode + " ," + this.getLocation() + "]";
    }


    private class CopyListener extends OneOpWithSemListener {

        private final FileTransferable reason;
        private DataLocation targetLocation;


        public CopyListener(FileTransferable reason, Semaphore sem) {
            super(sem);
            this.reason = reason;
        }

        @Override
        public void notifyEnd(DataOperation fOp) {
            String targetPath = this.reason.getDataTarget();
            try {
                SimpleURI targetURI = new SimpleURI(ProtocolType.FILE_URI.getSchema() + targetPath);
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
