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
import es.bsc.compss.types.data.DataParams.FileData;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.RAccessId;

import es.bsc.compss.types.data.accessparams.FileAccessParams;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.FileTransferable;
import es.bsc.compss.types.data.operation.OneOpWithSemListener;
import es.bsc.compss.types.uri.SimpleURI;
import java.util.concurrent.Semaphore;


/**
 * Handling of an access from the main code to a file.
 */
public class FileMainAccess<D extends FileData, P extends FileAccessParams<D>> extends MainAccess<D, P> {

    /**
     * Creates a new FileMainAccess instance with the given mode {@code mode} and for the given file location
     * {@code loc}.
     *
     * @param app Id of the application accessing the file.
     * @param dir operation performed.
     * @param loc File location.
     * @return new FileMainAccess instance
     */
    public static FileMainAccess<FileData, FileAccessParams<FileData>> constructFMA(Application app, Direction dir,
        DataLocation loc) {
        FileAccessParams<FileData> f = FileAccessParams.constructFAP(app, dir, loc);
        return new FileMainAccess(f);
    }

    protected FileMainAccess(P params) {
        super(params);
    }

    /**
     * Fetches the last version of the file.
     *
     * @param daId Data Access Id.
     * @return Location of the transferred open file.
     */
    public DataLocation fetchForOpen(DataAccessId daId) {
        // Get target information
        DataInstanceId diId;
        if (daId.isWrite()) {
            DataAccessId.WritingDataAccessId waId = (DataAccessId.WritingDataAccessId) daId;
            diId = waId.getWrittenDataInstance();
        } else {
            // Read only mode
            RAccessId raId = (RAccessId) daId;
            diId = raId.getReadDataInstance();
        }
        String targetName = diId.getRenaming();

        LOGGER.debug("Openning file " + targetName);

        String pscoId = Comm.getData(targetName).getPscoId();
        if (pscoId == null && daId.isRead()) {
            LOGGER.debug("Asking for transfer");
            DataAccessId.ReadingDataAccessId rdaId = (DataAccessId.ReadingDataAccessId) daId;
            LogicalData srcData = rdaId.getReadDataInstance().getData();
            Semaphore sem = new Semaphore(0);
            FileTransferable ft;
            if (rdaId.isWrite()) {
                ft = new FileTransferable(daId.isPreserveSourceData());
                OneOpWithSemListener listener = new OneOpWithSemListener(sem);
                Comm.getAppHost().getData(srcData, targetName, (LogicalData) null, ft, listener);
            } else {
                ft = new FileTransferable();
                OneOpWithSemListener listener = new OneOpWithSemListener(sem);
                Comm.getAppHost().getData(srcData, ft, listener);
            }
            sem.acquireUninterruptibly();
            String finalPath = ft.getDataTarget();
            return createFileLocation(finalPath);
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
        return createLocalLocation(targetURI);
    }

    private DataLocation createFileLocation(String localPath) {
        SimpleURI targetURI = new SimpleURI(ProtocolType.FILE_URI.getSchema() + localPath);
        return createLocalLocation(targetURI);
    }

}
