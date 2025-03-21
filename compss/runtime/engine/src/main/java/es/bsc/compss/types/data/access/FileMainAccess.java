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
package es.bsc.compss.types.data.access;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.accessparams.FileAccessParams;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.FileTransferable;
import es.bsc.compss.types.data.operation.OneOpWithSemListener;
import es.bsc.compss.types.data.params.FileData;
import es.bsc.compss.types.uri.SimpleURI;
import java.util.concurrent.Semaphore;


/**
 * Handling of an access from the main code to a file.
 */
public class FileMainAccess<D extends FileData, P extends FileAccessParams<D>> extends MainAccess<DataLocation, D, P> {

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
        return new FileMainAccess(app, f);
    }

    protected FileMainAccess(Application app, P params) {
        super(app, params);
    }

    @Override
    public DataLocation getUnavailableValueResponse() {
        return this.createExpectedLocalLocation("null");
    }

    @Override
    public final DataLocation fetch(EngineDataAccessId daId) {
        // Get target information
        EngineDataInstanceId tgtDiId;
        if (daId.isWrite()) {
            WritingDataAccessId wdaId = (WritingDataAccessId) daId;
            tgtDiId = wdaId.getWrittenDataInstance();
        } else {
            // Read only mode
            ReadingDataAccessId rdaId = (ReadingDataAccessId) daId;
            tgtDiId = rdaId.getReadDataInstance();
        }
        String targetName = tgtDiId.getRenaming();

        String dataDesc = this.getParameters().getDataDescription();
        LOGGER_API.debug("Openning file " + targetName);

        DataLocation tgtLocation = this.getParameters().getLocation();
        if (daId.isRead()) {
            String pscoId = tgtDiId.getData().getPscoId();
            if (pscoId != null) {
                tgtLocation = fetchPSCO(pscoId, targetName);
            } else {
                tgtLocation = fetchData(daId, targetName);
            }
        }

        if (daId.isWrite()) {
            // Mode contains W
            LOGGER_API.debug("Access to " + dataDesc + " mode contains W, register new writer");
            String targetPath = Comm.getAppHost().getWorkingDirectory() + targetName;
            tgtLocation = createExpectedLocalLocation(targetPath);
            Comm.registerLocation(targetName, tgtLocation);
        }
        if (API_DEBUG) {
            LOGGER_API.debug(dataDesc + " located on " + (tgtLocation != null ? tgtLocation.toString() : "null"));
        }
        return tgtLocation;
    }

    private DataLocation fetchPSCO(String pscoId, String targetName) {
        LOGGER_API.debug("Auto-release");
        // Create location
        DataLocation targetLocation;
        targetLocation = createPSCOLocation(pscoId);
        Comm.registerLocation(targetName, targetLocation);
        // Register target location
        LOGGER_API.debug("Setting target location to " + targetLocation);
        return targetLocation;
    }

    private DataLocation createPSCOLocation(String pscoId) {
        SimpleURI targetURI = new SimpleURI(ProtocolType.PERSISTENT_URI.getSchema() + pscoId);
        return createLocalLocation(targetURI);
    }

    protected DataLocation fetchData(EngineDataAccessId daId, String targetName) {
        LOGGER_API.debug("Asking for transfer");
        ReadingDataAccessId rdaId = (ReadingDataAccessId) daId;
        LogicalData srcData = rdaId.getReadDataInstance().getData();
        Semaphore sem = new Semaphore(0);
        FileTransferable ft;
        LogicalData tgtData;
        if (rdaId.isWrite()) {
            ft = createExpectedTransferable(daId.isPreserveSourceData());
            tgtData = null;
        } else {
            ft = createExpectedTransferable(true);
            tgtData = srcData;
        }
        OneOpWithSemListener listener = new OneOpWithSemListener(sem);
        Comm.getAppHost().getData(srcData, targetName, tgtData, ft, listener);
        sem.acquireUninterruptibly();
        String finalPath = ft.getDataTarget();
        return createExpectedLocalLocation(finalPath);
    }

    protected final DataLocation createExpectedLocalLocation(String localPath) {
        SimpleURI targetURI = new SimpleURI(expectedProtocol().getSchema() + localPath);
        return createLocalLocation(targetURI);
    }

    protected FileTransferable createExpectedTransferable(boolean preserveSource) {
        return new FileTransferable(preserveSource);
    }

    protected ProtocolType expectedProtocol() {
        return ProtocolType.FILE_URI;
    }

    @Override
    public boolean isAccessFinishedOnRegistration() {
        return false;
    }

    @Override
    public boolean resultRemainOnMain() {
        return false;
    }
}
