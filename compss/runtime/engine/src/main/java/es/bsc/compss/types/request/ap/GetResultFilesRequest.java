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
package es.bsc.compss.types.request.ap;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.ResultFile;
import es.bsc.compss.types.data.info.FileInfo;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.PersistentLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.DirectoryTransferable;
import es.bsc.compss.types.data.operation.FileTransferable;
import es.bsc.compss.types.data.operation.ResultListener;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.Tracer;

import java.io.File;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Semaphore;

import storage.StorageException;
import storage.StorageItf;


public class GetResultFilesRequest implements APRequest {

    // Constants definition
    private static final String RES_FILE_TRANSFER_ERR = "Error transferring result files";

    private final Application app;
    private final Semaphore sem;

    private final LinkedList<ResultFile> blockedData;


    /**
     * Creates a new request to retrieve the result files.
     *
     * @param app Application.
     * @param sem Waiting semaphore.
     */
    public GetResultFilesRequest(Application app, Semaphore sem) {
        this.app = app;
        this.sem = sem;
        this.blockedData = new LinkedList<>();
    }

    /**
     * Returns the application of the request.
     *
     * @return The application of the request.
     */
    public Application getApp() {
        return this.app;
    }

    /**
     * Returns the waiting semaphore of the request.
     *
     * @return The waiting semaphore of the request.
     */
    public Semaphore getSemaphore() {
        return this.sem;
    }

    /**
     * Returns a list containing the blocked result files.
     *
     * @return A list containing the blocked result files.
     */
    public LinkedList<ResultFile> getBlockedData() {
        return this.blockedData;
    }

    @Override
    public void process(AccessProcessor ap, TaskDispatcher td) {
        ResultListener listener = new ResultListener(sem);
        Set<FileInfo> writtenData = this.app.getWrittenFiles();
        if (writtenData != null) {
            for (FileInfo fInfo : writtenData) {
                ResultFile rf;
                rf = blockResultFile(fInfo);
                if (rf != null) {
                    fetchResult(rf, listener);
                    this.blockedData.add(rf);
                }

            }
            listener.enable();
        } else {
            this.sem.release();
        }

    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.BLOCK_AND_GET_RESULT_FILES;
    }

    /**
     * Blocks fInfo and retrieves its result file.
     *
     * @param fInfo Data Id.
     * @return The result file.
     */
    private ResultFile blockResultFile(FileInfo fInfo) {
        EngineDataInstanceId lastVersion;
        if (DEBUG) {
            int dataId = fInfo.getDataId();
            LOGGER.debug("Get Result file for data " + dataId);
        }
        if (fInfo.isCurrentVersionBeenUsed()) {
            if (!fInfo.isCurrentVersionToDelete()) { // If current version is to delete do not
                // transfer
                String[] splitPath = fInfo.getOriginalLocation().getPath().split(File.separator);
                String origName = splitPath[splitPath.length - 1];
                if (origName.startsWith("compss-serialized-obj_")) {
                    // Do not transfer objects serialized by the bindings
                    if (DEBUG) {
                        LOGGER.debug("Discarding file " + origName + " as a result");
                    }
                    return null;
                }
                fInfo.blockDeletions();

                // Look for the last available version
                lastVersion = fInfo.getCurrentDataVersion().getDataInstanceId();
                String renaming = lastVersion.getRenaming();
                while (renaming != null && !Comm.existsData(renaming)) {
                    renaming = EngineDataInstanceId.previousVersionRenaming(renaming);
                }
                if (renaming == null) {
                    LOGGER.error(RES_FILE_TRANSFER_ERR + ": Cannot transfer file " + lastVersion.getRenaming()
                        + " nor any of its previous versions");
                    return null;
                }
                LogicalData data = Comm.getData(renaming);

                return new ResultFile(fInfo, data);
            } else {
                if (fInfo.isCurrentVersionToDelete()) {
                    if (DEBUG) {
                        String[] splitPath = fInfo.getOriginalLocation().getPath().split(File.separator);
                        String origName = splitPath[splitPath.length - 1];
                        LOGGER.debug("Trying to delete file " + origName);
                    }
                    fInfo.delete();
                }
            }
        }

        return null;
    }

    private void fetchResult(ResultFile rf, ResultListener listener) {
        LogicalData data = rf.getData();

        // Check if data is a PSCO and must be consolidated
        for (DataLocation loc : data.getLocations()) {
            if (loc instanceof PersistentLocation) {
                String pscoId = ((PersistentLocation) loc).getId();
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(TraceEvent.STORAGE_CONSOLIDATE);
                }
                try {
                    StorageItf.consolidateVersion(pscoId);
                } catch (StorageException e) {
                    LOGGER.error("Cannot consolidate PSCO " + pscoId, e);
                } finally {
                    if (Tracer.isActivated()) {
                        Tracer.emitEventEnd(TraceEvent.STORAGE_CONSOLIDATE);
                    }
                }
                LOGGER.debug("Returned because persistent object");
                return;
            }

        }

        // If no PSCO location is found, perform normal getData
        if (rf.getOriginalLocation().getProtocol() == ProtocolType.BINDING_URI) {
            // Comm.getAppHost().getData(data, rf.getOriginalLocation(), new
            // BindingObjectTransferable(),
            // listener);
            if (DEBUG) {
                int dataId = rf.getFileInfo().getDataId();
                LOGGER.debug("Discarding data d" + dataId + " as a result because it is a binding object");
            }
        } else {
            DataLocation origLoc = rf.getOriginalLocation();
            if (origLoc.getProtocol() == ProtocolType.DIR_URI) {
                listener.addOperation();
                Comm.getAppHost().getData(data, origLoc, new DirectoryTransferable(), listener);
            } else {
                listener.addOperation();
                Comm.getAppHost().getData(data, origLoc, new FileTransferable(), listener);
            }
        }
    }
}
