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
package es.bsc.compss.gos.master;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import es.bsc.compss.exceptions.CopyException;
import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.gos.master.exceptions.GOSCopyException;
import es.bsc.compss.gos.master.monitoring.GOSMonitoring;
import es.bsc.compss.gos.master.monitoring.transfermonitor.GOSTransferMonitor;
import es.bsc.compss.gos.master.sshutils.staticmethods.SSHFileSystem;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.LocationType;
import es.bsc.compss.types.data.operation.OperationEndState;
import es.bsc.compss.types.data.operation.copy.ImmediateAsyncCopy;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.util.ErrorManager;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GOSCopy extends ImmediateAsyncCopy {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final String ERR_NO_TGT_URI = "No valid target URIs";
    private static final String ERR_NO_SRC_URI = "No valid source URIs";
    private final String dbgPrefix;
    private boolean isBindingObject;
    private final GOSWorkerNode workerNode;
    private final String type;


    public void logError(String s, Exception e) {
        LOGGER.error(dbgPrefix + s, e);
    }

    public void logError(String msg) {
        LOGGER.error(msg);
    }

    public void logWarn(String msg) {
        LOGGER.warn(dbgPrefix + msg);
    }

    public GOSWorkerNode getWorkerNode() {
        return workerNode;
    }


    /**
     * The enum GOSCopy state.
     */
    public enum GOSCopyState {
        RUNNING, FAILED, SUCCESSFUL
    }


    private GOSCopyState finishedState;


    /**
     * Constructs a new ImmediateCopy.
     *
     * @param srcData source logical data
     * @param srcLoc preferred source data location
     * @param tgtLoc preferred target data location
     * @param tgtData target logical data
     * @param reason Transfer reason
     * @param listener listener to notify events
     */
    public GOSCopy(LogicalData srcData, DataLocation srcLoc, DataLocation tgtLoc, LogicalData tgtData,
        Transferable reason, EventListener listener, GOSWorkerNode node, String type) {

        super(srcData, srcLoc, tgtLoc, tgtData, reason, listener);
        this.workerNode = node;
        this.type = type;
        isBindingObject();
        treatUris();
        this.finishedState = GOSCopyState.RUNNING;
        this.dbgPrefix = "[GOS_COPY " + this.getId() + "] ";
        LOGGER.debug(dbgPrefix + "created copy type " + type + " " + this);

    }

    @Override
    public String toString() {
        String name = (srcData != null) ? srcData.getName() : "null";
        String path = (srcLoc != null) ? srcLoc.getPath() : "null";
        String s = "[" + name + " - " + path + "] ==> ";
        name = (tgtData != null) ? tgtData.getName() : "null";
        path = (tgtLoc != null) ? tgtLoc.getPath() : "null";
        s += "[" + name + " - " + path + "]";
        return s;
    }

    private void treatUris() {
        for (MultiURI uri : tgtLoc.getURIs()) {
            String path = uri.getPath();
            if (path.startsWith(File.separator)) {
                break;
            } else {
                Resource host = uri.getHost();
                try {
                    if (isBindingObject) {
                        this.tgtLoc = DataLocation.createLocation(host,
                            host.getCompleteRemotePath(DataType.BINDING_OBJECT_T, path));
                    } else {
                        this.tgtLoc =
                            DataLocation.createLocation(host, host.getCompleteRemotePath(DataType.FILE_T, path));
                    }
                } catch (Exception e) {
                    ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
                }
            }
        }
    }

    private void isBindingObject() {
        isBindingObject =
            srcData.isBindingData() || (reason != null && reason.getType().equals(DataType.BINDING_OBJECT_T))
                || (srcLoc != null && srcLoc.getType().equals(LocationType.BINDING))
                || (tgtData != null && tgtLoc.getType().equals(LocationType.BINDING));
    }

    @Override
    public void specificCopy() throws CopyException {
        LOGGER.debug(dbgPrefix + "performing specific copy");

        // Fetch valid destination URIs
        List<MultiURI> targetURIs = tgtLoc.getURIs();
        final List<GOSUri> selectedTargetURIs = selectTargetUris(targetURIs);

        // Fetch valid source URIs
        List<GOSUri> selectedSourceURIs;
        synchronized (srcData) {
            selectedSourceURIs = selectSourceUris();
        }

        launchAsyncCopy(selectedSourceURIs, selectedTargetURIs);
    }

    private void launchAsyncCopy(List<GOSUri> listSrc, List<GOSUri> listDst) throws GOSCopyException {

        GOSUri src = listSrc.get(0);
        GOSUri dst = listDst.get(0);
        if (listSrc.size() > 1 || listDst.size() > 1) {
            LOGGER.debug("MORE THAN ONE SOURCE OR DST in doCopy");
        }
        GOSTransferMonitor monitor;
        try {
            monitor = SSHFileSystem.transferFile(this, src, dst);
        } catch (JSchException | IOException | SftpException e) {
            LOGGER.error(dbgPrefix + "Error in ssh transfer", e);
            throw new GOSCopyException(e);
        }
        // If the transfer is fully local transferFile return null;
        if (monitor != null) {
            getMonitoring().addTransferMonitor(monitor);
        } else {
            LOGGER.warn("Monitor for " + this + " is null");
        }
    }

    private List<GOSUri> selectSourceUris() throws GOSCopyException {
        List<MultiURI> sourceURIs;
        List<GOSUri> selectedSourceURIs = new LinkedList<>();

        if (srcLoc != null) {
            sourceURIs = srcLoc.getURIs();
            for (MultiURI uri : sourceURIs) {
                try {
                    GOSUri internalURI = (GOSUri) uri.getInternalURI(GOSAdaptor.ID);
                    if (internalURI != null) {
                        selectedSourceURIs.add(internalURI);
                    }
                } catch (UnstartedNodeException une) {
                    LOGGER.error(dbgPrefix + "Exception selecting source URI");
                    throw new GOSCopyException(une);
                }
            }
        }

        LOGGER.debug(dbgPrefix + "SrcData: " + srcData);
        sourceURIs = srcData.getURIs();
        for (MultiURI uri : sourceURIs) {
            try {
                GOSUri internalURI = (GOSUri) uri.getInternalURI(GOSAdaptor.ID);
                if (internalURI != null) {
                    selectedSourceURIs.add(internalURI);
                }
            } catch (UnstartedNodeException une) {
                LOGGER.error(dbgPrefix + "Exception selecting source URI for " + getName());
                throw new GOSCopyException(une);
            }
        }

        if (selectedSourceURIs.isEmpty()) {
            if (srcData.isInMemory()) {
                LOGGER.debug(dbgPrefix + "Data for " + getName() + " is in memory.");
                try {
                    srcData.writeToStorage();
                    sourceURIs = srcData.getURIs();
                    for (MultiURI uri : sourceURIs) {
                        GOSUri internalURI = (GOSUri) uri.getInternalURI(GOSAdaptor.ID);
                        if (internalURI != null) {
                            selectedSourceURIs.add(internalURI);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error(dbgPrefix + "Exception writing object to file. " + e);
                    throw new GOSCopyException(ERR_NO_SRC_URI);
                }
            } else {
                LOGGER.error(dbgPrefix + ERR_NO_SRC_URI);
                throw new GOSCopyException(ERR_NO_SRC_URI);
            }
        }

        return selectedSourceURIs;
    }

    private List<GOSUri> selectTargetUris(List<MultiURI> targetURIs) throws GOSCopyException {
        List<GOSUri> selectedTargetURIs;
        selectedTargetURIs = new LinkedList<>();
        for (MultiURI uri : targetURIs) {
            try {
                GOSUri internalURI = (GOSUri) uri.getInternalURI(GOSAdaptor.ID);
                if (internalURI != null) {
                    selectedTargetURIs.add(internalURI);
                }
            } catch (UnstartedNodeException une) {
                throw new GOSCopyException(une);
            }
        }
        if (selectedTargetURIs.isEmpty()) {
            LOGGER.error(dbgPrefix + ERR_NO_TGT_URI);
            throw new GOSCopyException(ERR_NO_TGT_URI);
        }
        return selectedTargetURIs;
    }

    public GOSMonitoring getMonitoring() {
        return workerNode.getConfig().getMonitoring();
    }

    /**
     * Mark as finished.
     */
    public void markAsFinished(boolean success) {
        if (success) {
            finishedState = GOSCopyState.SUCCESSFUL;
        } else {
            finishedState = GOSCopyState.FAILED;
        }
    }

    /**
     * Sets state finished STATE of GOSCopy.
     *
     * @param state the state that wants to be set to.
     */
    public void setState(GOSCopyState state) {
        finishedState = state;
    }

    /**
     * Notify state with internal parameters.
     */
    public void notifyEnd() {
        if (finishedState.equals(GOSCopyState.FAILED)) {
            LOGGER.debug(dbgPrefix + "copy was a failure.");
            GOSCopyException e = new GOSCopyException("Failure in Async GOSCopy..");
            notifyEndAsyncCopy(OperationEndState.OP_FAILED, e);
            return;
        }
        if (finishedState.equals(GOSCopyState.SUCCESSFUL)) {
            LOGGER.debug(dbgPrefix + "copy " + type + " was a success.");
            notifyEndAsyncCopy(OperationEndState.OP_OK, null);
            return;
        }
        notifyEndAsyncCopy(OperationEndState.OP_IN_PROGRESS, null);
    }

    /**
     * Notify end of async copy.
     * 
     * @param state end state
     * @param e Exception in case of failure state, can be null in case of success
     */
    public void notifyEndAsyncCopy(OperationEndState state, CopyException e) {
        if (state.equals(OperationEndState.OP_IN_PROGRESS)) {
            end(state);
            LOGGER.error("Should not be called in progress here.");
        }
        if (state.equals(OperationEndState.OP_OK)) {
            DataLocation actualLocation;
            synchronized (srcData) {
                actualLocation = srcData.finishedCopy(this);

                if (tgtData != null && actualLocation != null) {
                    synchronized (tgtData) {
                        tgtData.addLocation(actualLocation);
                    }
                }
            }
            end(state);
            LOGGER.debug(dbgPrefix + "Async copy done " + getName() + " done.");
        } else {
            LOGGER.debug(dbgPrefix + "Failure during copy.");
            end(state, e);
        }

    }
}
