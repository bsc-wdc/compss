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
package es.bsc.compss.gat.master;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.data.BindingDataManager;
import es.bsc.compss.exceptions.CopyException;
import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.gat.master.exceptions.GATCopyException;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.LocationType;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.copy.ImmediateCopy;
import es.bsc.compss.types.data.transferable.WorkersDebugInfoCopyTransferable;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.util.ErrorManager;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.GATObjectCreationException;
import org.gridlab.gat.URI;
import org.gridlab.gat.io.FileInterface;


public class GATCopy extends ImmediateCopy {

    private static final String ERR_NO_TGT_URI = "No valid target URIs";
    private static final String ERR_NO_SRC_URI = "No valid source URIs";
    private static final String DBG_PREFIX = "[GAT_COPY] ";
    private final Transferable reason;
    boolean isBindingObject = false;


    /**
     * Creates a new GATCOpy instance with the given source and target information.
     * 
     * @param srcData Source LogicalData.
     * @param prefSrc Preferred DataLocation of the source.
     * @param prefTgt Preferred DataLocation of the target.
     * @param tgtData Target LogicalData.
     * @param reason Transfer reason.
     * @param listener Transfer listener.
     */
    public GATCopy(LogicalData srcData, DataLocation prefSrc, DataLocation prefTgt, LogicalData tgtData,
        Transferable reason, EventListener listener) {

        super(srcData, prefSrc, prefTgt, tgtData, reason, listener);
        this.reason = reason;
        if (srcData.isBindingData() || (reason != null && reason.getType().equals(DataType.BINDING_OBJECT_T))
            || (prefSrc != null && prefSrc.getType().equals(LocationType.BINDING))
            || (prefTgt != null && prefTgt.getType().equals(LocationType.BINDING))) {
            isBindingObject = true;
        }
        for (MultiURI uri : prefTgt.getURIs()) {
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
        LOGGER.debug(DBG_PREFIX + "GAT Specific Copy created");
    }

    @Override
    public void specificCopy() throws CopyException {
        LOGGER.debug(DBG_PREFIX + "Performing GAT Specific Copy for " + getName());

        // Fetch valid destination URIs
        List<MultiURI> targetURIs = tgtLoc.getURIs();
        List<URI> selectedTargetURIs = new LinkedList<>();
        for (MultiURI uri : targetURIs) {
            try {
                URI internalURI = (URI) uri.getInternalURI(GATAdaptor.ID);
                if (internalURI != null) {
                    selectedTargetURIs.add(internalURI);
                }
            } catch (UnstartedNodeException une) {
                throw new GATCopyException(une);
            }
        }

        if (selectedTargetURIs.isEmpty()) {
            LOGGER.error(DBG_PREFIX + ERR_NO_TGT_URI);
            throw new GATCopyException(ERR_NO_TGT_URI);
        }
        LOGGER.debug(DBG_PREFIX + "Selected target URIs");
        // Fetch valid source URIs
        List<MultiURI> sourceURIs;
        List<URI> selectedSourceURIs = new LinkedList<>();
        synchronized (srcData) {
            if (srcLoc != null) {
                LOGGER.debug(DBG_PREFIX + "SrcLoc: " + srcLoc);
                sourceURIs = srcLoc.getURIs();
                for (MultiURI uri : sourceURIs) {
                    try {
                        URI internalURI = (URI) uri.getInternalURI(GATAdaptor.ID);
                        if (internalURI != null) {
                            selectedSourceURIs.add(internalURI);
                        }
                    } catch (UnstartedNodeException une) {
                        LOGGER.error(DBG_PREFIX + "Exception selecting source URI");
                        throw new GATCopyException(une);
                    }
                }
            }
            LOGGER.debug(DBG_PREFIX + "SrcData: " + srcData);
            sourceURIs = srcData.getURIs();
            for (MultiURI uri : sourceURIs) {
                try {
                    URI internalURI = (URI) uri.getInternalURI(GATAdaptor.ID);
                    if (internalURI != null) {
                        selectedSourceURIs.add(internalURI);
                    }
                } catch (UnstartedNodeException une) {
                    LOGGER.error(DBG_PREFIX + "Exception selecting source URI for " + getName());
                    throw new GATCopyException(une);
                }
            }

            if (selectedSourceURIs.isEmpty()) {
                if (srcData.isInMemory()) {
                    LOGGER.debug(DBG_PREFIX + "Data for " + getName() + " is in memory");
                    try {
                        srcData.writeToStorage();
                        sourceURIs = srcData.getURIs();
                        for (MultiURI uri : sourceURIs) {
                            URI internalURI = (URI) uri.getInternalURI(GATAdaptor.ID);
                            if (internalURI != null) {
                                selectedSourceURIs.add(internalURI);
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.fatal(DBG_PREFIX + "Exception writing object to file.", e);
                        throw new GATCopyException(ERR_NO_SRC_URI);
                    }
                } else {
                    LOGGER.error(DBG_PREFIX + ERR_NO_SRC_URI);
                    throw new GATCopyException(ERR_NO_SRC_URI);
                }
            }
        }

        GATInvocationException exception = new GATInvocationException("default logical file");
        for (URI src : selectedSourceURIs) {
            for (URI tgt : selectedTargetURIs) {
                try {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(DBG_PREFIX + "Converting URIs From: " + src + " to " + tgt + " to GAT format");
                    }
                    // Source and target URIs contain Runtime information (schema)
                    // Convert it to GAT format and get Binding object type and elements if required
                    String srcPath = src.getPath();
                    int boType = 0;
                    int boElements = 0;
                    if (isBindingObject) {
                        if (srcPath.contains("#")) {
                            BindingObject bo = BindingObject.generate(srcPath);
                            srcPath = bo.getId();
                            boType = bo.getType();
                            boElements = bo.getElements();
                        }
                    }
                    URI gatSrc = new URI(ProtocolType.ANY_URI.getSchema() + src.getHost() + "/" + srcPath);
                    String tgtPath = tgt.getPath();
                    if (isBindingObject) {
                        if (tgtPath.contains("#")) {
                            BindingObject bo = BindingObject.generate(tgtPath);
                            tgtPath = bo.getId();
                            boType = bo.getType();
                            boElements = bo.getElements();
                        }
                    }
                    URI gatTgt = new URI(ProtocolType.ANY_URI.getSchema() + tgt.getHost() + "/" + tgtPath);

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(DBG_PREFIX + "Copying file From: " + gatSrc + " to " + gatTgt);
                    }
                    doCopy(gatSrc, gatTgt);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(DBG_PREFIX + "File " + gatTgt + " copied.");
                    }
                    if (isBindingObject) {
                        if (tgt.getHost().equals(Comm.getAppHost().getName())) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(DBG_PREFIX + "Loading Binding object from " + tgtPath + " with id: "
                                    + getName() + " type: " + boType + " elements: " + boElements);
                            }

                            int result = BindingDataManager.loadFromFile(getName(), tgtPath, boType, boElements);
                            if (result != 0) {
                                LOGGER.error("Error loading object to cache from file " + tgtPath);
                                throw new Exception("Error loading object to cache from file " + tgtPath);
                            }
                        }
                    }

                    // Try to copy from each location until successful
                } catch (Exception e) {
                    exception.add("default logical file", e);
                    LOGGER.warn(DBG_PREFIX + "Error copying file", e);
                    continue;
                }
                return;
            }
        }

        if (!(this.reason instanceof WorkersDebugInfoCopyTransferable)) {
            ErrorManager.error("File '" + srcData.getName() + "' could not be copied because it does not exist.",
                exception);
        }

        throw new GATCopyException(exception);
    }

    private void doCopy(org.gridlab.gat.URI src, org.gridlab.gat.URI dest) throws GATCopyException {
        // Try to copy from each location until successful
        FileInterface f = null;
        LOGGER.debug("RawPath: " + src.getRawPath());
        LOGGER.debug("isLocal: " + src.isLocal());
        if (src.isLocal() && !(new File(src.getRawPath())).exists()) {
            String errorMessage = null;
            if (this.reason instanceof WorkersDebugInfoCopyTransferable) {
                // Only warn, hide error to ErrorManager
                errorMessage = "Workers Debug Information not supported in GAT Adaptor";
                LOGGER.warn(errorMessage);
            } else {
                // Notify error to ErrorManager
                errorMessage = "File '" + src.toString() + "' could not be copied to '" + dest.toString()
                    + "' because it does not exist.";
                ErrorManager.warn(errorMessage);
                LOGGER.warn(errorMessage);
            }
            throw new GATCopyException(errorMessage);
        }

        try {
            f = org.gridlab.gat.GAT.createFile(GATAdaptor.getTransferContext(), src).getFileInterface();
            f.copy(dest);
        } catch (GATObjectCreationException | GATInvocationException e) {
            throw new GATCopyException(e);
        }
    }

}
