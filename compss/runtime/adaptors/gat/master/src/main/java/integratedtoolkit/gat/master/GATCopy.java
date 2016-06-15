package integratedtoolkit.gat.master;

import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.location.URI;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.operation.WorkersDebugInfoCopyTransferable;
import integratedtoolkit.types.data.operation.Copy.ImmediateCopy;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.util.ErrorManager;

import java.io.File;
import java.util.LinkedList;

import org.gridlab.gat.GATInvocationException;
import org.gridlab.gat.io.FileInterface;


public class GATCopy extends ImmediateCopy {

    private static final String ERR_NO_TGT_URI = "No valid target URIs";
    private static final String ERR_NO_SRC_URI = "No valid source URIs";
    private final Transferable reason;

    public GATCopy(LogicalData srcData, DataLocation prefSrc, DataLocation prefTgt, LogicalData tgtData, Transferable reason, EventListener listener) {
        super(srcData, prefSrc, prefTgt, tgtData, reason, listener);
        this.reason = reason;

        for (URI uri : prefTgt.getURIs()) {
            String path = uri.getPath();
            if (path.startsWith(File.separator)) {
                break;
            } else {
                Resource host = uri.getHost();
                this.tgtLoc = DataLocation.getLocation(host, host.getCompleteRemotePath(DataType.FILE_T, path));
            }
        }
    }

    @Override
    public void specificCopy() throws Exception {
        //Fetch valid destination URIs
        LinkedList<URI> targetURIs = tgtLoc.getURIs();
        LinkedList<org.gridlab.gat.URI> gatTargetUris = new LinkedList<org.gridlab.gat.URI>();
        for (URI uri : targetURIs) {
            org.gridlab.gat.URI internalURI = (org.gridlab.gat.URI) uri.getInternalURI(GATAdaptor.ID);
            if (internalURI != null) {
                gatTargetUris.add(internalURI);
            }
        }

        if (gatTargetUris.isEmpty()) {
            throw new GATCopyException(ERR_NO_TGT_URI);
        }

        //Fetch valid source URIs
        LinkedList<URI> sourceURIs;
        LinkedList<org.gridlab.gat.URI> gatSrcUris = new LinkedList<org.gridlab.gat.URI>();
        synchronized (srcData) {
            if (srcLoc != null) {
                sourceURIs = srcLoc.getURIs();
                for (URI uri : sourceURIs) {
                    org.gridlab.gat.URI internalURI = (org.gridlab.gat.URI) uri.getInternalURI(GATAdaptor.ID);
                    if (internalURI != null) {
                        gatSrcUris.add(internalURI);
                    }
                }
            }

            sourceURIs = srcData.getURIs();
            for (URI uri : sourceURIs) {
                org.gridlab.gat.URI internalURI = (org.gridlab.gat.URI) uri.getInternalURI(GATAdaptor.ID);
                if (internalURI != null) {
                    gatSrcUris.add(internalURI);
                }
            }

            if (gatSrcUris.isEmpty()) {
                if (srcData.isInMemory()) {
                    try {
                        srcData.writeToFile();
                        sourceURIs = srcData.getURIs();
                        for (URI uri : sourceURIs) {
                            org.gridlab.gat.URI internalURI = (org.gridlab.gat.URI) uri.getInternalURI(GATAdaptor.ID);
                            if (internalURI != null) {
                                gatSrcUris.add(internalURI);
                            }
                        }
                    } catch (Exception e) {
                        logger.fatal("Exception  writing object to file.", e);
                        throw new GATCopyException(ERR_NO_SRC_URI);
                    }
                } else {
                    throw new GATCopyException(ERR_NO_SRC_URI);
                }
            }
        }

        GATInvocationException exception = new GATInvocationException("default logical file");
        for (org.gridlab.gat.URI src : gatSrcUris) {
            for (org.gridlab.gat.URI tgt : gatTargetUris) {
                try {
                    doCopy(src, tgt);
                    // Try to copy from each location until successful
                } catch (Exception e) {
                    exception.add("default logical file", e);
                    continue;
                }
                return;
            }
        }
        throw exception;
    }

    private void doCopy(org.gridlab.gat.URI src, org.gridlab.gat.URI dest) throws Exception {
        // Try to copy from each location until successful
        FileInterface f = null;
        logger.debug("RawPath: " + src.getRawPath());
        logger.debug("isLocal: " + src.isLocal());
        if (src.isLocal() && !(new File(src.getRawPath())).exists()) {
            String errorMessage = null;
            if (this.reason instanceof WorkersDebugInfoCopyTransferable) {
                // Only warn, hide error to ErrorManager
                errorMessage = "Workers Debug Information not supported in GAT Adaptor";
                logger.warn(errorMessage);
            } else {
                // Notify error to ErrorManager
                errorMessage = "File '" + src.toString() + "' could not be copied to '" + dest.toString() + "' because it does not exist.";
                ErrorManager.warn(errorMessage);
                logger.warn(errorMessage);
            }
            throw new Exception(errorMessage);
        }
        f = org.gridlab.gat.GAT.createFile(GATAdaptor.getTransferContext(), src).getFileInterface();
        f.copy(dest);
    }

}
