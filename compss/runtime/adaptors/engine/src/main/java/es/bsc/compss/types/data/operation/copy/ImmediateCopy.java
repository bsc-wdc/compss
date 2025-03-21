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
package es.bsc.compss.types.data.operation.copy;

import es.bsc.compss.exceptions.CopyException;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.data.operation.OperationEndState;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.uri.MultiURI;

import java.util.LinkedList;


public abstract class ImmediateCopy extends Copy {

    /**
     * Constructs a new ImmediateCopy.
     *
     * @param srcData source logical data
     * @param prefSrc preferred source data location
     * @param prefTgt preferred target data location
     * @param tgtData target logical data
     * @param reason Transfer reason
     * @param listener listener to notify events
     */
    public ImmediateCopy(LogicalData srcData, DataLocation prefSrc, DataLocation prefTgt, LogicalData tgtData,
        Transferable reason, EventListener listener) {
        super(srcData, prefSrc, prefTgt, tgtData, reason, listener);
        if (DEBUG) {
            LOGGER.debug("Created Immediate Copy " + this.getName() + " (id: " + this.getId() + ")");
        }
    }

    /**
     * Perform data copy.
     */
    @Override
    public final void perform() {
        Resource targetHost = ((LinkedList<Resource>) tgtLoc.getHosts()).getFirst();
        LOGGER.debug("THREAD " + Thread.currentThread().getName() + " - Copy file " + getName() + " to " + tgtLoc);

        synchronized (srcData) {
            if (tgtData != null) {
                MultiURI u;
                if ((u = srcData.alreadyAvailable(targetHost)) != null
                    && (srcData.getName().equals(tgtData.getName()))) {
                    setFinalTarget(u.getPath());
                    end(OperationEndState.OP_OK);
                    LOGGER.debug("THREAD " + Thread.currentThread().getName() + " - A copy of " + getName()
                        + " is already present at " + targetHost + " on path " + u.getPath());
                    return;
                }
                Copy copyInProgress = null;
                if ((copyInProgress = srcData.alreadyCopying(tgtLoc)) != null) {
                    String path = copyInProgress.tgtLoc.getURIInHost(targetHost).getPath();
                    setFinalTarget(path);
                    // The same operation is already in progress - no need to repeat it
                    end(OperationEndState.OP_IN_PROGRESS);

                    // This group must be notified as well when the operation finishes
                    synchronized (copyInProgress.getEventListeners()) {
                        copyInProgress.addEventListeners(getEventListeners());
                    }
                    LOGGER.debug("THREAD " + Thread.currentThread().getName() + " - A copy to " + path
                        + " is already in progress, skipping replication");
                    return;
                }
                if (srcData.getLocations().isEmpty()) {
                    for (Copy inProgressCopy : srcData.getCopiesInProgress()) {
                        LOGGER.debug("No source locations for copy " + getName() + "." + " Waiting for copy "
                            + inProgressCopy.getName() + " to finish.");
                        inProgressCopy.addEventListener(new EventListener() {

                            @Override
                            public void notifyEnd(DataOperation fOp) {
                                perform();
                            }

                            @Override
                            public void notifyFailure(DataOperation fOp, Exception e) {
                                end(OperationEndState.OP_FAILED, e);
                            }
                        });
                        return;
                    }
                    end(OperationEndState.OP_FAILED,
                        new Exception(" No source location nor copies in progress for copy " + getName()));

                }

            }
            srcData.startCopy(this, tgtLoc);
        }

        try {
            LOGGER.debug("[InmediateCopy] Performing Inmediate specific Copy for " + getName());
            specificCopy();
        } catch (CopyException e) {
            end(OperationEndState.OP_FAILED, e);
            return;
        } finally {
            DataLocation actualLocation;
            synchronized (srcData) {
                actualLocation = srcData.finishedCopy(this);
            }
            if (tgtData != null) {
                synchronized (tgtData) {
                    tgtData.addLocation(actualLocation);
                }
            }
        }

        String path = tgtLoc.getURIInHost(targetHost).getPath();
        setFinalTarget(path);

        synchronized (srcData) {
            end(OperationEndState.OP_OK);
        }
        LOGGER.debug("[InmediateCopy] Inmediate Copy for " + getName() + " performed.");
    }

    public abstract void specificCopy() throws CopyException;

}
