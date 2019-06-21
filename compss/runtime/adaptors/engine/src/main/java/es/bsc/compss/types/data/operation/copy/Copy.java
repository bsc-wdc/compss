/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.listener.SafeCopyListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.util.ErrorManager;

import java.util.concurrent.Semaphore;


public abstract class Copy extends DataOperation {

    protected final LogicalData srcData;
    protected final DataLocation srcLoc;
    protected final LogicalData tgtData;
    protected DataLocation tgtLoc;
    protected final Transferable reason;

    /**
     * Data Copy Constructor.
     *
     * @param srcData  source logical data
     * @param prefSrc  preferred source data location
     * @param prefTgt  preferred target data location
     * @param tgtData  target logical data
     * @param reason   Transfer reason
     * @param listener listener to notify events
     */
    public Copy(LogicalData srcData, DataLocation prefSrc, DataLocation prefTgt, LogicalData tgtData,
            Transferable reason, EventListener listener) {

        super(srcData, listener);
        this.srcData = srcData;
        this.srcLoc = prefSrc;
        this.tgtData = tgtData;
        this.tgtLoc = prefTgt;
        this.reason = reason;
    }

    /**
     * Returns the type of the data value being copied.
     *
     * @return type of value being copied
     */
    public DataType getType() {
        return reason.getType();
    }

    /**
     * Returns the source data.
     *
     * @return
     */
    public LogicalData getSourceData() {
        return this.srcData;
    }

    /**
     * Returns the preferred location of the source data.
     *
     * @return
     */
    public DataLocation getPreferredSource() {
        return this.srcLoc;
    }

    public void setProposedSource(Object source) {
        reason.setDataSource(source);
    }

    /**
     * Returns the target location.
     *
     * @return
     */
    public DataLocation getTargetLoc() {
        return this.tgtLoc;
    }

    /**
     * Returns the target data.
     *
     * @return
     */
    public LogicalData getTargetData() {
        return this.tgtData;
    }

    /**
     * Sets a new final target data.
     *
     * @param targetAbsolutePath Absolute path
     */
    public void setFinalTarget(String targetAbsolutePath) {
        if (DEBUG) {
            LOGGER.debug(" Setting StorageCopy final target to : " + targetAbsolutePath);
        }
        this.reason.setDataTarget(targetAbsolutePath);
    }

    /**
     * Returns whether the target data is registered or not.
     *
     * @return
     */
    public boolean isRegistered() {
        return this.tgtData != null;
    }

    public String getFinalTarget() {
        return reason.getDataTarget();
    }

    /**
     * Blocks the thread until a copy to a resource is finished.
     *
     * @param copy     Copy to wait
     * @param resource Resource
     */
    public static void waitForCopyTofinish(Copy copy, COMPSsNode resource) {
        Semaphore sem = new Semaphore(0);
        SafeCopyListener currentCopylistener = new SafeCopyListener(sem);
        copy.addEventListener(currentCopylistener);
        currentCopylistener.addOperation();
        currentCopylistener.enable();
        try {
            sem.acquire();
        } catch (InterruptedException ex) {
            ErrorManager.warn("Error waiting for files in resource " + resource.getName() + " to get saved");
            Thread.currentThread().interrupt();
        }
        if (DEBUG) {
            LOGGER.debug("Copy " + copy.getName() + "(id: " + copy.getId() + ") is finished");
        }

    }

}
