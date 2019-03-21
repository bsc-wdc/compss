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

import java.util.concurrent.Semaphore;

import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.listener.SafeCopyListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.util.ErrorManager;


public abstract class Copy extends DataOperation {

    protected final LogicalData srcData;
    protected final DataLocation srcLoc;
    protected final LogicalData tgtData;
    protected DataLocation tgtLoc;
    protected final Transferable reason;


    public Copy(LogicalData srcData, DataLocation prefSrc, DataLocation prefTgt, LogicalData tgtData,
            Transferable reason, EventListener listener) {

        super(srcData, listener);
        this.srcData = srcData;
        this.srcLoc = prefSrc;
        this.tgtData = tgtData;
        this.tgtLoc = prefTgt;
        this.reason = reason;
        if (DEBUG) {
            LOGGER.debug("Created copy " + this.getName() + " (id: " + this.getId() + ")");
        }
    }

    public LogicalData getSourceData() {
        return srcData;
    }

    public DataLocation getPreferredSource() {
        return srcLoc;
    }

    public DataLocation getTargetLoc() {
        return tgtLoc;
    }

    public LogicalData getTargetData() {
        return tgtData;
    }

    public boolean isRegistered() {
        return tgtData != null;
    }

    public void setProposedSource(Object source) {
        reason.setDataSource(source);
    }

    public void setFinalTarget(String targetAbsolutePath) {
        if (DEBUG) {
            LOGGER.debug(" Setting copy final target to : " + targetAbsolutePath);
        }
        reason.setDataTarget(targetAbsolutePath);
    }

    public String getFinalTarget() {
        return reason.getDataTarget();
    }

    public DataType getType() {
        return reason.getType();
    }

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
