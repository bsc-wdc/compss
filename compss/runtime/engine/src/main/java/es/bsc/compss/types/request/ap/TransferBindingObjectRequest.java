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
package es.bsc.compss.types.request.ap;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.location.BindingObjectLocation;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.operation.BindingObjectTransferable;
import es.bsc.compss.types.data.operation.OneOpWithSemListener;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.ErrorManager;

import java.util.concurrent.Semaphore;


/**
 * The TransferObjectRequest is a request for an object contained in a remote worker.
 */
public class TransferBindingObjectRequest extends APRequest {

    private final Semaphore sem;
    private final DataAccessId daId;

    private Object response;

    private LogicalData target;
    private String targetName;


    /**
     * Constructs a new TransferObjectRequest.
     *
     * @param daId Object required, data id + version.
     * @param sem Semaphore where to synchronize until the operation is done.
     */
    public TransferBindingObjectRequest(DataAccessId daId, Semaphore sem) {
        this.daId = daId;
        this.sem = sem;
    }

    /**
     * Returns the data id + version of the required object.
     *
     * @return data id + version of the required object.
     */
    public DataAccessId getDaId() {
        return this.daId;
    }

    /**
     * Returns the semaphore where to synchronize until the object can be read.
     *
     * @return the semaphore where to synchronize until the object can be read.
     */
    public Semaphore getSemaphore() {
        return this.sem;
    }

    /**
     * Returns the requested object (Null if it was on a file).
     *
     * @return the requested object (Null if it was on a file).
     */
    public Object getResponse() {
        return this.response;
    }

    /**
     * Sets the requested object.
     *
     * @param response The requested object.
     */
    public void setResponse(Object response) {
        this.response = response;
    }

    /**
     * Returns the requested LogicalData instance.
     *
     * @return The requested LogicalData instance.
     */
    public LogicalData getLogicalDataTarget() {
        return this.target;
    }

    /**
     * Returns the target data.
     * 
     * @param ld The target data.
     */
    public void setTargetData(LogicalData ld) {
        this.target = ld;

    }

    /**
     * Returns the target name.
     * 
     * @return The target name.
     */
    public String getTargetName() {
        return this.targetName;
    }

    /**
     * Sets a new target name.
     * 
     * @param name New target name.
     */
    public void setTargetName(String name) {
        this.targetName = name;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        this.target = transferBindingObject();
    }

    private LogicalData transferBindingObject() {
        DataAccessId daId = this.getDaId();
        RAccessId rwaId = (RAccessId) daId;
        String sourceName = rwaId.getReadDataInstance().getRenaming();

        if (DEBUG) {
            LOGGER.debug("[DataInfoProvider] Requesting getting object " + sourceName);
        }
        LogicalData srcLd = rwaId.getReadDataInstance().getData();
        if (DEBUG) {
            LOGGER.debug("[DataInfoProvider] Logical data for binding object is:" + srcLd);
        }
        if (srcLd == null) {
            ErrorManager.error("Unregistered data " + sourceName);
            return null;
        }
        if (DEBUG) {
            LOGGER.debug("Requesting tranfers binding object " + sourceName + " to " + Comm.getAppHost().getName());
        }

        Semaphore sem = this.getSemaphore();
        BindingObject srcBO = BindingObject.generate(srcLd.getURIs().get(0).getPath());
        BindingObject tgtBO = new BindingObject(sourceName, srcBO.getType(), srcBO.getElements());
        LogicalData tgtLd = srcLd;
        DataLocation targetLocation = new BindingObjectLocation(Comm.getAppHost(), tgtBO);
        Transferable transfer = new BindingObjectTransferable(this);

        Comm.getAppHost().getData(srcLd, targetLocation, tgtLd, transfer, new OneOpWithSemListener(sem));
        if (DEBUG) {
            LOGGER.debug(" Setting tgtName " + transfer.getDataTarget() + " in " + Comm.getAppHost().getName());
        }
        return srcLd;
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.TRANSFER_OBJECT;
    }

}
