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
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.ObjectTransferable;
import es.bsc.compss.types.data.operation.OneOpWithSemListener;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.serializers.Serializer;
import java.io.IOException;

import java.util.concurrent.Semaphore;


/**
 * The TransferObjectRequest is a request for an object contained in a remote worker.
 */
public class TransferObjectRequest extends APRequest {

    private final DataAccessId daId;
    private final Semaphore sem;

    private Object response;

    private LogicalData target;


    /**
     * Constructs a new TransferObjectRequest.
     *
     * @param daId Object required, data id + version.
     * @param sem Semaphore where to synchronize until the operation is done.
     */
    public TransferObjectRequest(DataAccessId daId, Semaphore sem) {
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
     * Returns the requested LogicalData instance.
     *
     * @return the requested LogicalData instance.
     */
    public LogicalData getTargetData() {
        return this.target;
    }

    /**
     * Sets the requested LogicalData instance.
     *
     * @param ld the requested LogicalData instance.
     */
    public void setTargetData(LogicalData ld) {
        this.target = ld;

    }

    /**
     * Sets the requested object.
     *
     * @param response The requested object.
     */
    public void setResponse(Object response) {
        this.response = response;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        transferObjectValue();
    }

    /**
     * Transfers the value of an object.
     */
    private void transferObjectValue() {
        Semaphore sem = this.getSemaphore();
        DataAccessId daId = this.getDaId();
        RWAccessId rwaId = (RWAccessId) daId;
        String sourceName = rwaId.getReadDataInstance().getRenaming();
        // String targetName = rwaId.getWrittenDataInstance().getRenaming();
        if (DEBUG) {
            LOGGER.debug("Requesting getting object " + sourceName);
        }
        LogicalData ld = rwaId.getReadDataInstance().getData();

        if (ld == null) {
            ErrorManager.error("Unregistered data " + sourceName);
            return;
        }

        if (ld.isInMemory()) {
            Object value = null;
            if (!rwaId.isPreserveSourceData()) {
                value = ld.getValue();
                // Clear value
                ld.removeValue();
            } else {
                try {
                    ld.writeToStorage();
                } catch (Exception e) {
                    ErrorManager.error("Exception writing object to file.", e);
                }
                for (DataLocation loc : ld.getLocations()) {
                    if (loc.getProtocol() != ProtocolType.OBJECT_URI) {
                        MultiURI mu = loc.getURIInHost(Comm.getAppHost());
                        String path = mu.getPath();
                        try {
                            value = Serializer.deserialize(path);
                            break;
                        } catch (IOException | ClassNotFoundException e) {
                            ErrorManager.error("Exception writing object to file.", e);
                        }
                    }
                }
            }
            // Set response
            this.setResponse(value);
            this.setTargetData(ld);
            sem.release();
        } else {
            if (DEBUG) {
                LOGGER.debug(
                    "Object " + sourceName + " not in memory. Requesting tranfers to " + Comm.getAppHost().getName());
            }
            DataLocation targetLocation = null;
            String path = ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getWorkingDirectory() + sourceName;
            try {
                SimpleURI uri = new SimpleURI(path);
                targetLocation = DataLocation.createLocation(Comm.getAppHost(), uri);
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
            }
            this.setTargetData(ld);
            Comm.getAppHost().getData(ld, targetLocation, new ObjectTransferable(), new OneOpWithSemListener(sem));
        }

    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.TRANSFER_OBJECT;
    }

}
