/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;

import java.util.concurrent.Semaphore;

import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.LogicalData;


/**
 * The TransferObjectRequest is a request for an object contained in a remote worker
 */
public class TransferBindingObjectRequest extends APRequest {

    /**
     * Data Access Id
     */
    private DataAccessId daId;
    /**
     * Semaphore where to synchronize until the operation is done
     */
    private Semaphore sem;
    /**
     * Object asked for
     */
    private Object response;

    /**
     * LogicalData referring to the final object location
     */
    private LogicalData target;

    private String targetName;


    /**
     * Constructs a new TransferObjectRequest
     *
     * @param daId Object required, data id + version
     * @param sem Semaphore where to synchronize until the operation is done
     */
    public TransferBindingObjectRequest(DataAccessId daId, Semaphore sem) {
        this.daId = daId;
        this.sem = sem;
    }

    /**
     * Returns the data id + version of the required object
     *
     * @return data id + version of the required object
     */
    public DataAccessId getDaId() {
        return daId;
    }

    /**
     * Sets the requested data id and version
     *
     * @param daId data id + version of the required object
     */
    public void setDaId(DataAccessId daId) {
        this.daId = daId;
    }

    /**
     * Returns the semaphore where to synchronize until the object can be read
     *
     * @return the semaphore where to synchronize until the object can be read
     */
    public Semaphore getSemaphore() {
        return sem;
    }

    /**
     * Sets the semaphore where to synchronize until the requested object can be read
     *
     * @param sem the semaphore where to synchronize until the requested object can be read
     */
    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }

    /**
     * Returns the requested object (Null if it was on a file).
     *
     * @return the requested object (Null if it was on a file).
     */
    public Object getResponse() {
        return response;
    }

    /**
     * Returns the requested LogicalData instance
     *
     * @return the requested LogicalData instance
     */
    public LogicalData getLogicalDataTarget() {
        return target;
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
        this.target = dip.transferBindingObject(this);
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.TRANSFER_OBJECT;
    }

    public void setTargetData(LogicalData ld) {
        this.target = ld;

    }

    public void setTargetName(String name) {
        this.targetName = name;
    }

    public String getTargetName() {
        return this.targetName;
    }

}
