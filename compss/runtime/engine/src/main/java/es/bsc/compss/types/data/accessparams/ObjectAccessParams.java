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
package es.bsc.compss.types.data.accessparams;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataInfo;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.DataVersion;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessparams.DataParams.ObjectData;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.ObjectTransferable;
import es.bsc.compss.types.data.operation.OneOpWithSemListener;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import java.util.concurrent.Semaphore;


public class ObjectAccessParams<T extends Object, D extends ObjectData> extends AccessParams<D> {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    private static final String ERROR_OBJECT_LOAD = "ERROR: Cannot load object from storage (file or PSCO)";

    private final T value;


    /**
     * Creates a new ObjectAccessParams instance for the given object.
     *
     * @param app Id of the application accessing the object.
     * @param dir operation performed.
     * @param value Associated object.
     * @param code Hashcode of the associated object.
     * @return new ObjectAccessParams instance
     */
    public static final <T extends Object> ObjectAccessParams<T, ObjectData> constructObjectAP(Application app,
        Direction dir, T value, int code) {
        return new ObjectAccessParams(new ObjectData(app, code), dir, value);
    }

    protected ObjectAccessParams(D data, Direction dir, T value) {
        super(data, dir);
        this.value = value;
    }

    /**
     * Returns the associated object.
     *
     * @return The associated object.
     */
    public T getValue() {
        return value;
    }

    /**
     * Returns the hashcode of the associated object.
     *
     * @return The hashcode of the associated object.
     */
    public final int getCode() {
        return this.data.getCode();
    }

    @Override
    public void registeredAsFirstVersionForData(DataInfo dInfo) {
        DataVersion dv = dInfo.getCurrentDataVersion();
        if (mode != AccessMode.W) {
            DataInstanceId lastDID = dv.getDataInstanceId();
            String renaming = lastDID.getRenaming();
            Comm.registerValue(renaming, value);
        } else {
            dv.invalidate();
        }
    }

    @Override
    public void externalRegister() {
        // Do nothing. No need to register the access anywhere.
    }

    /**
     * Fetches the last version of the object.
     *
     * @param daId Data Access Id.
     * @return Location of the transferred open file.
     */
    public T fetchObject(DataAccessId daId) {

        RWAccessId rwaId = (RWAccessId) daId;
        DataInstanceId diId = rwaId.getReadDataInstance();
        String sourceName = diId.getRenaming();
        if (DEBUG) {
            LOGGER.debug("Requesting getting object " + sourceName);
        }
        LogicalData ld = diId.getData();
        if (ld == null) {
            ErrorManager.error("Unregistered data " + sourceName);
            return null;
        }

        if (ld.isInMemory()) {
            if (!daId.isPreserveSourceData()) {
                return (T) ld.removeValue();
            } else {
                try {
                    ld.writeToStorage();
                } catch (Exception e) {
                    ErrorManager.error("Exception writing object to storage.", e);
                }
            }
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
            Semaphore sem = new Semaphore(0);
            Comm.getAppHost().getData(ld, targetLocation, new ObjectTransferable(), new OneOpWithSemListener(sem));
            sem.acquireUninterruptibly();
        }

        try {
            return (T) ld.readFromStorage();
        } catch (Exception e) {
            String errMsg = ERROR_OBJECT_LOAD + ": " + ((ld == null) ? "null" : ld.getName());
            LOGGER.fatal(errMsg, e);
            ErrorManager.fatal(errMsg, e);
        }
        return null;
    }

    @Override
    public String toString() {
        return "[" + this.getApp() + ", " + this.mode + " ," + this.getCode() + "]";
    }

}
