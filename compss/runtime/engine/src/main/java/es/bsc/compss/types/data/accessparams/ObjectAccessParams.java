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
package es.bsc.compss.types.data.accessparams;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.DataVersion;
import es.bsc.compss.types.data.params.ObjectData;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;


public class ObjectAccessParams<T extends Object, D extends ObjectData> extends AccessParams<D> {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

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
        return new ObjectAccessParams(app, new ObjectData(code), dir, value);
    }

    protected ObjectAccessParams(Application app, D data, Direction dir, T value) {
        super(app, data, dir);
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
    public void checkAccessValidity() throws ValueUnawareRuntimeException {
        DataInfo oInfo = data.getRegisteredData(this.app);
        boolean validValue = oInfo.getCurrentDataVersion().isValueOnMain();
        if (validValue) {
            // Main code is still performing the same modification.
            // No need to register it as a new version.
            throw new ValueUnawareRuntimeException();
        }
    }

    @Override
    protected void registerValueForVersion(DataVersion dv) {
        if (mode != AccessMode.W) {
            EngineDataInstanceId lastDID = dv.getDataInstanceId();
            String renaming = lastDID.getRenaming();
            Comm.registerValue(renaming, value);
        } else {
            dv.invalidate();
        }
    }

    @Override
    protected void externalRegister() {
        // Do nothing. No need to register the access anywhere.
    }

    @Override
    public String toString() {
        return "[" + this.mode + " ," + this.getCode() + "]";
    }

}
