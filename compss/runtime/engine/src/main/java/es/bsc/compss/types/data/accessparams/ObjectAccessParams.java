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
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.DataInfo;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.DataParams.ObjectData;
import es.bsc.compss.types.data.DataVersion;
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
    public void checkAccessValidity(DataInfoProvider dip) throws ValueUnawareRuntimeException {
        DataInstanceId dId = dip.getLastDataAccess(this.data);
        boolean validValue = dip.isHere(dId);
        if (validValue) {
            // Main code is still performing the same modification.
            // No need to register it as a new version.
            throw new ValueUnawareRuntimeException();
        }
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
    public boolean resultRemainOnMain() {
        return true;
    }

    @Override
    public void externalRegister() {
        // Do nothing. No need to register the access anywhere.
    }

    @Override
    public String toString() {
        return "[" + this.getApp() + ", " + this.mode + " ," + this.getCode() + "]";
    }

}
