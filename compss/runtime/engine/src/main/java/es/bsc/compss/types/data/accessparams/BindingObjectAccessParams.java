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
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.DataInfo;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.DataParams.BindingObjectData;
import es.bsc.compss.types.data.DataVersion;


public class BindingObjectAccessParams extends ObjectAccessParams<BindingObject, BindingObjectData> {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Creates a new BindingObjectAccessParams instance.
     *
     * @param app Id of the application accessing the BindingObject.
     * @param dir operation performed.
     * @param bo Associated BindingObject.
     * @param hashCode Hashcode of the associated BindingObject.
     * @return new BindingObjectAccessParams instance
     */
    public static final BindingObjectAccessParams constructBOAP(Application app, Direction dir, BindingObject bo,
        int hashCode) {
        return new BindingObjectAccessParams(app, dir, bo, hashCode);
    }

    private BindingObjectAccessParams(Application app, Direction dir, BindingObject bo, int hashCode) {
        super(new BindingObjectData(app, hashCode), dir, bo);
    }

    /**
     * Returns the associated BindingObject.
     * 
     * @return The associated BindingObject.
     */
    public BindingObject getBindingObject() {
        return (BindingObject) this.getValue();
    }

    @Override
    public void registeredAsFirstVersionForData(DataInfo dInfo) {
        DataVersion dv = dInfo.getCurrentDataVersion();
        if (mode != AccessMode.W) {
            DataInstanceId lastDID = dv.getDataInstanceId();
            String renaming = lastDID.getRenaming();
            Comm.registerBindingObject(renaming, getBindingObject());
        } else {
            dv.invalidate();
        }
    }

    @Override
    public boolean resultRemainOnMain() {
        return false;
    }

}
