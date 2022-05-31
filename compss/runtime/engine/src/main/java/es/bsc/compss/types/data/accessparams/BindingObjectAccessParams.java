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

import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.data.DataAccessId;


public class BindingObjectAccessParams extends ObjectAccessParams {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Creates a new BindingObjectAccessParams instance.
     * 
     * @param app Id of the application accessing the BindingObject.
     * @param mode Access mode.
     * @param bo Associated BindingObject.
     * @param hashCode Hashcode of the associated BindingObject.
     */
    public BindingObjectAccessParams(Application app, AccessMode mode, BindingObject bo, int hashCode) {
        super(app, mode, bo, hashCode);
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
    public DataAccessId registerAccess(DataInfoProvider dip) {
        return dip.registerBindingObjectAccess(this.app, this.mode, this.getBindingObject(), this.getCode());
    }

}
