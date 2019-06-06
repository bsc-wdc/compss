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
package es.bsc.compss.types.data.accessparams;

import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.data.DataAccessId;


public class BindingObjectAccessParams extends AccessParams {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    private final int hashCode;
    private final BindingObject bindingObject;


    /**
     * Creates a new BindingObjectAccessParams instance.
     * 
     * @param mode Access mode.
     * @param bo Associated BindingObject.
     * @param hashCode Hashcode of the associated BindingObject.
     */
    public BindingObjectAccessParams(AccessMode mode, DataInfoProvider dip, BindingObject bo, int hashCode) {
        super(mode, dip);
        this.bindingObject = bo;
        this.hashCode = hashCode;
    }

    /**
     * Returns the associated BindingObject.
     * 
     * @return The associated BindingObject.
     */
    public BindingObject getBindingObject() {
        return this.bindingObject;
    }

    /**
     * Returns the associated BindingObject's hashcode.
     * 
     * @return The associated BindingObject's hashcode.
     */
    public int getCode() {
        return this.hashCode;
    }

    @Override
    public DataAccessId register() {
        return this.dip.registerBindingObjectAccess(this.mode, this.bindingObject, this.hashCode);
    }

}
