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
import es.bsc.compss.types.data.DataAccessId;


public class ObjectAccessParams extends AccessParams {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    private int hashCode;
    private Object value;


    /**
     * Creates a new ObjectAccessParams instance for the given object.
     * 
     * @param app Id of the application accessing the object.
     * @param mode Access mode.
     * @param value Associated object.
     * @param hashCode Hashcode of the associated object.
     */
    public ObjectAccessParams(Application app, AccessMode mode, Object value, int hashCode) {
        super(app, mode);
        this.value = value;
        this.hashCode = hashCode;
    }

    /**
     * Returns the associated object.
     * 
     * @return The associated object.
     */
    public Object getValue() {
        return value;
    }

    /**
     * Returns the hashcode of the associated object.
     * 
     * @return The hashcode of the associated object.
     */
    public int getCode() {
        return hashCode;
    }

    @Override
    public DataAccessId registerAccess(DataInfoProvider dip) {
        return dip.registerObjectAccess(this.app, this.mode, this.value, this.hashCode);
    }

    @Override
    public void registerAccessCompletion(DataInfoProvider dip) {
        dip.finishObjectAccess(this.mode, this.hashCode);
    }

    @Override
    public String toString() {
        return "[" + this.app + ", " + this.mode + " ," + this.hashCode + "]";
    }
}
