/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.parameter.impl;

import es.bsc.compss.api.ParameterMonitor;
import es.bsc.compss.semantics.data.DataType;
import es.bsc.compss.semantics.data.access.AccessMode;
import es.bsc.compss.semantics.task.parameter.StdIOStream;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.data.accessparams.ObjectAccessParams;
import es.bsc.compss.types.data.params.ObjectData;
import storage.StubItf;

public class ObjectParameter<V extends Object, A extends ObjectAccessParams<V, D>, D extends ObjectData>
    extends DependencyParameter<A> {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Creates a new Object Parameter.
     *
     * @param app Application performing the access
     * @param accessMode Parameter direction.
     * @param stream Standard IO Stream flags.
     * @param prefix Parameter prefix.
     * @param name Parameter name.
     * @param contentType Parameter content type.
     * @param weight Parameter weight.
     * @param value Parameter object value.
     * @param hashCode Parameter object hashcode.
     * @param monitor object to notify to changes on the parameter
     * @return new ObjectParameter instance
     */
    public static <V extends Object> ObjectParameter<V, ObjectAccessParams<V, ObjectData>, ObjectData> newOP(
        Application app, AccessMode accessMode, StdIOStream stream, String prefix, String name, String contentType,
        double weight, V value, int hashCode, ParameterMonitor monitor) {
        ObjectAccessParams<V, ObjectData> oap;
        oap = ObjectAccessParams.constructObjectAP(app, accessMode, value, hashCode);

        return new ObjectParameter(oap, DataType.OBJECT_T, accessMode, stream, prefix, name, contentType, weight,
            monitor);
    }

    protected ObjectParameter(A objectAP, DataType type, AccessMode accessMode, StdIOStream stream, String prefix,
        String name, String contentType, double weight, ParameterMonitor monitor) {
        super(objectAP, type, accessMode, stream, prefix, name, contentType, weight, false, monitor);

    }

    @Override
    public final boolean isCollective() {
        return false;
    }

    public final V getValue() {
        return this.getAccess().getValue();
    }

    public final int getCode() {
        return this.getAccess().getCode();
    }

    @Override
    public String toString() {
        return "ObjectParameter with hash code " + this.getCode() + ", type " + getType() + ", direction "
            + getAccessMode();
    }

    @Override
    public boolean register(Task task, boolean isConstraining) {
        if (this.getType() == DataType.OBJECT_T) {
            // Check if its PSCO class and persisted to infer its type
            if (this.getValue() instanceof StubItf && ((StubItf) this.getValue()).getID() != null) {
                this.setType(DataType.PSCO_T);
            }
        }
        return super.register(task, isConstraining);
    }
}
