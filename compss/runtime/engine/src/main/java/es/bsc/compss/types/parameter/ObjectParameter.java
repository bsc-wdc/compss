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
package es.bsc.compss.types.parameter;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;


public class ObjectParameter extends DependencyParameter {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    private final int hashCode;
    private Object value;


    /**
     * Creates a new Object Parameter.
     * 
     * @param direction Parameter direction.
     * @param stream Standard IO Stream flags.
     * @param prefix Parameter prefix.
     * @param name Parameter name.
     * @param contentType Parameter content type.
     * @param weight Parameter weight.
     * @param value Parameter object value.
     * @param hashCode Parameter object hashcode.
     */
    public ObjectParameter(Direction direction, StdIOStream stream, String prefix, String name, String contentType,
        double weight, Object value, int hashCode) {

        super(DataType.OBJECT_T, direction, stream, prefix, name, contentType, weight, false);
        this.value = value;
        this.hashCode = hashCode;
    }

    public Object getValue() {
        return this.value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public int getCode() {
        return this.hashCode;
    }

    @Override
    public String toString() {
        return "ObjectParameter with hash code " + this.hashCode + ", type " + getType() + ", direction "
            + getDirection();
    }

}
