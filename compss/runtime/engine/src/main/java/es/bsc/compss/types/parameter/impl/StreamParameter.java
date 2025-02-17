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
package es.bsc.compss.types.parameter.impl;

import es.bsc.compss.api.ParameterMonitor;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.data.accessparams.StreamAccessParams;
import es.bsc.compss.types.data.params.StreamData;


public class StreamParameter<V extends Object, A extends StreamAccessParams<V, D>, D extends StreamData>
    extends ObjectParameter<V, A, D> {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Creates a new Stream Parameter.
     * 
     * @param app Application performing the access
     * @param direction Parameter direction.
     * @param stream Standard IO Stream flags.
     * @param prefix Parameter prefix.
     * @param name Parameter name.
     * @param value Parameter object value.
     * @param hashCode Parameter object hashcode.
     * @param monitor object to notify to changes on the parameter
     * @return new StreamParam instance
     */
    public static <V extends Object> StreamParameter<V, StreamAccessParams<V, StreamData>, StreamData> newSP(
        Application app, Direction direction, StdIOStream stream, String prefix, String name, V value, int hashCode,
        ParameterMonitor monitor) {
        StreamAccessParams<V, StreamData> sap;
        sap = StreamAccessParams.constructStreamAP(app, direction, value, hashCode);
        return new StreamParameter(sap, DataType.STREAM_T, direction, stream, prefix, name, monitor);
    }

    protected StreamParameter(A streamAP, DataType type, Direction direction, StdIOStream stream, String prefix,
        String name, ParameterMonitor monitor) {
        super(streamAP, type, direction, stream, prefix, name, "null", 1.0, monitor);

    }

    @Override
    public String toString() {
        return "StreamParameter with hash code " + this.getCode() + ", type " + getType() + ", direction "
            + getDirection();
    }

}
