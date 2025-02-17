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
import es.bsc.compss.types.data.accessparams.ExternalStreamAccessParams;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.params.ExternalStreamData;


public class ExternalStreamParameter
    extends StreamParameter<DataLocation, ExternalStreamAccessParams, ExternalStreamData> {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    private final String originalName;


    /**
     * Creates a new Stream Parameter.
     * 
     * @param app Application performing the access
     * @param direction Parameter direction.
     * @param stream Standard IO Stream flags.
     * @param prefix Parameter prefix.
     * @param name Parameter name.
     * @param location File location.
     * @param originalName Original file name.
     * @param monitor object to notify to changes on the parameter
     * @return new ExternalStreamParameter instance
     */
    public static final ExternalStreamParameter newESP(Application app, Direction direction, StdIOStream stream,
        String prefix, String name, DataLocation location, String originalName, ParameterMonitor monitor) {
        ExternalStreamAccessParams esap = ExternalStreamAccessParams.constructESAP(app, direction, location);
        return new ExternalStreamParameter(esap, direction, stream, prefix, name, originalName, monitor);
    }

    private ExternalStreamParameter(ExternalStreamAccessParams esap, Direction direction, StdIOStream stream,
        String prefix, String name, String originalName, ParameterMonitor monitor) {
        super(esap, DataType.EXTERNAL_STREAM_T, direction, stream, prefix, name, monitor);
        this.originalName = originalName;
    }

    public DataLocation getLocation() {
        return this.getAccess().getValue();
    }

    @Override
    public String getOriginalName() {
        return this.originalName;
    }

    @Override
    public String toString() {
        return "ExternalStreamParameter with location " + this.getLocation() + ", direction " + getDirection();
    }

}
