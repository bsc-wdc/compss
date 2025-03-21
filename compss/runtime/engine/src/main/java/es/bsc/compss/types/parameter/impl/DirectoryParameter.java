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
import es.bsc.compss.types.data.accessparams.DirectoryAccessParams;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.params.DirectoryData;


public class DirectoryParameter extends FileParameter<DirectoryData, DirectoryAccessParams> {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Creates a new Directory Parameter.
     *
     * @param app Application performing the access
     * @param direction Parameter direction.
     * @param stream Standard IO Stream flags.
     * @param prefix Parameter prefix.
     * @param name Parameter name.
     * @param contentType Parameter content type.
     * @param weight Parameter weight.
     * @param keepRename Parameter keep rename property.
     * @param location Directory location.
     * @param originalName Original dir name.
     * @param monitor object to notify to changes on the parameter
     * @return new Directory Parameter instance
     */
    public static final DirectoryParameter newDP(Application app, Direction direction, StdIOStream stream,
        String prefix, String name, String contentType, double weight, boolean keepRename, DataLocation location,
        String originalName, ParameterMonitor monitor) {
        DirectoryAccessParams dap = DirectoryAccessParams.constructDAP(app, direction, location);
        return new DirectoryParameter(dap, direction, stream, prefix, name, contentType, weight, keepRename,
            originalName, monitor);
    }

    protected DirectoryParameter(DirectoryAccessParams dap, Direction direction, StdIOStream stream, String prefix,
        String name, String contentType, double weight, boolean keepRename, String originalName,
        ParameterMonitor monitor) {

        super(dap, DataType.DIRECTORY_T, direction, stream, prefix, name, contentType, weight, keepRename, originalName,
            monitor);

    }

    @Override
    public String toString() {
        return "DirectoryParameter with location " + this.getLocation() + ", type " + getType() + ", direction "
            + getDirection();
    }

}
