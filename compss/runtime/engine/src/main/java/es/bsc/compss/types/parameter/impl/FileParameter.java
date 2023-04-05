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
package es.bsc.compss.types.parameter.impl;

import es.bsc.compss.api.ParameterMonitor;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.data.accessparams.FileAccessParams;

import es.bsc.compss.types.data.location.DataLocation;


public class FileParameter extends DependencyParameter {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    // File parameter fields
    private final DataLocation location;
    private final String originalName;
    private final FileAccessParams access;


    /**
     * Creates a new Stream Parameter.
     * 
     * @param app Application performing the access
     * @param direction Parameter direction.
     * @param stream Standard IO Stream flags.
     * @param prefix Parameter prefix.
     * @param name Parameter name.
     * @param contentType Parameter content type.
     * @param weight Parameter weight.
     * @param keepRename Parameter keep rename property.
     * @param location File location.
     * @param originalName Original file name.
     * @param monitor object to notify to changes on the parameter
     */
    public FileParameter(Application app, Direction direction, StdIOStream stream, String prefix, String name,
        String contentType, double weight, boolean keepRename, DataLocation location, String originalName,
        ParameterMonitor monitor) {

        super(app, DataType.FILE_T, direction, stream, prefix, name, contentType, weight, keepRename, monitor);
        this.location = location;
        this.originalName = originalName;
        this.access = new FileAccessParams(app, getAccessMode(direction), location);
    }

    @Override
    public boolean isCollective() {
        return false;
    }

    public DataLocation getLocation() {
        return this.location;
    }

    @Override
    public String getOriginalName() {
        return this.originalName;
    }

    @Override
    public String toString() {
        return "FileParameter with location " + this.location + ", type " + getType() + ", direction " + getDirection()
            + ", CONTENT TYPE" + getContentType();
    }

    @Override
    public FileAccessParams getAccess() {
        return access;
    }

}
