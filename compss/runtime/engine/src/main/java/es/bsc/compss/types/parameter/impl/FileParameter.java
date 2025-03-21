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
import es.bsc.compss.types.data.accessparams.FileAccessParams;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.params.FileData;


public class FileParameter<D extends FileData, A extends FileAccessParams<D>> extends DependencyParameter<A> {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    private final String originalName;


    /**
     * Creates a new File Parameter.
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
     * @return creates a new File Parameter
     */
    public static final FileParameter newFP(Application app, Direction direction, StdIOStream stream, String prefix,
        String name, String contentType, double weight, boolean keepRename, DataLocation location, String originalName,
        ParameterMonitor monitor) {
        FileAccessParams fap = FileAccessParams.constructFAP(app, direction, location);
        return new FileParameter(fap, DataType.FILE_T, direction, stream, prefix, name, contentType, weight, keepRename,
            originalName, monitor);
    }

    protected FileParameter(A fap, DataType type, Direction direction, StdIOStream stream, String prefix, String name,
        String contentType, double weight, boolean keepRename, String originalName, ParameterMonitor monitor) {

        super(fap, type, direction, stream, prefix, name, contentType, weight, keepRename, monitor);
        this.originalName = originalName;
    }

    @Override
    public boolean isCollective() {
        return false;
    }

    public final DataLocation getLocation() {
        return this.getAccess().getLocation();
    }

    @Override
    public final String getOriginalName() {
        return this.originalName;
    }

    @Override
    public String toString() {
        return "FileParameter with location " + this.getAccess().getLocation() + ", type " + getType() + ", direction "
            + getDirection() + ", CONTENT TYPE" + getContentType();
    }

}
