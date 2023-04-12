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
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;

import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;


public abstract class DependencyParameter<T extends AccessParams> extends Parameter
    implements es.bsc.compss.types.parameter.DependencyParameter {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    private final T access;

    private DataAccessId daId;
    private Object dataSource;
    private String dataTarget; // URI (including PROTOCOL) where to find the data within the executing resource


    /**
     * Creates a new DependencyParameter instance from the given parameters.
     *
     * @param type Parameter type.
     * @param direction Parameter direction.
     * @param access description of the access performed on the data
     * @param stream Parameter IO stream mode.
     * @param prefix Parameter prefix.
     * @param name Parameter name.
     * @param weight Parameter weight.
     * @param keepRename Parameter keep rename property.
     * @param monitor object to notify to changes on the parameter
     */
    protected DependencyParameter(T access, DataType type, Direction direction, StdIOStream stream, String prefix,
        String name, String contentType, double weight, boolean keepRename, ParameterMonitor monitor) {
        super(type, direction, stream, prefix, name, contentType, weight, keepRename, monitor);
        this.access = access;
    }

    @Override
    public boolean isPotentialDependency() {
        return true;
    }

    @Override
    public final T getAccess() {
        return access;
    }

    @Override
    public DataAccessId getDataAccessId() {
        return this.daId;
    }

    @Override
    public void setDataAccessId(DataAccessId daId) {
        this.daId = daId;
    }

    @Override
    public String getOriginalName() {
        return NO_NAME;
    }

    @Override
    public Object getDataSource() {
        return this.dataSource;
    }

    @Override
    public void setDataSource(Object dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public String getDataTarget() {
        return this.dataTarget;
    }

    @Override
    public void setDataTarget(String target) {
        this.dataTarget = target;
    }

    @Override
    public String generateDataTargetName(String tgtName) {
        if (getType().equals(DataType.PSCO_T) || getType().equals(DataType.EXTERNAL_PSCO_T)) {
            return getDataTarget();
        } else {
            return tgtName;
        }
    }

    @Override
    public String toString() {
        return "DependencyParameter";
    }

    @Override
    public boolean isSourcePreserved() {
        return this.daId.isPreserveSourceData();
    }

    @Override
    public boolean isTargetFlexible() {
        return true;
    }
}
