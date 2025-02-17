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
import es.bsc.compss.types.Task;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;


public abstract class Parameter implements es.bsc.compss.types.parameter.Parameter {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    // Parameter fields
    private DataType type;
    private final Direction direction;
    private final StdIOStream stream;
    private final String prefix;
    private final String name;
    private final String contentType;
    private final double weight;
    private final boolean keepRename;
    private final ParameterMonitor monitor;


    /**
     * Creates a new Parameter instance from the given values.
     *
     * @param type Parameter type.
     * @param direction Parameter direction.
     * @param stream Parameter IO stream mode.
     * @param prefix Parameter prefix.
     * @param name Parameter name.
     * @param contentType Python object type.
     * @param weight Parameter weight for taking scheduling decisions
     * @param keepRename if {@literal true}, parameter remains renamed within the task's execution sandbox; otherwise,
     *            the value recovers its original name
     * @param monitor object to notify to changes on the parameter
     */
    protected Parameter(DataType type, Direction direction, StdIOStream stream, String prefix, String name,
        String contentType, double weight, boolean keepRename, ParameterMonitor monitor) {
        this.type = type;
        this.direction = direction;
        this.stream = stream;
        if (prefix == null || prefix.isEmpty()) {
            this.prefix = Constants.PREFIX_EMPTY;
        } else {
            this.prefix = prefix;
        }
        this.name = name;
        this.contentType = contentType;
        this.weight = weight;
        this.keepRename = keepRename;
        this.monitor = monitor;
    }

    @Override
    public abstract boolean isPotentialDependency();

    @Override
    public abstract boolean isCollective();

    @Override
    public DataType getType() {
        return this.type;
    }

    @Override
    public void setType(DataType type) {
        this.type = type;
    }

    @Override
    public Direction getDirection() {
        return this.direction;
    }

    @Override
    public StdIOStream getStream() {
        return this.stream;
    }

    @Override
    public String getPrefix() {
        return this.prefix;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getContentType() {
        return this.contentType;
    }

    @Override
    public double getWeight() {
        return weight;
    }

    @Override
    public boolean isKeepRename() {
        return keepRename;
    }

    @Override
    public ParameterMonitor getMonitor() {
        return monitor;
    }

    /**
     * Returns a description of the access performed by the parameter.
     *
     * @return description of the access performed by the parameter.
     */
    public abstract AccessParams getAccess();

    /**
     * Registers the data access by the parameter and the potential data dependencies to the task given as parameter.
     *
     * @param task Task with potential dependencies
     * @param isConstraining {@literal true}, if the parameter constrains the scheduling of the task; {@literal false},
     *            otherwise
     * @return {@literal true}, if the task has requested an edge in the graph; {@literal false}, otherwise.
     */
    public abstract boolean register(Task task, boolean isConstraining);

    /**
     * Registers the failed parameter processing of the as part of a given task.
     * 
     * @param task task that executed the parameter
     */
    public abstract void cancel(Task task);

    /**
     * Registers the end of the parameter processing as part of a given task.
     * 
     * @param task task that executed the parameter
     */
    public abstract void commit(Task task);

    /**
     * Marks the data generated by the parameter to be removed as soon as possible.
     *
     * @return
     */
    public abstract DataInfo removeData() throws ValueUnawareRuntimeException;
}
