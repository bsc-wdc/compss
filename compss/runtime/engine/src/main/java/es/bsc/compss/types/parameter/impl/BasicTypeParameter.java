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
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.info.DataInfo;


public class BasicTypeParameter extends Parameter implements es.bsc.compss.types.parameter.BasicTypeParameter {

    /*
     * Basic type parameter can be: - boolean - char - String - byte - short - int - long - float - double
     */

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    private Object value;


    /**
     * Creates a new BasicTypeParameter instance with the given information.
     *
     * @param type Parameter type.
     * @param direction Parameter direction.
     * @param stream Parameter IO stream mode.
     * @param prefix Parameter prefix.
     * @param name Parameter name.
     * @param value Parameter value.
     * @param weight Parameter weight.
     * @param monitor object to notify to changes on the parameter
     * @return new BasicTypeParameter instance
     */
    public static final BasicTypeParameter newBP(DataType type, Direction direction, StdIOStream stream, String prefix,
        String name, Object value, double weight, String contentType, ParameterMonitor monitor) {
        return new BasicTypeParameter(type, direction, stream, prefix, name, value, weight, contentType, monitor);
    }

    private BasicTypeParameter(DataType type, Direction direction, StdIOStream stream, String prefix, String name,
        Object value, double weight, String contentType, ParameterMonitor monitor) {
        super(type, direction, stream, prefix, name, contentType, weight, false, monitor);
        this.value = value;
    }

    @Override
    public boolean isPotentialDependency() {
        return false;
    }

    @Override
    public boolean isCollective() {
        return false;
    }

    @Override
    public Object getValue() {
        return this.value;
    }

    @Override
    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return this.value + " " + getType() + " " + getDirection();
    }

    @Override
    public AccessParams getAccess() {
        return null;
    }

    @Override
    public boolean register(Task task, boolean isConstraining) {
        task.registerFreeParam(this);
        return false;
    }

    @Override
    public void cancel(Task t) {
        // Nothing to do
    }

    @Override
    public void commit(Task t) {
        // Nothing to do
    }

    @Override
    public DataInfo removeData() {
        // Nothing to do
        return null;
    }

}
