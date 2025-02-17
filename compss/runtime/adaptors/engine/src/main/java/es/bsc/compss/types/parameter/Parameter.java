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
package es.bsc.compss.types.parameter;

import es.bsc.compss.api.ParameterMonitor;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;

import java.io.Serializable;


public interface Parameter extends Serializable {

    /**
     * Returns whether the parameter is a DependencyParameter or a basic type parameter.
     *
     * @return {@literal true}, if the parameter represents a data that may incur a dependency; {@literal false} if it
     *         is a basic type
     */
    public boolean isPotentialDependency();

    /**
     * Returns whether the parameter is a Collective or Single value.
     *
     * @return {@literal true}, if the parameter represents a collection of values; {@literal false} if it a single
     *         value.
     */
    public boolean isCollective();

    /**
     * Returns the parameter type.
     *
     * @return The parameter type.
     */
    public DataType getType();

    /**
     * Sets a new parameter type.
     *
     * @param type New parameter type.
     */
    public void setType(DataType type);

    /**
     * Returns the parameter direction.
     *
     * @return The parameter direction.
     */
    public Direction getDirection();

    /**
     * Returns the parameter IO stream mode.
     *
     * @return The parameter IO stream mode.
     */
    public StdIOStream getStream();

    /**
     * Returns the parameter prefix.
     *
     * @return The parameter prefix.
     */
    public String getPrefix();

    /**
     * Returns the parameter name.
     *
     * @return The parameter name.
     */
    public String getName();

    /**
     * Returns the python object type.
     *
     * @return The content type.
     */
    public String getContentType();

    /**
     * Returns the parameter weight.
     * 
     * @return The parameter weight.
     */
    public double getWeight();

    /**
     * Check is parameter can keep the renamed name.
     * 
     * @return True if parameter can keep the renamed name. Otherwise, false.
     */
    public boolean isKeepRename();

    /**
     * Returns the object monitoring changes on the parameter.
     * 
     * @return The object monitoring the parameter.
     */
    public ParameterMonitor getMonitor();

}
