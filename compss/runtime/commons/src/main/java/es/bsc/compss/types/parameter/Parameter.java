/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Stream;

import java.io.Serializable;


public abstract class Parameter implements Serializable {

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;

    // Parameter fields
    private DataType type;
    private final Direction direction;
    private final Stream stream;
    private final String prefix;
    private final String name;

	public Parameter(DataType type, Direction direction, Stream stream, String prefix, String name) {
        this.type = type;
        this.direction = direction;
        this.stream = stream;
        if (prefix == null || prefix.isEmpty()) {
            this.prefix = Constants.PREFIX_EMTPY;
        } else { 
            this.prefix = prefix;
        }
        this.name = name;
    } 

    public DataType getType() {
        return this.type;
    }

    public void setType(DataType type) {
        this.type = type;
    }

    public Direction getDirection() {
        return this.direction;
    }

    public Stream getStream() {
        return this.stream;
    }
    
    public String getPrefix() {
        return this.prefix;
    }

    public String getName() { return this.name; }

}
