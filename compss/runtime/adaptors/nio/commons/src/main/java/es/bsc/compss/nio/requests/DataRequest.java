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
package es.bsc.compss.nio.requests;

import es.bsc.compss.nio.NIOData;
import es.bsc.compss.types.annotations.parameter.DataType;


public abstract class DataRequest {

    private final DataType type;
    private final NIOData source;
    private final String target;


    /**
     * Creates a new DataRequest instance.
     * 
     * @param type Data type.
     * @param source Source data.
     * @param target Target data.
     */
    public DataRequest(DataType type, NIOData source, String target) {
        this.source = source;
        this.target = target;
        this.type = type;
    }

    /**
     * Returns the associated source.
     * 
     * @return The associated source.
     */
    public NIOData getSource() {
        return this.source;
    }

    /**
     * Returns the associated target.
     * 
     * @return The associated target.
     */
    public String getTarget() {
        return this.target;
    }

    /**
     * Returns the data type.
     * 
     * @return The data type.
     */
    public DataType getType() {
        return this.type;
    }

}
