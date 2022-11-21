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
package es.bsc.compss.types.parameter;

import es.bsc.compss.api.ParameterMonitor;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;

import java.util.List;


/**
 * The internal Collection representation. A Collection is a COMPSs Parameter objects which may contain other COMPSs
 * parameter objects. The object has an identifier by itself and points to other object identifiers (which are the ones
 * contained in it)
 */
public class CollectionParameter extends CollectiveParameter {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Default constructor. Intended to be called from COMPSsRuntimeImpl when gathering and compacting parameter
     * information fed from bindings or Java Loader
     * 
     * @param id identifier of the collection object.
     * @param direction Direction of the collection
     * @param stream N/A (At least temporarily)
     * @param prefix N/A (At least temporarily)
     * @param name Name of the parameter in the user code
     * @param weight Parameter weight.
     * @param keepRename Parameter keep rename.
     * @param monitor object to notify to changes on the parameter
     * @see DependencyParameter
     * @see es.bsc.compss.api.impl.COMPSsRuntimeImpl
     * @see es.bsc.compss.components.impl.TaskAnalyser
     */
    public CollectionParameter(String id, Direction direction, StdIOStream stream, String prefix, String name,
        String contentType, double weight, boolean keepRename, ParameterMonitor monitor, List<Parameter> parameters) {

        // Type will always be COLLECTION_T, no need to pass it as a constructor parameter and wont be modified
        // Stream and prefix are still forwarded for possible, future uses
        super(DataType.COLLECTION_T, id, direction, stream, prefix, name, contentType, weight, keepRename, monitor,
            parameters);
    }

    @Override
    public String toString() {
        // Stringbuilder adds less overhead when creating a string
        StringBuilder sb = new StringBuilder();
        sb.append("CollectionParameter ").append(this.getCollectionId()).append("\n");
        sb.append("Name: ").append(getName()).append("\n");
        sb.append("Contents:\n");
        for (Parameter s : this.getElements()) {
            sb.append("\t").append(s).append("\n");
        }
        return sb.toString();
    }

}
