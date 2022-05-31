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

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import java.util.Map;


/**
 * The internal Dictionary (Collection) representation. A Dictionary Collection is a COMPSs Parameter objects which may
 * contain other COMPSs parameter objects. The object has an identifier by itself and points to other object identifiers
 * (which are the ones contained in it)
 */
public class DictCollectionParameter extends DependencyParameter {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    // Identifier of the collection object
    private String dictCollectionId;
    // Parameter objects of the collection contents
    private Map<Parameter, Parameter> parameters;


    /**
     * Default constructor. Intended to be called from COMPSsRuntimeImpl when gathering and compacting parameter
     * information fed from bindings or Java Loader
     * 
     * @param dictCollectionId Name of the File identifier of the collection object per se.
     * @param parameters Parameters of the CollectionParameter
     * @param direction Direction of the collection
     * @param stream N/A (At least temporarily)
     * @param prefix N/A (At least temporarily)
     * @param name Name of the parameter in the user code
     * @see DependencyParameter
     * @see es.bsc.compss.api.impl.COMPSsRuntimeImpl
     * @see es.bsc.compss.components.impl.TaskAnalyser
     */
    public DictCollectionParameter(String dictCollectionId, Map<Parameter, Parameter> parameters, Direction direction,
        StdIOStream stream, String prefix, String name, String contentType, double weight, boolean keepRename) {

        // Type will always be DICT_COLLECTION_T, no need to pass it as a constructor parameter and wont be modified
        // Stream and prefix are still forwarded for possible, future uses
        super(DataType.DICT_COLLECTION_T, direction, stream, prefix, name, contentType, weight, keepRename);
        this.parameters = parameters;
        this.dictCollectionId = dictCollectionId;
    }

    /**
     * Get the identifier of the collection.
     * 
     * @return The collection identifier.
     */
    public String getDictCollectionId() {
        return this.dictCollectionId;
    }

    /**
     * Set the identifier of the collection.
     * 
     * @param dictCollectionId The collection Id.
     */
    public void setDictCollectionId(String dictCollectionId) {
        this.dictCollectionId = dictCollectionId;
    }

    @Override
    public String toString() {
        // String builder adds less overhead when creating a string
        StringBuilder sb = new StringBuilder();
        sb.append("DictCollectionParameter ").append(this.dictCollectionId).append("\n");
        sb.append("Name: ").append(getName()).append("\n");
        sb.append("Contents:\n");
        for (Map.Entry<Parameter, Parameter> entry : parameters.entrySet()) {
            sb.append("\t").append(entry.getKey()).append(" - ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    /**
     * Returns the collection parameters.
     * 
     * @return List of the internal parameters of the collection.
     */
    public Map<Parameter, Parameter> getParameters() {
        return this.parameters;
    }

    /**
     * Sets the internal parameters of the collection.
     * 
     * @param parameters New internal parameters of the collection.
     */
    public void setParameters(Map<Parameter, Parameter> parameters) {
        this.parameters = parameters;
    }
}
