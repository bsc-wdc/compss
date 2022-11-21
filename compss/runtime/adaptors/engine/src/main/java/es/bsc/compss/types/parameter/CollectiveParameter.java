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
import java.util.Iterator;

import java.util.List;


/**
 * The internal Collection representation. A Collection is a COMPSs Parameter objects which may contain other COMPSs
 * parameter objects. The object has an identifier by itself and points to other object identifiers (which are the ones
 * contained in it)
 */
public class CollectiveParameter extends DependencyParameter {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    // Identifier of the collection object
    private String collectionId;

    // Parameter objects of the collection contents
    private List<Parameter> elements;


    /**
     * Default constructor. Intended to be called from COMPSsRuntimeImpl when gathering and compacting parameter
     * information fed from bindings or Java Loader
     * 
     * @param type type of collection
     * @param id identifier of the collection
     * @param direction Direction of the collection
     * @param stream N/A (At least temporarily)
     * @param prefix N/A (At least temporarily)
     * @param name Name of the parameter in the user code
     * @param weight Parameter weight.
     * @param keepRename Parameter keep rename.
     * @param monitor object to notify to changes on the parameter
     * @param elements Elements of the collection
     * @see DependencyParameter
     */
    public CollectiveParameter(DataType type, String id, Direction direction, StdIOStream stream, String prefix,
        String name, String contentType, double weight, boolean keepRename, ParameterMonitor monitor,
        List<Parameter> elements) {
        super(type, direction, stream, prefix, name, contentType, weight, keepRename, monitor);
        this.collectionId = id;
        this.elements = elements;
    }

    @Override
    public boolean isCollective() {
        return true;
    }

    /**
     * Get the identifier of the collection.
     * 
     * @return The collection identifier.
     */
    public String getCollectionId() {
        return this.collectionId;
    }

    /**
     * Set the identifier of the collection.
     * 
     * @param collectionId The collection Id.
     */
    public void setCollectionId(String collectionId) {
        this.collectionId = collectionId;
    }

    /**
     * Returns the collection parameters.
     * 
     * @return List of the internal parameters of the collection.
     */
    public List<Parameter> getElements() {
        return this.elements;
    }

    /**
     * Sets the internal parameters of the collection.
     * 
     * @param elements New internal parameters of the collection.
     */
    public void setElements(List<Parameter> elements) {
        this.elements = elements;
    }

    @Override
    public String toString() {
        if (this.getType() == DataType.COLLECTION_T) {
            return this.toCollectionString();
        } else { // this.getType() == DataType.DICT_COLLECTION_T
            return this.toDictionaryString();
        }
    }

    private String toDictionaryString() {
        // String builder adds less overhead when creating a string
        StringBuilder sb = new StringBuilder();
        sb.append("DictCollectionParameter ").append(this.getCollectionId()).append("\n");
        sb.append("Name: ").append(getName()).append("\n");
        sb.append("Contents:\n");
        Iterator<Parameter> it = this.getElements().iterator();
        while (it.hasNext()) {
            Parameter key = it.next();
            Parameter value = it.next();
            sb.append("\t").append(key).append(" - ").append(value).append("\n");
        }
        return sb.toString();
    }

    private String toCollectionString() {
        // String builder adds less overhead when creating a string
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
