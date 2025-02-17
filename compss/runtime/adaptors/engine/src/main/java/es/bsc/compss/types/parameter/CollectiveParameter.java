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

import java.util.List;


/**
 * The internal Collection representation. A Collection is a COMPSs Parameter objects which may contain other COMPSs
 * parameter objects. The object has an identifier by itself and points to other object identifiers (which are the ones
 * contained in it)
 */
public interface CollectiveParameter<T extends Parameter> extends DependencyParameter {

    /**
     * Get the identifier of the collection.
     *
     * @return The collection identifier.
     */
    public String getCollectionId();

    /**
     * Returns the collection parameters.
     *
     * @return List of the internal parameters of the collection.
     */
    public List<T> getElements();

    /**
     * Sets the internal parameters of the collection.
     *
     * @param elements New internal parameters of the collection.
     */
    public void setElements(List<T> elements);

}
