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
package es.bsc.compss.local;

import es.bsc.compss.types.execution.InvocationParamCollection;
import es.bsc.compss.types.parameter.Parameter;

import java.util.LinkedList;
import java.util.List;


/**
 * Extension of the LocalParameter class to handle collection types. Basically, a LocalParameter plus a list of
 * LocalParameters representing the contents of the collection.
 * 
 * @see LocalParameter
 */

public class LocalParameterCollection extends LocalParameter implements InvocationParamCollection<LocalParameter> {

    private final List<LocalParameter> collectionParameters;


    /**
     * Create a new LocalParameterCollection copying the given LocalParameter values.
     * 
     * @param p LocalParameter to copy.
     */
    public LocalParameterCollection(Parameter p) {
        super(p);

        // Empty attributes
        this.collectionParameters = new LinkedList<>();
    }

    @Override
    public boolean isCollective() {
        return true;
    }

    @Override
    public int getSize() {
        return this.collectionParameters.size();
    }

    @Override
    public List<LocalParameter> getCollectionParameters() {
        return this.collectionParameters;
    }

    @Override
    public void addElement(LocalParameter p) {
        this.collectionParameters.add(p);
    }

}
