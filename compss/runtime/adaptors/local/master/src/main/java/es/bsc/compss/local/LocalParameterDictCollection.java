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
package es.bsc.compss.local;

import es.bsc.compss.types.execution.InvocationParamDictCollection;
import es.bsc.compss.types.parameter.Parameter;

import java.util.HashMap;
import java.util.Map;


/**
 * Extension of the LocalParameter class to handle dict_collection types. Basically, a LocalParameter plus a Map of
 * LocalParameters representing the contents of the dictionary collection.
 * 
 * @see LocalParameter
 */

public class LocalParameterDictCollection extends LocalParameter
    implements InvocationParamDictCollection<LocalParameter, LocalParameter> {

    private Map<LocalParameter, LocalParameter> dictCollectionParameters;


    /**
     * Create a new LocalParameterCollection instance for externalization.
     */
    public LocalParameterDictCollection() {
        // Only executed by externalizable
        super();
    }

    /**
     * Create a new LocalParameterCollection copying the given LocalParameter values.
     * 
     * @param p LocalParameter to copy.
     */
    public LocalParameterDictCollection(Parameter p) {
        super(p);

        // Empty attributes
        this.dictCollectionParameters = new HashMap<LocalParameter, LocalParameter>();
    }

    /**
     * Returns the number of internal parameters of the dictionary collection.
     * 
     * @return The number of internal parameters of the dictionary collection.
     */
    public int getSize() {
        return this.dictCollectionParameters.size();
    }

    /**
     * Returns a Map of objects containing the dictionary collection parameters.
     * 
     * @return A map of objects containing the dictionary collection parameters.
     */
    public Map<LocalParameter, LocalParameter> getDictCollectionParameters() {
        return this.dictCollectionParameters;
    }

    /**
     * Adds a new parameter to the dictionary collection.
     * 
     * @param k Key to add.
     * @param v Value to add.
     */
    public void addParameter(LocalParameter k, LocalParameter v) {
        this.dictCollectionParameters.put(k, v);
    }

}
