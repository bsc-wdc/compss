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
import java.util.Iterator;
import java.util.Map;


/**
 * Extension of the LocalParameter class to handle dict_collection types. Basically, a LocalParameter plus a Map of
 * LocalParameters representing the contents of the dictionary collection.
 * 
 * @see LocalParameter
 */

public class LocalParameterDictCollection extends LocalParameterCollection
    implements InvocationParamDictCollection<LocalParameter> {

    /**
     * Create a new LocalParameterCollection copying the given LocalParameter values.
     * 
     * @param p LocalParameter to copy.
     */
    public LocalParameterDictCollection(Parameter p) {
        super(p);

    }

    @Override
    public int getSize() {
        return super.getSize() / 2;
    }

    @Override
    public Map<LocalParameter, LocalParameter> getDictionary() {
        Map<LocalParameter, LocalParameter> map = new HashMap<>();
        Iterator<LocalParameter> elements = super.getCollectionParameters().iterator();
        while (elements.hasNext()) {
            LocalParameter k = elements.next();
            LocalParameter v = elements.next();
            map.put(k, v);
        }
        return map;
    }

    @Override
    public void addEntry(LocalParameter k, LocalParameter v) {
        super.addElement(k);
        super.addElement(v);
    }

}
