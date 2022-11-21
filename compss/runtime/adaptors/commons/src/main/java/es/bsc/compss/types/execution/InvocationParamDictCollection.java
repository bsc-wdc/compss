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
package es.bsc.compss.types.execution;

import java.util.Map;


public interface InvocationParamDictCollection<T extends InvocationParam> extends InvocationParam {

    /**
     * Returns a list of objects containing the dictionary collection parameters.
     *
     * @return A list of objects containing the dictionary collection parameters.
     */
    public Map<T, T> getDictionary();

    /**
     * Adds a new parameter to the dictionary collection.
     *
     * @param k Key parameter to add.
     * @param v Value parameter to add.
     */
    public void addEntry(T k, T v);

    /**
     * Returns the number of internal parameters of the dictionary collection.
     *
     * @return The number of internal parameters of the dictionary collection.
     */
    public int getSize();

}
