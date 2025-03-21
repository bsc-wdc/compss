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
package es.bsc.compss.agent.types;

import java.util.List;


/**
 * Extension of the ApplicationResult class to handle collection types. Basically, an ApplicationResult plus a list of
 * ApplicationResults representing the contents of the collection.
 *
 * @see ApplicationResult
 */
public interface ApplicationResultCollection<T extends ApplicationResult> extends ApplicationResult {

    /**
     * Get elements within the result collection.
     * 
     * @return List with all the elements of the collection.
     */
    public List<T> getSubresults();

    /**
     * Adds a new result to the collection.
     * 
     * @param param result to be added.
     */
    public void addSubresult(T param);

    /**
     * Returns the number of results within the collection.
     * 
     * @return the number of results within the collection
     */
    public int getSize();

}
