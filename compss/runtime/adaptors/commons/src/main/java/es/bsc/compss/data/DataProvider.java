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

package es.bsc.compss.data;

import es.bsc.compss.types.execution.InvocationParam;


public interface DataProvider {

    /**
     * Returns whether the persistent C backend is enabled or not.
     * 
     * @return {@code true} if the persistent C backend is enabled, {@code false} otherwise.
     */
    public boolean isPersistentCEnabled();

    /**
     * Requests a new transfer with the given information.
     * 
     * @param param Invocation Parameter to request.
     * @param index Parameter index.
     * @param tt Transfer listener.
     */
    public void askForTransfer(InvocationParam param, int index, FetchDataListener tt);

    /**
     * Check if data in the parameter is being transferred.
     * 
     * @param param Invocation Parameter to check
     * @return {@code true} if data is being transferred, {@code false} otherwise.
     */
    public boolean isTransferingData(InvocationParam param);

}
