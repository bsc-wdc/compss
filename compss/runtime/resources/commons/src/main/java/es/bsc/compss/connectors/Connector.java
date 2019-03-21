/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.connectors;

import es.bsc.compss.types.ResourceCreationRequest;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;


public interface Connector {

    /**
     * Starts a resource
     * 
     * @param name
     * @param rR
     * @return
     */
    public boolean turnON(String name, ResourceCreationRequest rR);

    /**
     * Sets the stop flag
     */
    public void stopReached();

    /**
     * Returns the expected creation time for next request
     * 
     * @return
     * @throws ConnectorException
     */
    public Long getNextCreationTime() throws ConnectorException;

    /**
     * Returns the time slot size
     * 
     * @return
     */
    public long getTimeSlot();

    /**
     * Terminates an specific machine
     * 
     * @param worker
     * @param reduction
     */
    public void terminate(CloudMethodWorker worker, CloudMethodResourceDescription reduction);

    /**
     * Terminates all instances
     */
    public void terminateAll();

}
