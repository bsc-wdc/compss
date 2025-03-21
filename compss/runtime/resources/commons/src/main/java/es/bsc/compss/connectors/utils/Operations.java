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
package es.bsc.compss.connectors.utils;

import es.bsc.compss.connectors.ConnectorException;
import es.bsc.compss.connectors.VM;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.description.CloudImageDescription;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;


public interface Operations {

    public static final Object KNOWN_HOSTS = new Object();


    /**
     * Power on a machine.
     * 
     * @param name Machine name.
     * @param rd Associated resource description.
     * @return The VM description.
     * @throws ConnectorException When the connector raises an exception.
     */
    public Object poweron(String name, CloudMethodResourceDescription rd) throws ConnectorException;

    /**
     * Destroy a machine.
     * 
     * @param envId Machine Id.
     * @throws ConnectorException When the connector raises an exception.
     */
    public void destroy(Object envId) throws ConnectorException;

    /**
     * Wait for creation completion.
     * 
     * @param envId Machine Id.
     * @param request Request description.
     * @return The VM description of the granted machine by the connector.
     * @throws ConnectorException When the connector raises an exception.
     */
    public VM waitCreation(Object envId, CloudMethodResourceDescription request) throws ConnectorException;

    /**
     * Allow access from master and between VM.
     * 
     * @param ip Machine IP.
     * @param user Machine login user.
     * @param password Machine login password.
     * @throws ConnectorException When the connector raises an exception.
     */
    public void configureAccess(String ip, String user, String password) throws ConnectorException;

    /**
     * Prepare Machine to run tasks.
     * 
     * @param ip Machine IP.
     * @param cid Machine description.
     * @throws ConnectorException When the connector raises an exception.
     */
    public void prepareMachine(String ip, CloudImageDescription cid) throws ConnectorException;

    /**
     * Notification that the VM is available and fully operative.
     * 
     * @param vm VM to be ready.
     * @throws ConnectorException When the connector raises an exception.
     */
    public void vmReady(VM vm) throws ConnectorException;

    /**
     * Shutdown an existing machine.
     * 
     * @param rd Machine VM description.
     * @throws ConnectorException When the connector raises an exception.
     */
    public void poweroff(VM rd) throws ConnectorException;

    /**
     * Pause an existing machine.
     * 
     * @param worker Associated worker.
     * @return VM description of the paused machine.
     */
    public VM pause(CloudMethodWorker worker);

    /**
     * Returns whether the operation can be terminated or not.
     * 
     * @return {@literal true} if the operation can be terminated, {@literal false} otherwise.
     */
    public boolean getTerminate();

    /**
     * Data needed to check if VM is useful.
     * 
     * @return Data needed to check if VM is useful.
     */
    public boolean getCheck();

}
