/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.types.resources.description.CloudImageDescription;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;


public interface Operations {

    public static final Object knownHosts = new Object();


    /**
     * Power on a machine
     * 
     * @param name
     * @param rd
     * @return
     * @throws ConnectorException
     */
    public Object poweron(String name, CloudMethodResourceDescription rd) throws ConnectorException;

    /**
     * Destroy a machine
     * 
     * @param envId
     * @throws ConnectorException
     */
    public void destroy(Object envId) throws ConnectorException;

    /**
     * Wait for creation completion
     * 
     * @param envId
     * @param request
     * @return
     * @throws ConnectorException
     */
    public VM waitCreation(Object envId, CloudMethodResourceDescription request) throws ConnectorException;

    /**
     * Allow access from master and between VM
     * 
     * @param IP
     * @param user
     * @param password
     * @throws ConnectorException
     */
    public void configureAccess(String IP, String user, String password) throws ConnectorException;

    /**
     * Prepare Machine to run tasks
     * 
     * @param IP
     * @param cid
     * @throws ConnectorException
     */
    public void prepareMachine(String IP, CloudImageDescription cid) throws ConnectorException;

    /**
     * Notification that the vm is available and fully operative
     * 
     * @param vm
     * @throws ConnectorException
     */
    public void vmReady(VM vm) throws ConnectorException;

    /**
     * Shutdown an existing machine
     * 
     * @param rd
     * @throws ConnectorException
     */
    public void poweroff(VM rd) throws ConnectorException;

    /**
     * Pause an existing machine
     * 
     * @param worker
     * @return
     */
    public VM pause(CloudMethodWorker worker);

    /**
     * Data needed to check if VM is useful
     * 
     * @return
     */
    public boolean getTerminate();

    /**
     * Data needed to check if VM is useful
     * 
     * @return
     */
    public boolean getCheck();

}
