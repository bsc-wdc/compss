package integratedtoolkit.connectors.utils;

import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.connectors.VM;
import integratedtoolkit.types.resources.description.CloudImageDescription;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;


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
