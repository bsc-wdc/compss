package integratedtoolkit.connectors.utils;

import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.connectors.VM;
import integratedtoolkit.types.CloudImageDescription;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;


public interface Operations {

    public static final Object knownHosts = new Object();

    //Power on a new Machine
    public Object poweron(String name, CloudMethodResourceDescription rd)
            throws ConnectorException;

    public void destroy(Object envId) throws ConnectorException;

    public VM waitCreation(Object envId, CloudMethodResourceDescription request) throws ConnectorException;

    //Allow access from master and between VM
    public void configureAccess(String IP, String user, String password) throws ConnectorException;

    //Prepare Machine to run tasks
    public void prepareMachine(String IP, CloudImageDescription cid) throws ConnectorException;

    //Notification that the vm is available and fully operative
    public void vmReady(VM vm) throws ConnectorException;

    //Shutdown an existing machine
    public void poweroff(VM rd) throws ConnectorException;

    public VM pause(CloudMethodWorker worker);

    //Data needed to check if vm are useful
    public boolean getTerminate();

    public boolean getCheck();

}
