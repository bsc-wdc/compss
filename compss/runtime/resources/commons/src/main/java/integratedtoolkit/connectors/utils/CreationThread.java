package integratedtoolkit.connectors.utils;

import integratedtoolkit.components.ResourceUser;
import integratedtoolkit.connectors.AbstractConnector;
import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.connectors.VM;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import integratedtoolkit.types.ResourceCreationRequest;
import integratedtoolkit.types.CloudImageDescription;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.types.resources.configuration.MethodConfiguration;
import integratedtoolkit.util.ResourceManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.concurrent.Semaphore;

/**
 * Helper thread for VM creation
 *
 */
public class CreationThread extends Thread {

    // Loggers
    private static final Logger resourceLogger = LogManager.getLogger(Loggers.CONNECTORS_UTILS);
    private static final Logger runtimeLogger = LogManager.getLogger(Loggers.RM_COMP);
    private static final boolean debug = resourceLogger.isDebugEnabled();

    // Error and warn messages
    private static final String ERROR_ASKING_NEW_RESOURCE = "Error asking a new Resource to ";
    private static final String ERROR_WAITING_VM = "Error waiting for a machine that should be provided by ";
    private static final String ERROR_POWEROFF_VM = "Cannot poweroff the machine\n]";
    private static final String ERROR_GRANTED_NULL = "Error: Granted description is null";
    private static final String ERROR_CONFIGURE_ACCESS_VM = "Error configuring access to machine ";
    private static final String ERROR_PREPARING_VM = "Exception preparing machine ";
    private static final String ERROR_START_WORKER = "Error starting the worker application in machine ";
    private static final String ERROR_START_VM = "Error: Could not turn on the VM";
    private static final String ERROR_ANNOUNCE_VM = "Error announcing the machine ";
    private static final String ERROR_WORKER_SHUTDOWN = "Exception raised on worker shutdown";
    private static final String ERROR_ANNOUNCE_VM_DESTROY = "Error announcing VM destruction";
    private static final String ERROR_USELESS_VM = "Useless VM";
    private static final String WARN_VM_REFUSED = "New resource has been refused because COMPSs has been stopped";
    private static final String WARN_CANNOT_PROVIDE_VM = "Provider can not provide the vm";

    private static ResourceUser listener;
    private static final Object countSynchronizer = new Object();
    private static Integer count = 0;

    private final Operations operations;
    private final String name; // Id for the CloudProvider or IP if VM is reused
    private final String provider;
    private final ResourceCreationRequest rcr;
    private final VM reused;


    /**
     * New helper thread for VM creation with the given properties
     * 
     * @param operations
     * @param name
     * @param provider
     * @param rR
     * @param reused
     */
    public CreationThread(Operations operations, String name, String provider, ResourceCreationRequest rR, VM reused) {
        this.setName("Creation Thread " + name);

        this.operations = operations;
        this.provider = provider;
        this.name = name;
        this.rcr = rR;
        this.reused = reused;
        synchronized (countSynchronizer) {
            count++;
        }
    }

    @Override
    public void run() {        
        boolean check = operations.getCheck();
        runtimeLogger.debug("Operations check = " + check);

        CloudMethodResourceDescription requested = rcr.getRequested();
        VM granted;

        if (reused == null) { // If the resources does not exist --> Create
            try {
                granted = createResourceOnProvider(requested);
            } catch (Exception e) {
                runtimeLogger.error("Error creating resource.", e);
                notifyFailure();
                return;
            }
            if (debug) {
                runtimeLogger.debug("Resource " + granted.getName() + " with id  " + granted.getEnvId() + " has been created ");
            }
            resourceLogger.info("RESOURCE_GRANTED = [\n\tNAME = " + granted.getName() + "\n\tSTATUS = ID " 
                                    + granted.getEnvId() + " CREATED\n]");
        } else {
            granted = reused;
            if (debug) {
                runtimeLogger.debug("Resource " + granted.getName() + " with id  " + granted.getEnvId() + " has been reused ");
            }
            resourceLogger.info("RESOURCE_GRANTED = [\n\tNAME = " + reused.getName() + "\n\tSTATUS = ID " 
                                    + granted.getEnvId() + " REUSED\n]");
        }

        this.setName("Creation Thread " + granted.getName());
        CloudMethodWorker r = ResourceManager.getDynamicResource(granted.getName());
        if (r == null) { // Resources are provided in a new VM
            if (reused == null) { // And are new --> Initiate VM
                try {
                    if (debug) {
                        runtimeLogger.debug(" Preparing new worker resource " + granted.getName() + ".");
                    }
                    r = prepareNewResource(granted);
                    operations.vmReady(granted);
                } catch (Exception e) {
                    runtimeLogger.error(ERROR_PREPARING_VM, e);
                    powerOff(granted);
                    notifyFailure();
                    return;
                }
            } else {
                int limitOfTasks = granted.getDescription().getTotalCPUComputingUnits();
                r = new CloudMethodWorker(granted.getDescription(), granted.getNode(), limitOfTasks,
                        rcr.getRequested().getImage().getSharedDisks());
                if (debug) {
                    runtimeLogger.debug("Worker for new resource " + granted.getName() + " set.");
                }
            }
            granted.setWorker(r);
            ResourceManager.addCloudWorker(rcr, r);
        } else {
            // Resources are provided in an existing VM
            ResourceManager.increasedCloudWorker(rcr, r, granted.getDescription());
        }

        synchronized (countSynchronizer) {
            count--;
        }
    }
    
    /**
     * Returns the number of active creation threads
     * 
     * @return
     */
    public static int getCount() {
        return count;
    }

    /**
     * Sets the associated task dispatcher
     * 
     * @param listener
     */
    public static void setTaskDispatcher(ResourceUser listener) {
        CreationThread.listener = listener;
    }

    /**
     * Returns the associated task dispatcher
     * 
     * @return
     */
    public static ResourceUser getTaskDispatcher() {
        return CreationThread.listener;
    }

    private VM createResourceOnProvider(CloudMethodResourceDescription requested) throws ConnectorException {
        VM granted;
        Object envID;
        // ASK FOR THE VIRTUAL RESOURCE
        try {
            // Turn on the VM and expects the new mr description
            envID = operations.poweron(name, requested);
        } catch (ConnectorException e) {
            runtimeLogger.error(ERROR_ASKING_NEW_RESOURCE + provider + "\n", e);
            resourceLogger.error("ERROR_MSG = [\n\t" + ERROR_ASKING_NEW_RESOURCE + provider + "\n]", e);
            throw e;
        }

        if (envID == null) {
            runtimeLogger.info(WARN_CANNOT_PROVIDE_VM);
            resourceLogger.info("INFO_MSG = [\n\t" + provider + WARN_CANNOT_PROVIDE_VM + "\n]");
            throw new ConnectorException(WARN_CANNOT_PROVIDE_VM);
        }

        // WAITING FOR THE RESOURCES TO BE RUNNING
        try {
            // Wait until the VM has been created
            granted = operations.waitCreation(envID, requested);
        } catch (ConnectorException e) {
            runtimeLogger.error(ERROR_WAITING_VM + provider + "\n", e);
            resourceLogger.error("ERROR_MSG = [\n\t" + ERROR_WAITING_VM + provider + "\n]", e);
            try {
                operations.destroy(envID);
            } catch (ConnectorException ex) {
                runtimeLogger.error(ERROR_POWEROFF_VM);
                resourceLogger.error("ERROR_MSG = [\n\t" + ERROR_POWEROFF_VM + "\n]");
            }
            throw new ConnectorException("Error waiting for the vm");
        }

        if (granted != null) {
            resourceLogger.debug("CONNECTOR_REQUEST = [");
            resourceLogger.debug("\tPROC_CPU_CU = " + requested.getTotalCPUComputingUnits());
            resourceLogger.debug("\tPROC_GPU_CU = " + requested.getTotalGPUComputingUnits());
            resourceLogger.debug("\tPROC_FPGA_CU = " + requested.getTotalFPGAComputingUnits());
            resourceLogger.debug("\tPROC_OTHER_CU = " + requested.getTotalOTHERComputingUnits());
            resourceLogger.debug("\tOS = " + requested.getOperatingSystemType());
            resourceLogger.debug("\tMEM = " + requested.getMemorySize());
            resourceLogger.debug("]");
            CloudMethodResourceDescription desc = granted.getDescription();
            resourceLogger.debug("CONNECTOR_GRANTED = [");
            resourceLogger.debug("\tPROC_CPU_CU = " + desc.getTotalCPUComputingUnits());
            resourceLogger.debug("\tPROC_GPU_CU = " + desc.getTotalGPUComputingUnits());
            resourceLogger.debug("\tPROC_FPGA_CU = " + desc.getTotalFPGAComputingUnits());
            resourceLogger.debug("\tPROC_OTHER_CU = " + desc.getTotalOTHERComputingUnits());
            resourceLogger.debug("\tOS = " + desc.getOperatingSystemType());
            resourceLogger.debug("\tMEM = " + desc.getMemorySize());
            resourceLogger.debug("]");
        } else {
            throw new ConnectorException(ERROR_GRANTED_NULL);
        }
        return granted;
    }

    private CloudMethodWorker prepareNewResource(VM vm) throws ConnectorException {
        CloudMethodResourceDescription granted = vm.getDescription();
        CloudImageDescription cid = granted.getImage();
        HashMap<String, String> workerProperties = cid.getProperties();
        String user = cid.getConfig().getUser();
        String password = workerProperties.get(AbstractConnector.PROPERTY_PASSW_NAME);
        try {
            operations.configureAccess(granted.getName(), user, password);
        } catch (ConnectorException e) {
            runtimeLogger.error(ERROR_CONFIGURE_ACCESS_VM + granted.getName(), e);
            resourceLogger.error("ERROR_MSG = [\n\t" + ERROR_CONFIGURE_ACCESS_VM + "\n\tNAME = " + granted.getName() + "\n\tPROVIDER =  "
                    + provider + "\n]", e);
            throw e;
        }

        try {
            operations.prepareMachine(granted.getName(), cid);
        } catch (ConnectorException e) {
            runtimeLogger.error(ERROR_PREPARING_VM + granted.getName(), e);
            resourceLogger.error("ERROR_MSG = [\n\t" + ERROR_PREPARING_VM + granted.getName() + "]", e);
            throw e;
        }
        CloudMethodWorker worker;
        MethodConfiguration mc = cid.getConfig();
        try {
            int limitOfTasks = mc.getLimitOfTasks();
            int computingUnits = granted.getTotalCPUComputingUnits();
            if (limitOfTasks < 0 && computingUnits < 0) {
                mc.setLimitOfTasks(0);
                mc.setTotalComputingUnits(0);
            } else {
                mc.setLimitOfTasks(Math.max(limitOfTasks, computingUnits));
                mc.setTotalComputingUnits(Math.max(limitOfTasks, computingUnits));
            }
            mc.setHost(granted.getName());

            worker = new CloudMethodWorker(granted.getName(), granted, mc, cid.getSharedDisks());
            //worker.start();
        } catch (Exception e) {
            runtimeLogger.error(ERROR_START_WORKER + granted.getName(), e);
            resourceLogger.error(
                    "ERROR_MSG = [\n\t" + ERROR_START_WORKER + "\n\tNAME = " + granted.getName() + "\n\tPROVIDER =  " + provider + "\n]");

            throw new ConnectorException(ERROR_START_VM, e);
        }

        try {
            worker.announceCreation();
        } catch (Exception e) {
            Semaphore sem = new Semaphore(0);
            ShutdownListener sl = new ShutdownListener(sem);
            worker.stop(sl);
            runtimeLogger.error(ERROR_ANNOUNCE_VM + granted.getName() + ". Shutting down", e);
            sl.enable();
            try {
                sem.acquire();
            } catch (Exception e2) {
                resourceLogger.error(ERROR_WORKER_SHUTDOWN, e2);
            }
            runtimeLogger.error("Machine " + granted.getName() + " shut down because an error announcing destruction");
            resourceLogger.error(
                    "ERROR_MSG = [\n\t" + ERROR_ANNOUNCE_VM + "\n\tNAME = " + granted.getName() + "\n\tPROVIDER =  " + provider + "\n]", e);

            throw new ConnectorException(e);
        }

        // Add the new machine to ResourceManager
        if (operations.getTerminate()) {
            resourceLogger.info("INFO_MSG = [\n\t" + WARN_VM_REFUSED + "\n\tRESOURCE_NAME = " + granted.getName() + "\n]");
            try {
                worker.announceDestruction();
            } catch (Exception e) {
                resourceLogger.error("ERROR_MSG = [\n\t" + ERROR_ANNOUNCE_VM_DESTROY + "\n\tVM_NAME = " + granted.getName() + "\n]", e);
            }
            Semaphore sem = new Semaphore(0);
            ShutdownListener sl = new ShutdownListener(sem);
            worker.stop(sl);

            sl.enable();
            try {
                sem.acquire();
            } catch (Exception e) {
                resourceLogger.error(ERROR_WORKER_SHUTDOWN);
            }

            throw new ConnectorException(ERROR_USELESS_VM);
        }

        return worker;
    }

    private void powerOff(VM granted) {
        try {
            operations.poweroff(granted);
        } catch (Exception e) {
            resourceLogger.error("ERROR_MSG = [\n\t" + ERROR_POWEROFF_VM + "\n]", e);
        }
    }

    private void notifyFailure() {
        synchronized (countSynchronizer) {
            count--;
        }
    }

}
