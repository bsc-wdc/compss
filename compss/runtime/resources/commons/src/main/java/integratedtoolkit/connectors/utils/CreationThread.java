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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper thread for VM creation
 *
 */
public class CreationThread extends Thread {

    // Loggers
    private static final Logger RESOURCE_LOGGER = LogManager.getLogger(Loggers.CONNECTORS_UTILS);
    private static final Logger RUNTIME_LOGGER = LogManager.getLogger(Loggers.RM_COMP);
    private static final boolean DEBUG = RESOURCE_LOGGER.isDebugEnabled();

    // Error and warn messages
    private static final String ERROR_ASKING_NEW_RESOURCE = "Error asking a new Resource to ";
    private static final String ERROR_WAITING_VM = "Error waiting for a machine that should be provided by ";
    private static final String ERROR_POWEROFF_VM = "Cannot poweroff the machine\n]";
    private static final String ERROR_GRANTED_NULL = "Error: Granted description is null";
    private static final String ERROR_CONFIGURE_ACCESS_VM = "Error configuring access to machine ";
    private static final String ERROR_PREPARING_VM = "Exception preparing machine ";
    private static final String ERROR_ANNOUNCE_VM = "Error announcing the machine ";
    private static final String ERROR_WORKER_SHUTDOWN = "Exception raised on worker shutdown";
    private static final String ERROR_ANNOUNCE_VM_DESTROY = "Error announcing VM destruction";
    private static final String ERROR_USELESS_VM = "Useless VM";
    private static final String WARN_VM_REFUSED = "New resource has been refused because COMPSs has been stopped";
    private static final String WARN_CANNOT_PROVIDE_VM = "Provider can not provide the vm";

    private static ResourceUser listener;
    private static final AtomicInteger COUNT = new AtomicInteger(0);

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
        COUNT.incrementAndGet();
    }

    /**
     * Returns the number of active creation threads
     *
     * @return
     */
    public static int getCount() {
        return COUNT.get();
    }

    @Override
    public void run() {
        boolean check = operations.getCheck();
        RUNTIME_LOGGER.debug("Operations check = " + check);

        CloudMethodResourceDescription requested = rcr.getRequested();
        VM granted;

        if (reused == null) { // If the resources does not exist --> Create           
            this.setName("Creation Thread " + name);
            try {
                granted = createResourceOnProvider(requested);
            } catch (Exception e) {
                RUNTIME_LOGGER.error(ERROR_ASKING_NEW_RESOURCE + provider, e);
                notifyFailure();
                return;
            }
            if (DEBUG) {
                RUNTIME_LOGGER.debug("Resource " + granted.getName() + " with id  " + granted.getEnvId() + " has been created ");
            }
            RESOURCE_LOGGER.info("RESOURCE_GRANTED = [\n\tNAME = " + granted.getName() + "\n\tSTATUS = ID "
                    + granted.getEnvId() + " CREATED\n]");
        } else {
            granted = reused;
            if (DEBUG) {
                RUNTIME_LOGGER.debug("Resource " + granted.getName() + " with id  " + granted.getEnvId() + " has been reused ");
            }
            RESOURCE_LOGGER.info("RESOURCE_GRANTED = [\n\tNAME = " + reused.getName() + "\n\tSTATUS = ID "
                    + granted.getEnvId() + " REUSED\n]");
        }

        this.setName("Creation Thread " + granted.getName());
        CloudMethodWorker r = ResourceManager.getDynamicResource(granted.getName());
        if (r == null) { // Resources are provided in a new VM
            if (reused == null) { // And are new --> Initiate VM
                try {
                    if (DEBUG) {
                        RUNTIME_LOGGER.debug(" Preparing new worker resource " + granted.getName() + ".");
                    }
                    r = prepareNewResource(granted);
                    operations.vmReady(granted);
                } catch (Exception e) {
                    RUNTIME_LOGGER.error(ERROR_PREPARING_VM, e);
                    powerOff(granted);
                    notifyFailure();
                    return;
                }
            } else {
                int limitOfTasks = granted.getDescription().getTotalCPUComputingUnits();
                r = new CloudMethodWorker(granted.getDescription(), granted.getNode(), limitOfTasks,
                        rcr.getRequested().getImage().getSharedDisks());
                if (DEBUG) {
                    RUNTIME_LOGGER.debug("Worker for new resource " + granted.getName() + " set.");
                }
            }
            granted.setWorker(r);
            ResourceManager.addCloudWorker(rcr, r);
        } else {
            // Resources are provided in an existing VM
            ResourceManager.increasedCloudWorker(rcr, r, granted.getDescription());
        }

        COUNT.decrementAndGet();
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
            RUNTIME_LOGGER.error(ERROR_ASKING_NEW_RESOURCE + provider + "\n", e);
            RESOURCE_LOGGER.error("ERROR_MSG = [\n\t" + ERROR_ASKING_NEW_RESOURCE + provider + "\n]", e);
            throw e;
        }

        if (envID == null) {
            RUNTIME_LOGGER.info(WARN_CANNOT_PROVIDE_VM);
            RESOURCE_LOGGER.info("INFO_MSG = [\n\t" + provider + WARN_CANNOT_PROVIDE_VM + "\n]");
            throw new ConnectorException(WARN_CANNOT_PROVIDE_VM);
        }

        // WAITING FOR THE RESOURCES TO BE RUNNING
        try {
            // Wait until the VM has been created
            granted = operations.waitCreation(envID, requested);
        } catch (ConnectorException e) {
            RUNTIME_LOGGER.error(ERROR_WAITING_VM + provider + "\n", e);
            RESOURCE_LOGGER.error("ERROR_MSG = [\n\t" + ERROR_WAITING_VM + provider + "\n]", e);
            try {
                operations.destroy(envID);
            } catch (ConnectorException ex) {
                RUNTIME_LOGGER.error(ERROR_POWEROFF_VM);
                RESOURCE_LOGGER.error("ERROR_MSG = [\n\t" + ERROR_POWEROFF_VM + "\n]");
            }
            throw new ConnectorException("Error waiting for the vm");
        }

        if (granted != null) {
            RESOURCE_LOGGER.debug("CONNECTOR_REQUEST = [");
            RESOURCE_LOGGER.debug("\tPROC_CPU_CU = " + requested.getTotalCPUComputingUnits());
            RESOURCE_LOGGER.debug("\tPROC_GPU_CU = " + requested.getTotalGPUComputingUnits());
            RESOURCE_LOGGER.debug("\tPROC_FPGA_CU = " + requested.getTotalFPGAComputingUnits());
            RESOURCE_LOGGER.debug("\tPROC_OTHER_CU = " + requested.getTotalOTHERComputingUnits());
            RESOURCE_LOGGER.debug("\tOS = " + requested.getOperatingSystemType());
            RESOURCE_LOGGER.debug("\tMEM = " + requested.getMemorySize());
            RESOURCE_LOGGER.debug("]");
            CloudMethodResourceDescription desc = granted.getDescription();
            RESOURCE_LOGGER.debug("CONNECTOR_GRANTED = [");
            RESOURCE_LOGGER.debug("\tPROC_CPU_CU = " + desc.getTotalCPUComputingUnits());
            RESOURCE_LOGGER.debug("\tPROC_GPU_CU = " + desc.getTotalGPUComputingUnits());
            RESOURCE_LOGGER.debug("\tPROC_FPGA_CU = " + desc.getTotalFPGAComputingUnits());
            RESOURCE_LOGGER.debug("\tPROC_OTHER_CU = " + desc.getTotalOTHERComputingUnits());
            RESOURCE_LOGGER.debug("\tOS = " + desc.getOperatingSystemType());
            RESOURCE_LOGGER.debug("\tMEM = " + desc.getMemorySize());
            RESOURCE_LOGGER.debug("]");
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
            RUNTIME_LOGGER.error(ERROR_CONFIGURE_ACCESS_VM + granted.getName(), e);
            RESOURCE_LOGGER.error("ERROR_MSG = [\n\t" + ERROR_CONFIGURE_ACCESS_VM + "\n\tNAME = " + granted.getName() + "\n\tPROVIDER =  "
                    + provider + "\n]", e);
            throw e;
        }

        try {
            operations.prepareMachine(granted.getName(), cid);
        } catch (ConnectorException e) {
            RUNTIME_LOGGER.error(ERROR_PREPARING_VM + granted.getName(), e);
            RESOURCE_LOGGER.error("ERROR_MSG = [\n\t" + ERROR_PREPARING_VM + granted.getName() + "]", e);
            throw e;
        }
        CloudMethodWorker worker;
        MethodConfiguration mc = cid.getConfig();
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

        try {
            worker.announceCreation();
        } catch (Exception e) {
            RUNTIME_LOGGER.error("Machine " + granted.getName() + " shut down because an error announcing creation");
            RESOURCE_LOGGER.error(
                    "ERROR_MSG = [\n\t" + ERROR_ANNOUNCE_VM + "\n\tNAME = " + granted.getName() + "\n\tPROVIDER =  " + provider + "\n]", e);

            throw new ConnectorException(e);
        }

        // Add the new machine to ResourceManager
        if (operations.getTerminate()) {
            RESOURCE_LOGGER.info("INFO_MSG = [\n\t" + WARN_VM_REFUSED + "\n\tRESOURCE_NAME = " + granted.getName() + "\n]");
            try {
                worker.announceDestruction();
            } catch (Exception e) {
                RESOURCE_LOGGER.error("ERROR_MSG = [\n\t" + ERROR_ANNOUNCE_VM_DESTROY + "\n\tVM_NAME = " + granted.getName() + "\n]", e);
            }
            Semaphore sem = new Semaphore(0);
            ShutdownListener sl = new ShutdownListener(sem);
            worker.stop(sl);

            sl.enable();
            try {
                sem.acquire();
            } catch (Exception e) {
                RESOURCE_LOGGER.error(ERROR_WORKER_SHUTDOWN);
            }

            throw new ConnectorException(ERROR_USELESS_VM);
        }

        return worker;
    }

    private void powerOff(VM granted) {
        try {
            operations.poweroff(granted);
        } catch (Exception e) {
            RESOURCE_LOGGER.error("ERROR_MSG = [\n\t" + ERROR_POWEROFF_VM + "\n]", e);
        }
    }

    private void notifyFailure() {
        COUNT.decrementAndGet();
    }

}
