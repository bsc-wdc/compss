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

import es.bsc.compss.components.ResourceUser;
import es.bsc.compss.connectors.AbstractConnector;
import es.bsc.compss.connectors.ConnectorException;
import es.bsc.compss.connectors.VM;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.CloudProvider;
import es.bsc.compss.types.ResourceCreationRequest;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;
import es.bsc.compss.types.resources.description.CloudImageDescription;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;
import es.bsc.compss.util.ResourceManager;

import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Helper thread for VM creation.
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
    private final CloudProvider provider;
    private final ResourceCreationRequest rcr;
    private final VM reused;


    /**
     * New helper thread for VM creation with the given properties.
     *
     * @param operations Operations to perform.
     * @param name Thread name.
     * @param provider Associated cloud provider.
     * @param rR Resource creation request.
     * @param reused Reused VM description.
     */
    public CreationThread(Operations operations, String name, CloudProvider provider, ResourceCreationRequest rR,
        VM reused) {

        this.setName("Creation Thread " + name);

        this.operations = operations;
        this.provider = provider;
        this.name = name;
        this.rcr = rR;
        this.reused = reused;
        COUNT.incrementAndGet();
    }

    /**
     * Returns the number of active creation threads.
     *
     * @return The number of active creation threads.
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

        if (this.reused == null) { // If the resources does not exist --> Create
            this.setName("Creation Thread " + this.name);
            try {
                granted = createResourceOnProvider(requested);
            } catch (Exception e) {
                RUNTIME_LOGGER.error(ERROR_ASKING_NEW_RESOURCE + this.provider, e);
                notifyFailure();
                return;
            }
            if (DEBUG) {
                RUNTIME_LOGGER
                    .debug("Resource " + granted.getName() + " with id  " + granted.getEnvId() + " has been created ");
            }
            RESOURCE_LOGGER.info("RESOURCE_GRANTED = [\n\tNAME = " + granted.getName() + "\n\tSTATUS = ID "
                + granted.getEnvId() + " CREATED\n]");
        } else {
            granted = this.reused;
            if (DEBUG) {
                RUNTIME_LOGGER
                    .debug("Resource " + granted.getName() + " with id  " + granted.getEnvId() + " has been reused ");
            }
            RESOURCE_LOGGER.info("RESOURCE_GRANTED = [\n\tNAME = " + this.reused.getName() + "\n\tSTATUS = ID "
                + granted.getEnvId() + " REUSED\n]");
        }
        String grantedName = granted.getName();
        this.setName("Creation Thread " + grantedName);
        CloudMethodWorker r = (CloudMethodWorker) ResourceManager.getDynamicResource(granted.getName());
        if (r == null) {
            // Resources are provided in a new VM
            if (this.reused == null) {
                // And are new --> Initiate VM
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
                int limitGPUTasks = granted.getDescription().getTotalGPUComputingUnits();
                int limitFPGATasks = granted.getDescription().getTotalFPGAComputingUnits();
                int limitOTHERTasks = granted.getDescription().getTotalOTHERComputingUnits();
                r = new CloudMethodWorker(grantedName, this.provider, granted.getDescription(), null, limitOfTasks,
                    limitGPUTasks, limitFPGATasks, limitOTHERTasks, rcr.getRequested().getImage().getSharedDisks());
                if (DEBUG) {
                    RUNTIME_LOGGER.debug("Worker for new resource " + grantedName + " set.");
                }
            }
            granted.setWorker(r);
            ResourceManager.addCloudWorker(this.rcr, r, granted.getDescription());
        } else {
            // Resources are provided in an existing VM
            ResourceManager.increasedCloudWorker(this.rcr, r, granted.getDescription());
        }

        COUNT.decrementAndGet();
    }

    /**
     * Sets the associated task dispatcher.
     *
     * @param listener Associated task dispatcher.
     */
    public static void setTaskDispatcher(ResourceUser listener) {
        CreationThread.listener = listener;
    }

    /**
     * Returns the associated task dispatcher.
     *
     * @return The associated task dispatcher.
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
            envID = this.operations.poweron(this.name, requested);
        } catch (ConnectorException e) {
            RUNTIME_LOGGER.error(ERROR_ASKING_NEW_RESOURCE + this.name + " to " + this.provider.getName() + "\n", e);
            RESOURCE_LOGGER.error(
                "ERROR_MSG = [\n\t" + ERROR_ASKING_NEW_RESOURCE + this.name + " to " + this.provider.getName() + "\n]",
                e);
            throw e;
        }

        if (envID == null) {
            RUNTIME_LOGGER.info(WARN_CANNOT_PROVIDE_VM);
            RESOURCE_LOGGER.info("INFO_MSG = [\n\t" + this.provider.getName() + WARN_CANNOT_PROVIDE_VM + " with name "
                + this.name + "\n]");
            throw new ConnectorException(WARN_CANNOT_PROVIDE_VM);
        }

        // WAITING FOR THE RESOURCES TO BE RUNNING
        try {
            // Wait until the VM has been created
            granted = this.operations.waitCreation(envID, requested);
        } catch (ConnectorException e) {
            RUNTIME_LOGGER.error(ERROR_WAITING_VM + envID + " to " + this.provider.getName() + "\n", e);
            RESOURCE_LOGGER
                .error("ERROR_MSG = [\n\t" + ERROR_WAITING_VM + envID + " to " + this.provider.getName() + "\n]", e);
            try {
                this.operations.destroy(envID);
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
        Map<String, String> workerProperties = cid.getProperties();
        String user = cid.getConfig().getUser();
        String password = workerProperties.get(AbstractConnector.PROPERTY_PASSW_NAME);
        try {
            this.operations.configureAccess(granted.getName(), user, password);
        } catch (ConnectorException e) {
            RUNTIME_LOGGER.error(ERROR_CONFIGURE_ACCESS_VM + granted.getName(), e);
            RESOURCE_LOGGER.error("ERROR_MSG = [\n\t" + ERROR_CONFIGURE_ACCESS_VM + "\n\tNAME = " + granted.getName()
                + "\n\tPROVIDER =  " + this.provider + "\n]", e);
            throw e;
        }

        try {
            this.operations.prepareMachine(granted.getName(), cid);
        } catch (ConnectorException e) {
            RUNTIME_LOGGER.error(ERROR_PREPARING_VM + granted.getName(), e);
            RESOURCE_LOGGER.error("ERROR_MSG = [\n\t" + ERROR_PREPARING_VM + granted.getName() + "]", e);
            throw e;
        }
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
        mc.setLimitOfGPUTasks(granted.getTotalGPUComputingUnits());
        mc.setTotalGPUComputingUnits(granted.getTotalGPUComputingUnits());
        mc.setLimitOfFPGATasks(granted.getTotalFPGAComputingUnits());
        mc.setTotalFPGAComputingUnits(granted.getTotalFPGAComputingUnits());
        mc.setLimitOfOTHERsTasks(granted.getTotalOTHERComputingUnits());
        mc.setTotalOTHERComputingUnits(granted.getTotalOTHERComputingUnits());

        // Fix NIO port range if requested by VM
        int minPort = granted.getImage().getConfig().getMinPort();
        if (minPort != -1) {
            mc.setMinPort(minPort);
        }
        int maxPort = granted.getImage().getConfig().getMaxPort();
        if (maxPort != -1) {
            mc.setMaxPort(maxPort);
        }

        // Create cloud method worker
        CloudMethodWorker worker =
            new CloudMethodWorker(granted.getName(), this.provider, granted, mc, cid.getSharedDisks());

        try {
            worker.announceCreation();
        } catch (Exception e) {
            RUNTIME_LOGGER.error("Machine " + granted.getName() + " shut down because an error announcing creation");
            RESOURCE_LOGGER.error("ERROR_MSG = [\n\t" + ERROR_ANNOUNCE_VM + "\n\tNAME = " + granted.getName()
                + "\n\tPROVIDER =  " + this.provider + "\n]", e);

            throw new ConnectorException(e);
        }

        // Add the new machine to ResourceManager
        if (this.operations.getTerminate()) {
            RESOURCE_LOGGER
                .info("INFO_MSG = [\n\t" + WARN_VM_REFUSED + "\n\tRESOURCE_NAME = " + granted.getName() + "\n]");
            try {
                worker.announceDestruction();
            } catch (Exception e) {
                RESOURCE_LOGGER.error(
                    "ERROR_MSG = [\n\t" + ERROR_ANNOUNCE_VM_DESTROY + "\n\tVM_NAME = " + granted.getName() + "\n]", e);
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
            this.operations.poweroff(granted);
        } catch (Exception e) {
            RESOURCE_LOGGER.error("ERROR_MSG = [\n\t" + ERROR_POWEROFF_VM + "\n]", e);
        }
    }

    private void notifyFailure() {
        COUNT.decrementAndGet();
    }

}
