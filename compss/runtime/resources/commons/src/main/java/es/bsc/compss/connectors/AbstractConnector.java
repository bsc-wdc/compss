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
package es.bsc.compss.connectors;

import es.bsc.compss.connectors.utils.CreationThread;
import es.bsc.compss.connectors.utils.DeadlineThread;
import es.bsc.compss.connectors.utils.DeletionThread;
import es.bsc.compss.connectors.utils.Operations;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.CloudProvider;
import es.bsc.compss.types.ResourceCreationRequest;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Abstract representation of a Runtime Cloud Connector.
 */
public abstract class AbstractConnector implements Connector, Operations, Cost {

    // Properties' names
    public static final String PROP_ESTIMATED_CREATION_TIME = "estimated-creation-time";
    public static final String PROP_MAX_VM_CREATION_TIME = "max-vm-creation-time";
    public static final String PROP_SERVER = "Server";
    public static final String PROP_PORT = "Port";
    public static final String PROPERTY_PASSW_NAME = "Password";
    public static final String PROP_APP_NAME = "app-name";
    public static final String ADAPTOR_MAX_PORT_PROPERTY_NAME = "adaptor-max-port";
    public static final String ADAPTOR_MIN_PORT_PROPERTY_NAME = "adaptor-min-port";

    // Time constants
    protected static final long MIN_TO_S = 60;
    protected static final long S_TO_MS = 1_000;
    protected static final long ONE_HOUR = 3_600_000;
    protected static final long TWO_MIN = 120_000;
    protected static final long ONE_MIN = 60_000;
    protected static final long HALF_MIN = 30_000;

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.CONNECTORS);

    // Timer properties
    private static final long INITIAL_CREATION_TIME = ONE_MIN;

    private final CloudProvider provider;
    private float currentCostPerHour;
    private final float deletedMachinesCost;
    private long meanCreationTime; // MS
    private int createdVMs;

    private final ConcurrentHashMap<String, VM> ip2vm;
    private final ConcurrentHashMap<Object, Long> powerOnVMTimestamp;
    private final TreeSet<VM> vmsToDelete;
    private final LinkedList<VM> vmsAlive;

    private boolean terminate = false;
    private boolean check = false;

    private final DeadlineThread dead;


    /**
     * New abstract connector implementation.
     *
     * @param provider Cloud provider.
     * @param props Connector specific properties.
     */
    public AbstractConnector(CloudProvider provider, Map<String, String> props) {
        this.provider = provider;

        String estCreationTimeStr = props.get(PROP_ESTIMATED_CREATION_TIME);
        if (estCreationTimeStr != null) {
            this.meanCreationTime = Integer.parseInt(estCreationTimeStr) * S_TO_MS;
        } else {
            String maxCreationTimeStr = props.get(PROP_MAX_VM_CREATION_TIME);
            if (maxCreationTimeStr != null) {
                this.meanCreationTime = Integer.parseInt(maxCreationTimeStr) * S_TO_MS;
            } else {
                this.meanCreationTime = INITIAL_CREATION_TIME;
            }
        }
        LOGGER.debug("[Abstract Connector] Initial mean creation time is " + this.meanCreationTime);

        this.createdVMs = 0;
        this.currentCostPerHour = 0.0f;
        this.deletedMachinesCost = 0.0f;
        this.terminate = false;
        this.check = false;

        this.ip2vm = new ConcurrentHashMap<>();
        this.powerOnVMTimestamp = new ConcurrentHashMap<>();
        this.vmsToDelete = new TreeSet<>();
        this.vmsAlive = new LinkedList<>();

        // Deadline thread
        this.dead = new DeadlineThread(this);
        this.dead.start();

        // Ender hook
        Runtime.getRuntime().addShutdownHook(new Ender(this));
    }

    /**
     * Returns the associated Cloud Provider.
     * 
     * @return The associated Cloud Provider.
     */
    public CloudProvider getProvider() {
        return this.provider;
    }

    /**
     * Returns a shallow copy of the alive VMs.
     * 
     * @return A shallow copy of the alive VMs.
     */
    @SuppressWarnings("unchecked")
    public List<VM> getCopyOfAliveVms() {
        synchronized (this.vmsAlive) {
            return (List<VM>) this.vmsAlive.clone();
        }
    }

    /**
     * Removes the given VM from the registered alive VMs. Does nothing if VM is not registered.
     * 
     * @param vm VM to remove from alive VMs.
     */
    public void removeFromAliveVms(VM vm) {
        synchronized (this.vmsAlive) {
            this.vmsAlive.remove(vm);
        }
    }

    /**
     * Removes the given VM from the registere toDelete VMs. Does nothing if VM is not registered.
     * 
     * @param vm VM to remove from toDelete VMs.
     */
    public void removeFromDeleteVms(VM vm) {
        synchronized (this.vmsToDelete) {
            this.vmsToDelete.remove(vm);
        }
    }

    /*
     *
     * Connector interface
     */
    @Override
    public boolean turnON(String name, ResourceCreationRequest rR) {
        if (this.terminate) {
            return false;
        }
        LOGGER
            .info("[Abstract Connector] Requesting a resource creation " + name + " : " + rR.getRequested().toString());
        // Check if we can reuse one of the vms put to delete (but not yet destroyed)
        VM vmInfo = tryToReuseVM(rR.getRequested());
        if (vmInfo != null) {
            LOGGER.info("[Abstract Connector] Reusing VM: " + vmInfo);
            CreationThread ct = new CreationThread((Operations) this, vmInfo.getName(), rR.getProvider(), rR, vmInfo);
            ct.start();
            return true;
        }
        try {
            CreationThread ct = new CreationThread((Operations) this, name, rR.getProvider(), rR, null);
            ct.start();
        } catch (Exception e) {
            LOGGER.info("[Abstract Connector] ResourceRequest failed", e);
            return false;
        }
        return true;
    }

    private VM tryToReuseVM(CloudMethodResourceDescription requested) {
        String imageReq = requested.getImage().getImageName();
        VM reusedVM = null;
        synchronized (this.vmsToDelete) {
            for (VM vm : this.vmsToDelete) {
                if (!vm.getDescription().getImage().getImageName().equals(imageReq)) {
                    continue;
                }

                if (!vm.getDescription().contains(requested)) {
                    continue;
                }

                reusedVM = vm;
                reusedVM.setToDelete(false);
                this.vmsToDelete.remove(reusedVM);
                break;
            }
        }

        return reusedVM;
    }

    @Override
    public void stopReached() {
        this.check = true;
    }

    @Override
    public Long getNextCreationTime() throws ConnectorException {
        return this.meanCreationTime;
    }

    @Override
    public void terminate(CloudMethodWorker worker, CloudMethodResourceDescription reduction) {
        (new DeletionThread(this, worker, reduction)).start();
    }

    @Override
    public void terminateAll() {
        // Mark the thread to terminate
        this.terminate = true;

        // Ask the deadline to terminate and wait
        this.dead.terminate();
        try {
            Thread.sleep(DeadlineThread.getMaxDeadlineInterval());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        // Clean pending VMs to delete
        synchronized (this.vmsToDelete) {
            for (VM vm : this.vmsToDelete) {
                try {
                    poweroff(vm);
                } catch (Exception e) {
                    LOGGER.error("ERROR: Exception while trying to destroy the virtual machine " + vm.getName(), e);
                }
                this.vmsToDelete.clear();
            }
        }

        // CLean ip2vm
        synchronized (this.ip2vm) {
            for (VM vm : this.ip2vm.values()) {
                LOGGER.info("[Abstract Connector] Retrieving data from VM " + vm.getName());
                vm.getWorker().disableExecution();
                vm.getWorker().retrieveTracingAndDebugData();
                LOGGER.info("[Abstract Connector] Destroying VM " + vm.getName());
                Semaphore sem = new Semaphore(0);
                ShutdownListener sl = new ShutdownListener(sem);
                vm.getWorker().stop(sl);
                sl.enable();
                try {
                    sem.acquire();
                } catch (Exception e) {
                    LOGGER.error("ERROR: Exception raised on worker shutdown");
                }
                try {
                    destroy(vm.getEnvId());
                } catch (Exception e) {
                    LOGGER.error("ERROR: Exception while trying to destroy the virtual machine " + vm.getName(), e);
                }
            }

            this.ip2vm.clear();
        }

        // Clean remaining alive VMs
        synchronized (this.vmsAlive) {
            this.vmsAlive.clear();
        }

        // Close connector
        close();
    }

    /**
     * Closes the specific connector.
     */
    protected abstract void close();

    @Override
    public Object poweron(String name, CloudMethodResourceDescription rd) throws ConnectorException {
        long requestTime = System.currentTimeMillis();
        Object vmId = create(name, rd);
        this.powerOnVMTimestamp.put(vmId, new Long(requestTime));
        return vmId;
    }

    /**
     * Contacts the Cloud Provider to create a new resource according to the provided description.
     *
     * @param name Request name
     * @param rd Description of the required resources
     * @return the internal Provider Id of the resources
     * @throws ConnectorException there was a problem during the VM Creation
     */
    public abstract Object create(String name, CloudMethodResourceDescription rd) throws ConnectorException;

    @Override
    public VM waitCreation(Object envId, CloudMethodResourceDescription requested) throws ConnectorException {
        CloudMethodResourceDescription granted = waitUntilCreation(envId, requested);
        VM vm = new VM(envId, granted);
        vm.setRequestTime(this.powerOnVMTimestamp.remove(envId));
        LOGGER.info("[Abstract Connector] Virtual machine created: " + vm);
        float oneHourCost = getMachineCostPerHour(granted);
        this.currentCostPerHour += oneHourCost;
        addMachine(vm);
        return vm;
    }

    /**
     * Waits until some requested resources are created and available to the external user.
     *
     * @param endId Internal Provider Id for the requested resources
     * @param requested Description of the requested resources
     * @return description of the granted resources
     * @throws ConnectorException An error occurred during the resources wait.
     */
    public abstract CloudMethodResourceDescription waitUntilCreation(Object endId,
        CloudMethodResourceDescription requested) throws ConnectorException;

    @Override
    public void vmReady(VM vmInfo) throws ConnectorException {
        synchronized (this) {
            vmInfo.setStartTime(System.currentTimeMillis());
            vmInfo.computeCreationTime();

            long totaltime = this.meanCreationTime * this.createdVMs;
            totaltime += vmInfo.getCreationTime();
            this.createdVMs++;
            this.meanCreationTime = totaltime / createdVMs;
            LOGGER.debug("[Abstract Connector] New mean creation time: " + this.meanCreationTime);
        }
    }

    @Override
    public VM pause(CloudMethodWorker worker) {
        String ip = worker.getName();

        synchronized (this.ip2vm) {
            VM vmInfo = this.ip2vm.get(ip);
            if (vmInfo != null && canBeSaved(vmInfo)) {
                LOGGER.info("[Abstract Connector] Virtual machine saved: " + vmInfo);
                vmInfo.setToDelete(true);
                synchronized (this.vmsToDelete) {
                    this.vmsToDelete.add(vmInfo);
                }
                return null;
            }
            return vmInfo; // If null means it can be deleted too;
        }
    }

    @Override
    public void poweroff(VM vm) throws ConnectorException {
        destroy(vm.getEnvId());
        removeMachine(vm);
    }

    private long getNumSlots(long time1, long time2) {
        long dif = time1 - time2;
        long slotTime = getTimeSlot();
        if (slotTime <= 0) {
            return time1 - time2;
        }
        long numSlots = (long) (dif / slotTime);
        if (dif % slotTime > 0) {
            numSlots++;
        }
        return numSlots;
    }

    private void addMachine(VM vmInfo) {
        String ip = vmInfo.getName();
        synchronized (this.ip2vm) {
            this.ip2vm.put(ip, vmInfo);
        }
        synchronized (this.vmsAlive) {
            this.vmsAlive.add(vmInfo);
        }
    }

    private void removeMachine(VM vmInfo) {
        synchronized (this.ip2vm) {
            this.ip2vm.remove(vmInfo.getName());
        }

        synchronized (this.vmsAlive) {
            this.vmsAlive.remove(vmInfo);
        }
        LOGGER.debug("[Abstract Connector] VM removed in the connector");
    }

    @Override
    public Float getTotalCost() {
        float aliveMachinesCost = 0;
        synchronized (this.vmsAlive) {
            long now = System.currentTimeMillis();
            for (VM vm : this.vmsAlive) {
                long numSlots = getNumSlots(now, vm.getStartTime());
                float pricePerSlot = getMachineCostPerTimeSlot(vm.getDescription());
                aliveMachinesCost += numSlots * pricePerSlot;
            }
        }
        float totalCost = aliveMachinesCost + this.deletedMachinesCost;
        return totalCost;
    }

    /**
     * Returns the machine cost per time slot for a given Resource Description.
     *
     * @param rd Machine resource description.
     * @return Cost per time slot.
     */
    public abstract float getMachineCostPerTimeSlot(CloudMethodResourceDescription rd);

    @Override
    public abstract long getTimeSlot();

    @Override
    public Float currentCostPerHour() {
        return this.currentCostPerHour;
    }

    @Override
    public Float getMachineCostPerHour(CloudMethodResourceDescription rc) {
        if (getTimeSlot() > 0) {
            return getMachineCostPerTimeSlot(rc) * ONE_HOUR / getTimeSlot();
        } else {
            return getMachineCostPerTimeSlot(rc);
        }
    }

    @Override
    public boolean getTerminate() {
        return this.terminate;
    }

    @Override
    public boolean getCheck() {
        return this.check;
    }

    private boolean canBeSaved(VM vmInfo) {
        long now = System.currentTimeMillis();
        long vmStart = vmInfo.getStartTime();
        long limit = getTimeSlot();
        if (limit <= 0) {
            return false;
        }
        long dif = now - vmStart;
        long mod = dif % limit;
        return mod < limit - DeadlineThread.getDeleteSafetyInterval();
    }


    /**
     * Ender Thread for JVM destruction.
     */
    private class Ender extends Thread {

        private final AbstractConnector ac;


        public Ender(AbstractConnector ac) {
            this.ac = ac;
        }

        @Override
        public void run() {
            for (VM vm : ip2vm.values()) {
                try {
                    LOGGER.info("[Abstract Connector] Destroying VM " + vm.getName());
                    this.ac.destroy(vm.getEnvId());
                } catch (ConnectorException e) {
                    LOGGER.info("[Abstract Connector] Error while trying to  the virtual machine " + vm.getName());
                } finally {
                    this.ac.close();
                }
            }
        }
    }

}
