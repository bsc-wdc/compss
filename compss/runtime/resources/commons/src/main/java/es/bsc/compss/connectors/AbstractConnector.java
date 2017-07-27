package es.bsc.compss.connectors;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.connectors.utils.CreationThread;
import es.bsc.compss.connectors.utils.DeletionThread;
import es.bsc.compss.connectors.utils.Operations;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.CloudProvider;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;
import es.bsc.compss.types.ResourceCreationRequest;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.ShutdownListener;

/**
 * Abstract representation of a Runtime Cloud Connector
 *
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

    // Constants
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
    private static final long MINIM_DEADLINE_INTERVAL = 10_000;
    private static final long DELETE_SAFETY_INTERVAL = HALF_MIN;
    private static final long MAX_DEADLINE_INTERVAL = ONE_MIN;

    private float currentCostPerHour;
    private final float deletedMachinesCost;
    private long meanCreationTime; // MS
    private int createdVMs;

    private final CloudProvider provider;
    private final ConcurrentHashMap<String, VM> IPToVM;
    private final ConcurrentHashMap<Object, Long> powerOnVMTimestamp;
    private boolean terminate = false;
    private boolean check = false;
    private final DeadlineThread dead;
    private final TreeSet<VM> vmsToDelete;
    private final LinkedList<VM> vmsAlive;

    /**
     * New abstract connector implementation
     *
     * @param provider
     * @param props
     */
    public AbstractConnector(CloudProvider provider, Map<String, String> props) {
        this.provider = provider;
        IPToVM = new ConcurrentHashMap<>();
        powerOnVMTimestamp = new ConcurrentHashMap<>();
        vmsToDelete = new TreeSet<>();
        vmsAlive = new LinkedList<>();
        // ipToConnection = Collections.synchronizedMap(new HashMap<String, Connection>());

        String estCreationTimeStr = props.get(PROP_ESTIMATED_CREATION_TIME);
        if (estCreationTimeStr != null) {
            meanCreationTime = Integer.parseInt(estCreationTimeStr) * S_TO_MS;
        } else {
            String maxCreationTimeStr = props.get(PROP_MAX_VM_CREATION_TIME);
            if (maxCreationTimeStr != null) {
                meanCreationTime = Integer.parseInt(maxCreationTimeStr) * S_TO_MS;
            } else {
                meanCreationTime = INITIAL_CREATION_TIME;
            }
        }

        LOGGER.debug("[Abstract Connector] Initial mean creation time is" + meanCreationTime);
        createdVMs = 0;
        currentCostPerHour = 0.0f;
        deletedMachinesCost = 0.0f;
        terminate = false;
        check = false;
        dead = new DeadlineThread();
        dead.start();
        Runtime.getRuntime().addShutdownHook(new Ender(this));
    }

    /*
     *
     * Connector interface
     */
    @Override
    public boolean turnON(String name, ResourceCreationRequest rR) {
        if (terminate) {
            return false;
        }
        LOGGER.info("[Abstract Connector]Requesting a resource creation " + name + " : " + rR.getRequested().toString());
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

    private synchronized VM tryToReuseVM(CloudMethodResourceDescription requested) {
        String imageReq = requested.getImage().getImageName();
        VM reusedVM = null;
        synchronized (vmsToDelete) {
            for (VM vm : vmsToDelete) {
                if (!vm.getDescription().getImage().getImageName().equals(imageReq)) {
                    continue;
                }

                if (!vm.getDescription().contains(requested)) {
                    continue;
                }

                reusedVM = vm;
                reusedVM.setToDelete(false);
                vmsToDelete.remove(reusedVM);
                break;
            }
        }

        return reusedVM;
    }

    @Override
    public void stopReached() {
        check = true;
    }

    @Override
    public Long getNextCreationTime() throws ConnectorException {
        return meanCreationTime;
    }

    @Override
    public void terminate(CloudMethodWorker worker, CloudMethodResourceDescription reduction) {
        (new DeletionThread(this, worker, reduction)).start();
    }

    @Override
    public void terminateAll() {
        terminate = true;
        dead.terminate();

        synchronized (IPToVM) {
            for (VM vm : IPToVM.values()) {
                LOGGER.info("[Abstract Connector] Retrieving data from VM " + vm.getName());
                vm.getWorker().retrieveData(false);
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
        }

        // Clear all
        synchronized (IPToVM) {
            IPToVM.clear();
        }
        synchronized (vmsToDelete) {
            vmsToDelete.clear();
        }
        synchronized (vmsAlive) {
            vmsAlive.clear();
        }

        // Close connector
        close();
    }

    /**
     * Closes the specific connector
     *
     */
    protected abstract void close();

    @Override
    public Object poweron(String name, CloudMethodResourceDescription rd) throws ConnectorException {
        long requestTime = System.currentTimeMillis();
        Object vmId = create(name, rd);
        powerOnVMTimestamp.put(vmId, new Long(requestTime));
        return vmId;
    }

    /**
     * Contacts the Cloud Provider to create a new resource according to the
     * provided description.
     *
     * @param name Request name
     * @param rd Description of the required resources
     * @return the internal Provider Id of the resources
     * @throws ConnectorException there was a problem during the VM Creation
     *
     */
    public abstract Object create(String name, CloudMethodResourceDescription rd) throws ConnectorException;

    @Override
    public VM waitCreation(Object envId, CloudMethodResourceDescription requested) throws ConnectorException {
        CloudMethodResourceDescription granted = waitUntilCreation(envId, requested);
        VM vm = new VM(envId, granted);
        vm.setRequestTime(powerOnVMTimestamp.remove(envId));
        LOGGER.info("[Abstract Connector] Virtual machine created: " + vm);
        float oneHourCost = getMachineCostPerHour(granted);
        currentCostPerHour += oneHourCost;
        addMachine(vm);
        return vm;
    }

    /**
     *
     * Waits until some requested resources are created and available to the
     * external user.
     *
     * @param endId Internal Provider Id for the requested resources
     * @param requested Description of the requested resources
     * @return description of the granted resources
     * @throws ConnectorException An error ocurred during the resources wait.
     */
    public abstract CloudMethodResourceDescription waitUntilCreation(Object endId, CloudMethodResourceDescription requested)
            throws ConnectorException;

    @Override
    public void vmReady(VM vmInfo) throws ConnectorException {
        synchronized (this) {
            vmInfo.setStartTime(System.currentTimeMillis());
            vmInfo.computeCreationTime();

            long totaltime = meanCreationTime * createdVMs;
            totaltime += vmInfo.getCreationTime();
            createdVMs++;
            meanCreationTime = totaltime / createdVMs;
            LOGGER.debug("[Abstract Connector] New mean creation time :" + meanCreationTime);
        }

    }

    @Override
    public VM pause(CloudMethodWorker worker) {
        String ip = worker.getName();

        synchronized (IPToVM) {
            VM vmInfo = IPToVM.get(ip);
            if (vmInfo != null && canBeSaved(vmInfo)) {
                LOGGER.info("[Abstract Connector] Virtual machine saved: " + vmInfo);
                vmInfo.setToDelete(true);
                synchronized (vmsToDelete) {
                    vmsToDelete.add(vmInfo);
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

    private synchronized void addMachine(VM vmInfo) {
        String ip = vmInfo.getName();
        synchronized (IPToVM) {
            IPToVM.put(ip, vmInfo);
        }
        synchronized (vmsAlive) {
            vmsAlive.add(vmInfo);
        }
    }

    private synchronized void removeMachine(VM vmInfo) {

        synchronized (IPToVM) {
            IPToVM.remove(vmInfo.getName());
        }

        synchronized (vmsAlive) {
            vmsAlive.remove(vmInfo);
        }
        LOGGER.debug("[Abstract Connector] VM removed in the connector");
    }

    @Override
    public Float getTotalCost() {
        float aliveMachinesCost = 0;
        synchronized (vmsAlive) {
            long now = System.currentTimeMillis();
            for (VM vm : vmsAlive) {
                long numSlots = getNumSlots(now, vm.getStartTime());
                float pricePerSlot = getMachineCostPerTimeSlot(vm.getDescription());
                aliveMachinesCost += numSlots * pricePerSlot;
            }
        }
        float totalCost = aliveMachinesCost + deletedMachinesCost;
        return totalCost;
    }

    /**
     * Returns the machine cost per time slot for a given Resource Description
     *
     * @rd
     *
     * @param rd
     * @return
     */
    public abstract float getMachineCostPerTimeSlot(CloudMethodResourceDescription rd);

    @Override
    public abstract long getTimeSlot();

    @Override
    public Float currentCostPerHour() {
        return currentCostPerHour;
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
        return terminate;
    }

    @Override
    public boolean getCheck() {
        return check;
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
        return mod < limit - DELETE_SAFETY_INTERVAL; // my deadline is less than 2 min away
    }

    /**
     * Deadline thread for VM timeout
     *
     */
    private class DeadlineThread extends Thread {

        private boolean keepGoing;

        public DeadlineThread() {
            keepGoing = true;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("[Abstract Connector] Connector " + provider.getName() + " deadline");

            long sleepTime = 1_000l;
            while (keepGoing) {
                try {
                    LOGGER.debug("[Abstract Connector] Deadline thread sleeps " + sleepTime + " ms.");
                    Thread.sleep(sleepTime);
                } catch (Exception e) {
                }
                synchronized (vmsAlive) {
                    if (vmsAlive.isEmpty()) {
                        // LOGGER.info("MONITOR STATUS DEAD no VMs alive");
                        sleepTime = getSleepTime();
                        LOGGER.debug("[Abstract Connector] No VMs alive deadline sleep set to " + sleepTime + " ms.");
                        continue;
                    } else {
                        sleepTime = getSleepTime();
                        LOGGER.debug("[Abstract Connector] VMs alive initial sleep set to " + sleepTime + " ms.");
                        try {
                            Iterator<VM> vms = vmsAlive.iterator();
                            while (vms.hasNext()) {
                                VM vmInfo = vms.next();
                                long timeLeft = timeLeft(vmInfo.getStartTime());
                                // LOGGER.info("MONITOR STATUS DEAD next VM " + vmInfo.ip + " @ " + vmInfo.startTime + " --> " + timeLeft);
                                if (timeLeft <= DELETE_SAFETY_INTERVAL) {
                                    if (vmInfo.isToDelete()) {
                                        LOGGER.info("[Abstract Connector] Deleting vm " + vmInfo.getName()
                                                + " because is marked to delete and it is on the safety delete interval");
                                        vmsAlive.pollFirst();
                                        synchronized (vmsToDelete) {
                                            vmsToDelete.remove(vmInfo);
                                        }
                                        DeletionThread dt;
                                        dt = new DeletionThread((Operations) AbstractConnector.this, vmInfo);
                                        dt.start();
                                    }
                                } else if (sleepTime > timeLeft - DELETE_SAFETY_INTERVAL) {
                                    sleepTime = timeLeft - DELETE_SAFETY_INTERVAL;
                                    LOGGER.debug("[Abstract Connector] Evaluating sleep time for "+vmInfo.getName()+" because an interval near to finish " + sleepTime + " ms.");
                                }
                            }
                        } catch (ConcurrentModificationException e) {
                            LOGGER.debug("[Abstract Connector] Concurrent modification in deadline thread. Ignoring", e);
                            sleepTime = 1_000l;
                        }

                    }
                }
            }
        }

        private long getSleepTime() {
            long time = getTimeSlot();
            if (time <= 0) {
                return MINIM_DEADLINE_INTERVAL;
            } else {
                time = time - DELETE_SAFETY_INTERVAL;
                if (time > MAX_DEADLINE_INTERVAL) {
                    return MAX_DEADLINE_INTERVAL;
                } else {
                    return time;
                }
            }
        }

        public void terminate() {
            keepGoing = false;
            this.interrupt();
        }

        private long timeLeft(long time) {
            long now = System.currentTimeMillis();
            long limit = getTimeSlot();
            if (limit <= 0) {
                return 0;
            }
            long result = limit - ((now - time) % limit);
            LOGGER.debug("Calculating sleep time at "+time+" now is "+now+": remaining --> "+ limit + " - " + (now - time)+" % "+limit +" = "+ result + " ms to deadline");
            return result;
        }

    }

    /**
     * Ender Thread for JVM destruction
     *
     */
    private class Ender extends Thread {

        private final AbstractConnector ac;

        public Ender(AbstractConnector ac) {
            this.ac = ac;
        }

        @Override
        public void run() {
            for (VM vm : IPToVM.values()) {
                try {
                    LOGGER.info("[Abstract Connector] Destroying VM " + vm.getName());
                    ac.destroy(vm.getEnvId());
                } catch (ConnectorException e) {
                    LOGGER.info("[Abstract Connector] Error while trying to  the virtual machine " + vm.getName());
                } finally {
                    ac.close();
                }
            }
        }
    }

}
