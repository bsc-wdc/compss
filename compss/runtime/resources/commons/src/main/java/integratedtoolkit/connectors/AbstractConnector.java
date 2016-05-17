package integratedtoolkit.connectors;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import integratedtoolkit.connectors.utils.CreationThread;
import integratedtoolkit.connectors.utils.DeletionThread;
import integratedtoolkit.connectors.utils.Operations;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import integratedtoolkit.types.ResourceCreationRequest;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.ShutdownListener;

public abstract class AbstractConnector implements Connector, Operations, Cost {

    private float currentCostPerHour;
    private final float deletedMachinesCost;
    private long meanCreationTime;
    private int createdVMs;

    protected String providerName;
    private final ConcurrentHashMap<String, VM> IPToVM;
    private final ConcurrentHashMap<Object, Long> powerOnVMTimestamp;
    private boolean terminate = false;
    private boolean check = false;
    private final DeadlineThread dead;
    private final TreeSet<VM> vmsToDelete;
    private final LinkedList<VM> vmsAlive;

    public static final long ONE_HOUR = 3600000;
    public static final long FIFTY_EIGHT_MIN = 3480000;
    public static final long FIFTY_MIN = 3000000;
    public static final long FIVE_MIN = 300000;
    public static final long TWO_MIN = 120000;
    public static final long TEN_MIN = 600000;
    public static final long ONE_MIN = 60000;
    public static final long HALF_MIN = 30000;

    protected static long INITIAL_CREATION_TIME = TWO_MIN;
    protected static long MINIM_DEADLINE_INTERVAL = TWO_MIN;
    protected static long DELETE_SAFETY_INTERVAL = HALF_MIN;

    public AbstractConnector(String providerName, HashMap<String, String> props) {
        this.providerName = providerName;
        IPToVM = new ConcurrentHashMap<String, VM>();
        powerOnVMTimestamp = new ConcurrentHashMap<Object, Long>();
        vmsToDelete = new TreeSet<VM>();
        vmsAlive = new LinkedList<VM>();
        //ipToConnection = Collections.synchronizedMap(new HashMap<String, Connection>());
        String creationTimeStr = props.get("estimated-creation-time");
        if (creationTimeStr != null) {
            meanCreationTime = Integer.parseInt(creationTimeStr) * 1000;
        } else {
            meanCreationTime = INITIAL_CREATION_TIME;
        }

        logger.debug("Initial mean creation time is" + meanCreationTime);
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
     *      Connector interface
     * 
     */
    public boolean turnON(String name, ResourceCreationRequest rR) {
        if (terminate) {
            return false;
        }
        logger.info("Requesting a resource creation " + name + " : " + rR.getRequested().toString());
        // Check if we can reuse one of the vms put to delete (but not yet destroyed)
        VM vmInfo = tryToReuseVM(rR.getRequested());
        if (vmInfo != null) {
            logger.info("Reusing VM: " + vmInfo);
            CreationThread ct = new CreationThread((Operations) this, vmInfo.getName(), providerName, rR, vmInfo);
            ct.start();
            return true;
        }
        try {
            CreationThread ct = new CreationThread((Operations) this, name, providerName, rR, null);
            ct.start();
        } catch (Exception e) {
            logger.info("ResourceRequest failed");
            return false;
        }
        return true;
    }

    private synchronized VM tryToReuseVM(CloudMethodResourceDescription requested) {
        String imageReq = requested.getImage().getImageName();
        VM reusedVM = null;
        for (VM vm : vmsToDelete) {
            if (!vm.rd.getImage().getImageName().equals(imageReq)) {
                continue;
            }

            if (!vm.rd.contains(requested)) {
                continue;
            }

            reusedVM = vm;
            reusedVM.setToDelete(false);
            vmsToDelete.remove(reusedVM);
            break;
        }

        return reusedVM;
    }

    public void stopReached() {
        check = true;
    }

    public Long getNextCreationTime()
            throws ConnectorException {
        return meanCreationTime;
    }

    public void terminate(CloudMethodWorker worker, CloudMethodResourceDescription reduction) {
        (new DeletionThread(this, worker, reduction)).start();
    }

    @Override
    public void terminateAll() {
        terminate = true;
        dead.terminate();

        for (VM vm : IPToVM.values()) {

        	logger.info("Retrieving data from VM " + vm.getName());
            vm.getWorker().retrieveData(false);
            logger.info("Destroying VM " + vm.getName());
            Semaphore sem = new Semaphore(0);
            ShutdownListener sl = new ShutdownListener(sem);
            vm.getWorker().stop(sl);
            sl.enable();
            try {
                sem.acquire();
            } catch (Exception e) {
                logger.error("ERROR: Exception raised on worker shutdown");
            }
            try {
                destroy(vm.getEnvId());
            } catch (Exception e) {
                logger.error("ERROR: Exception while trying to destroy the virtual machine " + vm.getName(), e);
            }
        }

        synchronized (this) {
            IPToVM.clear();
            vmsToDelete.clear();
            vmsAlive.clear();
        }
        this.close();

    }

    protected abstract void close();

    @Override
    public Object poweron(String name, CloudMethodResourceDescription rd)
            throws ConnectorException {
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
        vm.requestTime = powerOnVMTimestamp.remove(envId);
        logger.info("Virtual machine created: " + vm);
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
    public abstract CloudMethodResourceDescription waitUntilCreation(Object endId,
            CloudMethodResourceDescription requested) throws ConnectorException;

    @Override
    public void vmReady(VM vmInfo) throws ConnectorException {
        synchronized (this) {
            vmInfo.startTime = System.currentTimeMillis();
            vmInfo.computeCreationTime();
            long totaltime = meanCreationTime * createdVMs;
            totaltime += vmInfo.creationTime;
            createdVMs++;
            meanCreationTime = totaltime / createdVMs;
            logger.debug("New mean creation time :" + meanCreationTime);
        }

    }

    public VM pause(CloudMethodWorker worker) {
        String ip = worker.getName();
        VM vmInfo = IPToVM.get(ip);
        if (canBeSaved(vmInfo)) {
            logger.info("Virtual machine saved: " + vmInfo);
            vmInfo.setToDelete(true);
            synchronized (this) {
                vmsToDelete.add(vmInfo);
            }
            return null;
        }
        return vmInfo;
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
        IPToVM.put(ip, vmInfo);
        vmsAlive.add(vmInfo);
    }

    private synchronized void removeMachine(VM vmInfo) {
        IPToVM.remove(vmInfo.getName());
        vmsAlive.remove(vmInfo);
    }

    public Float getTotalCost() {
        float aliveMachinesCost = 0;
        synchronized (this) {
            long now = System.currentTimeMillis();
            for (VM vm : vmsAlive) {
                long numSlots = getNumSlots(now, vm.getStartTime());
                float pricePerSlot = getMachineCostPerTimeSlot(vm.rd);
                aliveMachinesCost += numSlots * pricePerSlot;
            }
        }
        float totalCost = aliveMachinesCost + deletedMachinesCost;
        return totalCost;
    }

    public abstract float getMachineCostPerTimeSlot(CloudMethodResourceDescription rd);

    public abstract long getTimeSlot();

    public Float currentCostPerHour() {
        return currentCostPerHour;
    }

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

    private class DeadlineThread extends Thread {

        private boolean keepGoing;

        public DeadlineThread() {
            keepGoing = true;
        }

        public void run() {
            Thread.currentThread().setName("Connector " + providerName + " deadline");

            long sleepTime = 1000l;
            while (keepGoing) {
                try {
                    logger.debug("Deadline thread sleeps " + sleepTime + " ms.");
                    Thread.sleep(sleepTime);
                } catch (Exception e) {
                }
                synchronized (AbstractConnector.this) {
                    if (vmsAlive.isEmpty()) {
                        //logger.info("MONITORSTATUS DEAD no VMs alive");
                        long slTime = getSleepTime();
                        if (meanCreationTime > slTime) {
                            sleepTime = meanCreationTime;
                        } else {
                            sleepTime = slTime;
                        }
                        logger.debug("No VMs alive deadline sleep set to " + sleepTime + " ms.");
                        continue;
                    } else {
                        sleepTime = getSleepTime();
                        logger.debug("VMs alive initial sleep set to " + sleepTime + " ms.");
                        Iterator<VM> vms = vmsAlive.iterator();
                        while (vms.hasNext()) {
                            VM vmInfo = vms.next();
                            long timeLeft = timeLeft(vmInfo.getStartTime());
                            //logger.info("MONITOR STATUS DEAD next VM " + vmInfo.ip + " @ " + vmInfo.startTime + " --> " + timeLeft);
                            if (timeLeft < DELETE_SAFETY_INTERVAL) {

                                if (vmInfo.isToDelete()) {
                                    logger.info("Deleting vm " + vmInfo.getName() + " because is marked to delete and it is on the safety delete interval");
                                    vmsAlive.pollFirst();
                                    vmsToDelete.remove(vmInfo); //vmsToDelete.pollLast();
                                    DeletionThread dt;
                                    dt = new DeletionThread((Operations) AbstractConnector.this, vmInfo);
                                    dt.start();
                                }
                            } else {
                                if (sleepTime > timeLeft - DELETE_SAFETY_INTERVAL) {
                                    sleepTime = timeLeft - DELETE_SAFETY_INTERVAL;
                                    logger.debug("Reseting sleep time because a interval near to finish " + sleepTime + " ms.");
                                }

                            }
                        }
                    }
                }
            }
        }

        private long getSleepTime() {
            long time = getTimeSlot();
            if (time <= 0) {
                return MINIM_DEADLINE_INTERVAL - DELETE_SAFETY_INTERVAL;
            } else {
                return time - DELETE_SAFETY_INTERVAL;
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
            //logger.info("MONITOR STATUS DEAD Started at "+time+" now is "+now+" remaining --> "+(now - time)+" " +result+" ms to deadline");
            return result;
        }

    }

    private class Ender extends Thread {

        final AbstractConnector ac;

        public Ender(AbstractConnector ac) {
            this.ac = ac;
        }

        public void run() {
            for (VM vm : IPToVM.values()) {
                try {
                    logger.info("Destroying VM " + vm.getName());
                    ac.destroy(vm.getEnvId());

                } catch (Exception e) {
                    logger.info("Error while trying to  the virtual machine " + vm.getName());
                } finally {
                    ac.close();
                }
            }
        }
    }
}
