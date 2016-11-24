package integratedtoolkit.connectors;

import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;


/**
 * Representation of a VM
 *
 */
public class VM implements Comparable<VM> {

    private final Object envId;
    private final CloudMethodResourceDescription rd;

    private COMPSsWorker node;
    private CloudMethodWorker worker;

    private long requestTime;
    private long startTime;
    private long creationTime;

    private boolean toDelete;


    /**
     * New VM object with envId @envId and description @description
     * 
     * @param envId
     * @param description
     */
    public VM(Object envId, CloudMethodResourceDescription description) {
        this.envId = envId;
        this.rd = description;
        this.requestTime = System.currentTimeMillis();
        this.startTime = 0;
        this.creationTime = 0;
        this.toDelete = false;
    }

    /**
     * Returns the envId
     * 
     * @return
     */
    public Object getEnvId() {
        return envId;
    }

    /**
     * Returns the name
     * 
     * @return
     */
    public String getName() {
        return rd.getName();
    }

    /**
     * Returns the VM description
     * 
     * @return
     */
    public CloudMethodResourceDescription getDescription() {
        return this.rd;
    }

    /**
     * Returns the VM node
     * 
     * @return
     */
    public COMPSsWorker getNode() {
        return node;
    }

    /**
     * Returns the VM worker
     * 
     * @return
     */
    public CloudMethodWorker getWorker() {
        return worker;
    }

    /**
     * Returns the start time
     * 
     * @return
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Returns the creation time
     * 
     * @return
     */
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Returns the image name
     * 
     * @return
     */
    public String getImage() {
        return rd.getImage().getImageName();
    }

    /**
     * Returns if the VM is to delete or not
     * 
     * @return
     */
    public boolean isToDelete() {
        return toDelete;
    }

    /**
     * Computes the creation time (stored internally)
     * 
     */
    public void computeCreationTime() {
        creationTime = this.startTime - this.requestTime;
    }

    /**
     * Sets the start time
     * 
     * @param startTime
     */
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    /**
     * Sets the request time
     * 
     * @param requestTime
     */
    public void setRequestTime(long requestTime) {
        this.requestTime = requestTime;
    }

    /**
     * Sets the worker representing this VM
     * 
     * @param worker
     */
    public void setWorker(CloudMethodWorker worker) {
        this.worker = worker;
    }

    /**
     * Sets if the VM is to delete or not
     * 
     * @param toDelete
     */
    public void setToDelete(boolean toDelete) {
        this.toDelete = toDelete;
    }

    // Comparable interface implementation
    @Override
    public int compareTo(VM vm) throws NullPointerException {
        if (vm == null) {
            throw new NullPointerException();
        }

        if (vm.getName().equals(getName())) {
            return 0;
        }

        long now = System.currentTimeMillis();
        int mod1 = (int) (now - getStartTime()) % 3_600_000; // 1 h in ms
        int mod2 = (int) (now - vm.getStartTime()) % 3_600_000; // 1 h in ms

        return mod2 - mod1;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof VM) {
            VM vm = (VM) obj;
            return vm.getName().equals(getName());
        }
        
        return false;
    }
    
    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public String toString() {
        return "VM " + envId + " (ip = " + rd.getName() + ", request time = " + requestTime + ", start time = " + startTime
                + ", creation time = " + creationTime + ", image = " + rd.getImage().getImageName() + ", procs = CPU: "
                + rd.getTotalCPUComputingUnits() + ", GPU: " + rd.getTotalGPUComputingUnits() + ", FPGA: " + rd.getTotalFPGAComputingUnits()
                + ", OTHER: " + rd.getTotalOTHERComputingUnits() + ", memory = " + rd.getMemorySize() + ", disk = " + rd.getStorageSize()
                + ", to delete = " + toDelete + ")";
    }

}
