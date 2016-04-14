package integratedtoolkit.types.resources;

import integratedtoolkit.types.AdaptorDescription;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Implementation.Type;
import integratedtoolkit.types.annotations.Constraints;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import org.w3c.dom.Node;


public class MethodResourceDescription extends WorkerResourceDescription {

    public static final MethodResourceDescription EMPTY = new MethodResourceDescription();
    private static final String UNASSIGNED = "[unassigned]";

    //Capabilities
    protected List<String> hostQueue;
    protected String processorArchitecture = UNASSIGNED;
    protected int processorCPUCount = 0;
    protected int processorCoreCount = 0;
    protected float processorSpeed = 0;

    protected float memoryPhysicalSize = 0;
    protected float memoryVirtualSize = 0;
    protected float memoryAccessTime = 0;
    protected float memorySTR = 0;

    protected float storageElemSize = 0;
    protected float storageElemAccessTime = 0;
    protected float storageElemSTR = 0;

    protected String operatingSystemType = UNASSIGNED;

    protected List<String> appSoftware;

    protected int wallClockLimit = 0;
    
    //Extra fields
    protected int slots = 0;
    protected Float value = 0.0f;
    
    //Adaptors Info
    protected TreeMap<String, AdaptorDescription> adaptorsDesc;
    
    // Tags for key-value string constraints description
    public static final String PROC_ARCH = "ProcessorArch";
    public static final String PROC_CPU_COUNT = "ProcessorCPUCount";
    public static final String PROC_CORE_COUNT = "ProcessorCoreCount";
    public static final String PROC_SPEED = "ProcessorSpeed";
    public static final String MEM_PHYSICAL_SIZE = "MemoryPhysicalSize";
    public static final String MEM_VIRTUAL_SIZE = "MemoryVirtualSize";
    public static final String MEM_ACCESS_TIME = "MemoryAccessTime";
    public static final String MEM_STR = "MemorySTR";
    public static final String STORAGE_ELEM_SIZE = "StorageElemSize";
    public static final String STORAGE_ACCESS_TIME = "StorageElemAccessTime";
    public static final String STORAGE_ELEM_STR = "StorageElemSTR";
    public static final String OS_TYPE = "OperatingSystemType";
    public static final String HOST_QUEUE = "HostQueue";
    public static final String APP_SOFTWARE = "AppSoftware";
    public static final String WALL_CLOCK_LIMIT = "WallClockLimit";
    

    public MethodResourceDescription() {
        super();
        hostQueue = new LinkedList<String>();
        appSoftware = new LinkedList<String>();
    }

    public MethodResourceDescription(Constraints constraints) {
        hostQueue = new LinkedList<String>();
        String queueString = constraints.hostQueue();
        if (queueString.compareTo(UNASSIGNED) != 0) {
            for (String value : queueString.split(",")){
                hostQueue.add(value.trim());
            } 
        }

        processorArchitecture = constraints.processorArchitecture();
        processorCPUCount = constraints.processorCPUCount();
        processorCoreCount = constraints.processorCoreCount();
        processorSpeed = constraints.processorSpeed();

        memoryPhysicalSize = constraints.memoryPhysicalSize();
        memoryVirtualSize = constraints.memoryVirtualSize();
        memoryAccessTime = constraints.memoryAccessTime();
        memorySTR = constraints.memorySTR();

        storageElemSize = constraints.storageElemSize();
        storageElemAccessTime = constraints.storageElemAccessTime();
        storageElemSTR = constraints.storageElemSTR();

        operatingSystemType = constraints.operatingSystemType();

        appSoftware = new LinkedList<String>();
        String software = constraints.appSoftware();

        if (software.compareTo(UNASSIGNED) != 0) {
            for (String value : software.split(",")){
                this.appSoftware.add(value.trim());
            } 
        }
        
        wallClockLimit = constraints.wallClockLimit();
        
    }
    
    public MethodResourceDescription(String description){
        String[] constraints = description.split(";");
        String key, val;

        hostQueue = new LinkedList<String>();
        appSoftware = new LinkedList<String>();
        for(String c : constraints){
            key = c.split(":")[0].trim();
            val = c.split(":")[1].trim();
            switch (key){
                case PROC_ARCH:
                    processorArchitecture = val;
                    break;
                case PROC_CPU_COUNT:
                    processorCPUCount = Integer.parseInt(val);
                    break;
                case PROC_CORE_COUNT:
                    processorCoreCount = Integer.parseInt(val);
                    break;
                case PROC_SPEED:
                    processorSpeed = Float.parseFloat(val);
                    break;
                case MEM_PHYSICAL_SIZE:
                    memoryPhysicalSize = Float.parseFloat(val);
                    break;
                case MEM_VIRTUAL_SIZE:
                    memoryVirtualSize = Float.parseFloat(val);
                    break;
                case MEM_ACCESS_TIME:
                    memoryAccessTime = Float.parseFloat(val);
                    break;
                case MEM_STR:
                    memorySTR = Float.parseFloat(val);
                    break;
                case STORAGE_ELEM_SIZE:
                    storageElemSize =Float.parseFloat(val);
                    break;
                case STORAGE_ACCESS_TIME:
                    storageElemAccessTime = Float.parseFloat(val);
                    break;
                case STORAGE_ELEM_STR:
                    storageElemSTR = Float.parseFloat(val);
                    break;
                case OS_TYPE:
                    operatingSystemType = val;
                    break;
                case HOST_QUEUE :
                    if (val.compareTo(UNASSIGNED) != 0) {
                        for (String host : val.split(",")){
                            this.hostQueue.add(host.trim());
                        } 
                    }
                    break;
                case APP_SOFTWARE:
                    if (val.compareTo(UNASSIGNED) != 0) {
                        for (String app : val.split(",")){
                            this.appSoftware.add(app.trim());
                        } 
                    }
                    break;
                case WALL_CLOCK_LIMIT:
                	wallClockLimit = Integer.parseInt(val);
                    break;
            }
        }
    }

    public MethodResourceDescription(MethodResourceDescription clone) {
        super(clone);

        processorArchitecture = clone.processorArchitecture;
        processorCPUCount = clone.processorCPUCount;
        processorCoreCount = clone.processorCoreCount;
        processorSpeed = clone.processorSpeed;

        memoryPhysicalSize = clone.memoryPhysicalSize;
        memoryVirtualSize = clone.memoryVirtualSize;
        memoryAccessTime = clone.memoryAccessTime;
        memorySTR = clone.memorySTR;

        storageElemSize = clone.storageElemSize;
        storageElemAccessTime = clone.storageElemAccessTime;
        storageElemSTR = clone.storageElemSTR;

        operatingSystemType = clone.operatingSystemType;

        hostQueue = new LinkedList<String>();
        for (int i = 0; i < clone.hostQueue.size(); i++) {
            hostQueue.add(clone.hostQueue.get(i));
        }
        appSoftware = new LinkedList<String>();
        for (int i = 0; i < clone.appSoftware.size(); i++) {
            appSoftware.add(clone.appSoftware.get(i));
        }
        wallClockLimit = clone.wallClockLimit;
        
        slots = clone.slots;
        value = clone.value;
    }

    public MethodResourceDescription(Node n) {
        hostQueue = new LinkedList<String>();
        appSoftware = new LinkedList<String>();
        for (int i = 0; i < n.getChildNodes().getLength(); i++) {
            Node capabilities = n.getChildNodes().item(i);
            if (("Capabilities").equals(capabilities.getNodeName())) {
                parseCapabilities(capabilities);
            } else if (("Price").equals(capabilities.getNodeName())) {
                value = Float.parseFloat(capabilities.getTextContent());
            }
        }
    }

    private void parseCapabilities(Node capabilities) {
        
        for (int j = 0; j < capabilities.getChildNodes().getLength(); j++) {
            if (("Host").equals(capabilities.getChildNodes().item(j).getNodeName())) {
                parseHost(capabilities.getChildNodes().item(j));
            } else if (("Processor").equals(capabilities.getChildNodes().item(j).getNodeName())) {
                parseProcessor(capabilities.getChildNodes().item(j));
            } else if (("OS").equals(capabilities.getChildNodes().item(j).getNodeName())) {
                parseOS(capabilities.getChildNodes().item(j));
            } else if (("StorageElement").equals(capabilities.getChildNodes().item(j).getNodeName())) {
                parseStorage(capabilities.getChildNodes().item(j));
            } else if (("Memory").equals(capabilities.getChildNodes().item(j).getNodeName())) {
                parseMemory(capabilities.getChildNodes().item(j));
            } else if (("ApplicationSoftware").equals(capabilities.getChildNodes().item(j).getNodeName())) {
                parseApps(capabilities.getChildNodes().item(j));
            }
        }
    }
    

    private void parseHost(Node host) {
        for (int k = 0; k < host.getChildNodes().getLength(); k++) {
            if (("Queue").equals(host.getChildNodes().item(k).getNodeName())) {
                hostQueue.add(host.getChildNodes().item(k).getTextContent());
            }
        }
    }

    private void parseProcessor(Node processor) {
        for (int k = 0; k < processor.getChildNodes().getLength(); k++) {
            if (("Architecture").equals(processor.getChildNodes().item(k).getNodeName())) {
                processorArchitecture = processor.getChildNodes().item(k).getTextContent();
            } else if (("CPUCount").equals(processor.getChildNodes().item(k).getNodeName())) {
                processorCPUCount = Integer.parseInt(processor.getChildNodes().item(k).getTextContent());
            } else if (("CoreCount").equals(processor.getChildNodes().item(k).getNodeName())) {
                processorCoreCount = Integer.parseInt(processor.getChildNodes().item(k).getTextContent());
                if (processorCPUCount == 0) {
                    processorCPUCount = 1;
                }
            } else if (("Speed").equals(processor.getChildNodes().item(k).getNodeName())) {
                processorSpeed = Float.parseFloat(processor.getChildNodes().item(k).getTextContent());
            }
        }
    }

    private void parseOS(Node os) {
        for (int k = 0; k < os.getChildNodes().getLength(); k++) {
            if (("OSType").equals(os.getChildNodes().item(k).getNodeName())) {
                operatingSystemType = os.getChildNodes().item(k).getTextContent();
            }
        }
    }

    private void parseStorage(Node storage) {
        for (int k = 0; k < storage.getChildNodes().getLength(); k++) {
            if (("Size").equals(storage.getChildNodes().item(k).getNodeName())) {
                storageElemSize = Float.parseFloat(storage.getChildNodes().item(k).getTextContent());
            } else if (("AccessTime").equals(storage.getChildNodes().item(k).getNodeName())) {
                storageElemAccessTime = Float.parseFloat(storage.getChildNodes().item(k).getTextContent());
            } else if (("STR").equals(storage.getChildNodes().item(k).getNodeName())) {
                storageElemSTR = Float.parseFloat(storage.getChildNodes().item(k).getTextContent());
            }
        }
    }

    private void parseMemory(Node memory) {
        for (int k = 0; k < memory.getChildNodes().getLength(); k++) {
            if (("PhysicalSize").equals(memory.getChildNodes().item(k).getNodeName())) {
                memoryPhysicalSize = Float.parseFloat(memory.getChildNodes().item(k).getTextContent());
            } else if (("VirtualSize").equals(memory.getChildNodes().item(k).getNodeName())) {
                memoryVirtualSize = Float.parseFloat(memory.getChildNodes().item(k).getTextContent());
            } else if (("AccessTime").equals(memory.getChildNodes().item(k).getNodeName())) {
                memoryAccessTime = Float.parseFloat(memory.getChildNodes().item(k).getTextContent());
            } else if (("STR").equals(memory.getChildNodes().item(k).getNodeName())) {
                memorySTR = Float.parseFloat(memory.getChildNodes().item(k).getTextContent());
            }
        }
    }

    private void parseApps(Node apps) {
        for (int k = 0; k < apps.getChildNodes().getLength(); k++) {
            if (!("#text").equals(apps.getChildNodes().item(k).getNodeName())) {
                for (int app = 0; app < apps.getChildNodes().getLength(); app++) {
                    appSoftware.add(apps.getChildNodes().item(app).getTextContent());
                }
            }
        }
    }

    public int getSlots() {
        return slots;
    }

    public void setSlots(int qty) {
        slots = qty;
    }

    public void addSlot() {
        slots++;
    }

    //Processor getters and setters
    public String getProcessorArchitecture() {
        return processorArchitecture;
    }

    public void setProcessorArchitecture(String architecture) {
        processorArchitecture = architecture;
    }

    public int getProcessorCPUCount() {
        return processorCPUCount;
    }

    public void setProcessorCPUCount(int count) {
        processorCPUCount = count;
    }

    public int getProcessorCoreCount() {
        return processorCoreCount;
    }

    public void setProcessorCoreCount(int count) {
        processorCoreCount = count;
    }

    public float getProcessorSpeed() {
        return processorSpeed;
    }

    public void setProcessorSpeed(float ghz) {
        processorSpeed = ghz;
    }

    //Memory getters and setters   
    public float getMemoryPhysicalSize() {
        return memoryPhysicalSize;
    }

    public void setMemoryPhysicalSize(float gb) {
        memoryPhysicalSize = gb;
    }

    public float getMemoryVirtualSize() {
        return memoryVirtualSize;
    }

    public void setMemoryVirtualSize(float gb) {
        memoryVirtualSize = gb;
    }

    public float getMemoryAccessTime() {
        return memoryAccessTime;
    }

    public void setMemoryAccessTime(float ns) {
        memoryAccessTime = ns;
    }

    public float getMemorySTR() {
        return memorySTR;
    }

    public void setMemorySTR(float gBs) {
        memorySTR = gBs;
    }

    //Storage getters and setters
    public float getStorageElemSize() {
        return storageElemSize;
    }

    public void setStorageElemSize(float gb) {
        storageElemSize = gb;
    }

    public float getStorageElemAccessTime() {
        return storageElemAccessTime;
    }

    public void setStorageElemAccessTime(float ms) {
        storageElemAccessTime = ms;
    }

    public float getStorageElemSTR() {
        return storageElemSTR;
    }

    public void setStorageElemSTR(float mbs) {
        storageElemSTR = mbs;
    }

    //Host queue getters and setters
    public List<String> getHostQueue() {
        return hostQueue;
    }

    public void setHostQueue(List<String> hostQueue) {
        this.hostQueue = new LinkedList<String>(hostQueue);
    }

    public void addHostQueue(String queue) {
        this.hostQueue.add(queue);
    }

    //Operating System Type getters and setters
    public String getOperatingSystemType() {
        return operatingSystemType;
    }

    public void setOperatingSystemType(String osType) {
        operatingSystemType = osType;
    }

    //App Software getters and setters
    public List<String> getAppSoftware() {
        return appSoftware;
    }

    public void setAppSoftware(List<String> appSoftware) {
        this.appSoftware = new LinkedList<String>(appSoftware);
    }

    public void resetAppSoftware() {
        appSoftware = new LinkedList<String>();
    }

    public void addAppSoftware(String software) {
        appSoftware.add(software);
    }
    
    public long getWallClockLimit(){
    	return wallClockLimit;
    }
    
    public void setWallClockLimit(int wcl){
    	this.wallClockLimit = wcl;
    }

    public Float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }

    public void join(MethodResourceDescription mr2) {
        this.processorCPUCount = java.lang.Math.max(this.processorCPUCount, mr2.processorCPUCount);
        this.processorCoreCount = java.lang.Math.max(this.processorCoreCount, mr2.processorCoreCount);
        this.processorSpeed = java.lang.Math.max(this.processorSpeed, mr2.processorSpeed);

        this.memoryPhysicalSize = java.lang.Math.max(this.memoryPhysicalSize, mr2.memoryPhysicalSize);
        this.memoryVirtualSize = java.lang.Math.max(this.memoryVirtualSize, mr2.memoryVirtualSize);
        this.memoryAccessTime = java.lang.Math.max(this.memoryAccessTime, mr2.memoryAccessTime);
        this.memorySTR = java.lang.Math.max(this.memorySTR, mr2.memorySTR);

        this.storageElemSize = java.lang.Math.max(this.storageElemSize, mr2.storageElemSize);
        this.storageElemAccessTime = java.lang.Math.max(this.storageElemAccessTime, mr2.storageElemAccessTime);
        this.storageElemSTR = java.lang.Math.max(this.storageElemSTR, mr2.storageElemSTR);

        if (this.processorArchitecture.compareTo(UNASSIGNED) == 0) {
            this.processorArchitecture = mr2.processorArchitecture;
        }
        if (this.operatingSystemType.compareTo(UNASSIGNED) == 0) {
            this.operatingSystemType = mr2.operatingSystemType;
        }

        for (int i = 0; i < mr2.hostQueue.size(); i++) {
            if (!this.hostQueue.contains(mr2.hostQueue.get(i))) {
                this.hostQueue.add(mr2.hostQueue.get(i));
            }
        }

        for (int i = 0; i < mr2.appSoftware.size(); i++) {
            if (!this.appSoftware.contains(mr2.appSoftware.get(i))) {
                this.appSoftware.add(mr2.appSoftware.get(i));
            }
        }

        this.slots += mr2.slots;
        
        wallClockLimit = java.lang.Math.max(this.wallClockLimit, mr2.wallClockLimit);
    }

    public float difference(MethodResourceDescription mr2) {
        int weight = 10000;
        float processorDif = this.processorCoreCount - mr2.processorCoreCount;
        float memoryDif = this.memoryPhysicalSize - mr2.memoryPhysicalSize;
        return processorDif * weight + memoryDif;
    }

    public boolean contains(MethodResourceDescription rc2) {
        boolean contained = checkCompatibility(this.operatingSystemType, rc2.operatingSystemType);
        contained = contained && this.hostQueue.containsAll(rc2.hostQueue);
        contained = contained && this.appSoftware.containsAll(rc2.appSoftware);
        contained = contained && checkProcessor(rc2);
        contained = contained && checkMemory(rc2);
        contained = contained && checkStorage(rc2);
        return contained;
    }

    private boolean checkProcessor(MethodResourceDescription rc2) {
        return checkCompatibility(this.processorArchitecture, rc2.processorArchitecture)
                && checkInclusion(this.processorCoreCount, rc2.processorCoreCount)
                && checkInclusion(this.processorCPUCount, rc2.processorCPUCount)
                && checkInclusion(this.processorSpeed, rc2.processorSpeed);
    }

    private boolean checkMemory(MethodResourceDescription rc2) {
        return checkInclusion(this.memoryPhysicalSize, rc2.memoryPhysicalSize)
                && checkInclusion(this.memoryVirtualSize, rc2.memoryVirtualSize)
                && checkInclusion(this.memorySTR, rc2.memorySTR)
                && checkInclusion(rc2.memoryAccessTime, this.memoryAccessTime);
    }

    private boolean checkStorage(MethodResourceDescription rc2) {
        return checkInclusion(rc2.storageElemAccessTime, this.storageElemAccessTime)
                && checkInclusion(this.storageElemSTR, rc2.storageElemSTR)
                && checkInclusion(this.storageElemSize, rc2.storageElemSize);
    }

    private boolean checkCompatibility(String value1, String value2) {
        return value1.equals(value2) || value1.equals(UNASSIGNED) || value2.equals(UNASSIGNED);
    }

    private boolean checkInclusion(int value1, int value2) {
        return value1 >= value2 || value1 == 0 || value2 == 0;
    }

    private boolean checkInclusion(float value1, float value2) {
        return value1 >= value2 || value1 == 0 || value2 == 0;
    }

    public Integer canHostSimultaneously(MethodResourceDescription rc2) {
        float min = Float.MAX_VALUE;
        float ratio;
        if (rc2.processorCoreCount != 0) {
            ratio = this.processorCoreCount / (float) rc2.processorCoreCount;
            min = ratio;
        }
        if (rc2.processorCPUCount != 0) {
            ratio = this.processorCPUCount / (float) rc2.processorCPUCount;
            min = ratio;
        }
        if (rc2.memoryPhysicalSize != 0.0f) {
            ratio = this.memoryPhysicalSize / rc2.memoryPhysicalSize;
            min = Math.min(min, ratio);
        }
        if (rc2.memoryVirtualSize != 0.0f) {
            ratio = this.memoryVirtualSize / rc2.memoryVirtualSize;
            min = Math.min(min, ratio);
        }
        if (rc2.storageElemSize != 0.0f) {
            ratio = this.storageElemSize / rc2.storageElemSize;
            min = Math.min(min, ratio);
        }
        return (int) min;
    }

    public MethodResourceDescription multiply(int amount) {
        MethodResourceDescription rd = new MethodResourceDescription();
        rd.processorCoreCount = this.processorCoreCount * amount;
        rd.processorCPUCount = this.processorCPUCount * amount;
        rd.memoryPhysicalSize = this.memoryPhysicalSize * amount;
        rd.memoryVirtualSize = this.memoryVirtualSize * amount;
        rd.storageElemSize = this.storageElemSize * amount;
        return rd;
    }

    public void increase(MethodResourceDescription rd) {
        this.processorCPUCount += rd.processorCPUCount;
        this.processorCoreCount += rd.processorCoreCount;
        this.memoryPhysicalSize += rd.memoryPhysicalSize;
        this.memoryVirtualSize += rd.memoryVirtualSize;
        this.storageElemSize += rd.storageElemSize;
    }

    public void reduce(MethodResourceDescription rd) {
        this.processorCPUCount -= rd.processorCPUCount;
        this.processorCoreCount -= rd.processorCoreCount;
        this.memoryPhysicalSize -= rd.memoryPhysicalSize;
        this.memoryVirtualSize -= rd.memoryVirtualSize;
        this.storageElemSize -= rd.storageElemSize;
    }

    @Override
    public boolean canHost(Implementation<?> impl) {
        if (impl.getType() == Type.METHOD) {
            MethodResourceDescription wd = (MethodResourceDescription) impl.getRequirements();
            return this.contains(wd);
        }
        return false;

    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[FEATURES");
        sb.append(" [PROCESSOR");
        sb.append(" ARCHITECTURE=").append(processorArchitecture);
        sb.append(" CPUS=").append(processorCPUCount);
        sb.append(" CORES=").append(processorCoreCount);
        sb.append(" SPEED=").append(processorSpeed);
        sb.append("]");

        sb.append(" [MEMORY");
        sb.append(" PHYSICAL=").append(memoryPhysicalSize);
        sb.append(" VIRTUAL=").append(memoryVirtualSize);
        sb.append(" ACCESS_TIME=").append(memoryAccessTime);
        sb.append(" STR=").append(memorySTR);
        sb.append("]");

        sb.append(" [STORAGE");
        sb.append(" SIZE=").append(storageElemSize);
        sb.append(" ACCESS_TIME=").append(storageElemAccessTime);
        sb.append(" STR=").append(storageElemSTR);
        sb.append("]");

        sb.append(" [OS ");
        sb.append(operatingSystemType);
        sb.append("]");

        sb.append(" [QUEUES ");
        for (String queue : hostQueue) {
            sb.append(queue).append(" ");
        }

        sb.append(" [SOFTWARE ");
        for (String app : appSoftware) {
            sb.append(app).append(" ");
        }
        sb.append("]");
        sb.append("]");

        return sb.toString();
    }
}
