package integratedtoolkit.types.resources;

import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Implementation.Type;
import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.resources.components.Processor;

import java.util.LinkedList;
import java.util.List;

public class MethodResourceDescription extends WorkerResourceDescription {

    // Constant for weigth difference (dynamic increase/decrease)
    private static final int DIFFERENCE_WEIGHT = 10_000;

    // Empty Resource Description
    public static final MethodResourceDescription EMPTY_FOR_RESOURCE = new MethodResourceDescription();
    public static final MethodResourceDescription EMPTY_FOR_CONSTRAINTS = new MethodResourceDescription(ONE_INT);

    /* Tags for key-value string constraints description *****************/
    // !!!!!!!!!! WARNING: Coherent with constraints class
    public static final String PROC_NAME = "ProcessorName";
    public static final String COMPUTING_UNITS = "ComputingUnits";
    public static final String PROC_SPEED = "ProcessorSpeed";
    public static final String PROC_ARCH = "ProcessorArchitecture";
    public static final String PROC_PROP_NAME = "ProcessorPropertyName";
    public static final String PROC_PROP_VALUE = "ProcessorPropertyValue";
    public static final String MEM_SIZE = "MemorySize";
    public static final String MEM_TYPE = "MemoryType";
    public static final String STORAGE_SIZE = "StorageSize";
    public static final String STORAGE_TYPE = "StorageType";
    public static final String OS_TYPE = "OperatingSystemType";
    public static final String OS_DISTRIBUTION = "OperatingSystemDistribution";
    public static final String OS_VERSION = "OperatingSystemVersion";
    public static final String APP_SOFTWARE = "AppSoftware";
    public static final String HOST_QUEUES = "HostQueues";
    public static final String WALL_CLOCK_LIMIT = "WallClockLimit";

    /* Resource Description properties ***********************************/
    // Processor
    protected List<Processor> processors = new LinkedList<Processor>();
    protected int totalComputingUnits = ZERO_INT;

    // Memory
    protected float memorySize = UNASSIGNED_FLOAT;
    protected String memoryType = UNASSIGNED_STR;
    // Storage
    protected float storageSize = UNASSIGNED_FLOAT;
    protected String storageType = UNASSIGNED_STR;
    // Operating System
    protected String operatingSystemType = UNASSIGNED_STR;
    protected String operatingSystemDistribution = UNASSIGNED_STR;
    protected String operatingSystemVersion = UNASSIGNED_STR;
    // Applications
    protected List<String> appSoftware = new LinkedList<String>();
    // Host queues
    protected List<String> hostQueues = new LinkedList<String>();
    // Price
    protected int priceTimeUnit = UNASSIGNED_INT;
    protected float pricePerUnit = UNASSIGNED_FLOAT;
    // WallClock Limit (from constraints, not from XML files)
    protected int wallClockLimit = UNASSIGNED_INT;

    /* Internal fields ***************************************************/
    protected Float value = 0.0f;

    /* *******************************************
     * CONSTRUCTORS
     * *******************************************/
    public MethodResourceDescription() {
        super();
    }

    private MethodResourceDescription(int initialCUs) {
        super();

        // Add processor structures (with an empty non assigned processor)
        Processor p = new Processor();
        p.setComputingUnits(initialCUs);
        this.addProcessor(p);	// Increases the total CUs
    }

    public MethodResourceDescription(Constraints constraints) {
        super();

        // Parse processors - When comming from Constraints only one processor is available
        Processor p = new Processor();
        String procName = constraints.processorName();
        if (procName != null && !procName.equals(UNASSIGNED_STR)) {
            p.setName(procName);
        }
        int cu = constraints.computingUnits();
        if (cu > ONE_INT) {
            p.setComputingUnits(cu);
            totalComputingUnits = cu;
        } else {
            // When loading from constraints, always use at least one computing unit
            p.setComputingUnits(ONE_INT);
            totalComputingUnits = ONE_INT;
        }
        float speed = constraints.processorSpeed();
        if (speed != UNASSIGNED_FLOAT) {
            p.setSpeed(speed);
        }
        String arch = constraints.processorArchitecture();
        if (arch != null && !arch.equals(UNASSIGNED_STR)) {
            p.setArchitecture(arch);
        }
        String propName = constraints.processorPropertyName();
        if (propName != null && !propName.equals(UNASSIGNED_STR)) {
            p.setPropName(propName);
        }
        String propvalue = constraints.processorPropertyValue();
        if (propvalue != null && !propvalue.equals(UNASSIGNED_STR)) {
            p.setPropValue(propvalue);
        }
        this.processors.add(p);

        // Parse software
        String software = constraints.appSoftware();
        if (software != null && !software.equals(UNASSIGNED_STR)) {
            for (String value : software.split(",")) {
                this.appSoftware.add(value.trim().toUpperCase());
            }
        }

        // Parse queues
        String queues = constraints.hostQueues();
        if (queues != null && !queues.equals(UNASSIGNED_STR)) {
            for (String value : queues.split(",")) {
                this.hostQueues.add(value.trim().toUpperCase());
            }
        }

        // Parse memory, storage and OS constraints
        float memorySize = constraints.memorySize();
        if (memorySize != UNASSIGNED_FLOAT) {
            this.memorySize = memorySize;
        }
        String memoryType = constraints.memoryType();
        if (memoryType != null && !memoryType.equals(UNASSIGNED_STR)) {
            this.memoryType = memoryType;
        }

        float storageSize = constraints.storageSize();
        if (storageSize != UNASSIGNED_FLOAT) {
            this.storageSize = storageSize;
        }
        String storageType = constraints.storageType();
        if (storageType != null && !storageType.equals(UNASSIGNED_STR)) {
            this.storageType = storageType;
        }

        String operatingSystemType = constraints.operatingSystemType();
        if (operatingSystemType != null && !operatingSystemType.equals(UNASSIGNED_STR)) {
            this.operatingSystemType = operatingSystemType;
        }
        String operatingSystemDistribution = constraints.operatingSystemDistribution();
        if (operatingSystemDistribution != null && !operatingSystemDistribution.equals(UNASSIGNED_STR)) {
            this.operatingSystemDistribution = operatingSystemDistribution;
        }
        String operatingSystemVersion = constraints.operatingSystemVersion();
        if (operatingSystemVersion != null && !operatingSystemVersion.equals(UNASSIGNED_STR)) {
            this.operatingSystemVersion = operatingSystemVersion;
        }

        int wallClockLimit = constraints.wallClockLimit();
        if (wallClockLimit != UNASSIGNED_INT) {
            this.wallClockLimit = wallClockLimit;
        }

        // Prices don't come from constraints
    }

    public MethodResourceDescription(String description) {
        super();

        // Warning: When comming from constrains, only 1 PROCESSOR is available with at least 1 CU
        Processor proc = new Processor();
        proc.setComputingUnits(ONE_INT);

        String[] constraints = description.split(";");
        for (String c : constraints) {
            String key = c.split(":")[0].trim();
            String val = c.split(":")[1].trim();
            addConstraints(key, val, proc);
        }
        // Add the information retrieved from the processor constraints
        this.addProcessor(proc);	// Increases the totalCUs
    }

    public MethodResourceDescription(String[] constraints) {
        super();

        // Warning: When comming from constrains, only 1 PROCESSOR is available with at least 1 CU
        Processor proc = new Processor();
        proc.setComputingUnits(ONE_INT);
        for (String c : constraints) {
            String key = c.split("=")[0].trim();
            String val = c.split("=")[1].trim();
            addConstraints(key, val, proc);
        }
        // Add the information retrieved from the processor constraints
        this.addProcessor(proc);	// Increases the totalCUs
    }

    private void addConstraints(String key, String val, Processor proc) {
        if (val != null && !val.isEmpty()) {
            switch (key) {
                case PROC_NAME:
                    proc.setName(val);
                    break;
                case PROC_SPEED:
                    proc.setSpeed(Float.valueOf(val));
                    break;
                case PROC_ARCH:
                    proc.setArchitecture(val);
                    break;
                case PROC_PROP_NAME:
                    proc.setPropName(val);
                    break;
                case PROC_PROP_VALUE:
                    proc.setPropValue(val);
                    break;
                case COMPUTING_UNITS:
                    proc.setComputingUnits(Integer.valueOf(val));
                    break;
                case MEM_SIZE:
                    this.memorySize = Float.valueOf(val);
                    break;
                case MEM_TYPE:
                    this.memoryType = val;
                    break;
                case STORAGE_SIZE:
                    this.storageSize = Float.valueOf(val);
                    break;
                case STORAGE_TYPE:
                    this.storageType = val;
                    break;
                case OS_TYPE:
                    this.operatingSystemType = val;
                    break;
                case OS_DISTRIBUTION:
                    this.operatingSystemDistribution = val;
                    break;
                case OS_VERSION:
                    this.operatingSystemVersion = val;
                    break;
                case APP_SOFTWARE:
                    if (val.compareTo(UNASSIGNED_STR) != 0) {
                        for (String app : val.split(",")) {
                            this.appSoftware.add(app.trim().toUpperCase());
                        }
                    }
                    break;
                case HOST_QUEUES:
                    if (val.compareTo(UNASSIGNED_STR) != 0) {
                        for (String app : val.split(",")) {
                            this.hostQueues.add(app.trim().toUpperCase());
                        }
                    }
                    break;
                case WALL_CLOCK_LIMIT:
                    this.wallClockLimit = Integer.valueOf(val);
                    break;

            }
        }

    }

    public MethodResourceDescription(MethodResourceDescription clone) {
        super(clone);

        this.totalComputingUnits = 0;
        for (Processor p : clone.processors) {
            Processor newP = new Processor(p);
            this.addProcessor(newP);	// Increases totalCUs
        }

        this.memorySize = clone.memorySize;
        this.memoryType = clone.memoryType;

        this.storageSize = clone.storageSize;
        this.storageType = clone.storageType;

        this.operatingSystemType = clone.operatingSystemType;
        this.operatingSystemDistribution = clone.operatingSystemDistribution;
        this.operatingSystemVersion = clone.operatingSystemVersion;

        for (String app : clone.appSoftware) {
            this.appSoftware.add(app);
        }

        for (String queue : clone.hostQueues) {
            this.hostQueues.add(queue);
        }

        this.priceTimeUnit = clone.priceTimeUnit;
        this.pricePerUnit = clone.pricePerUnit;

        this.wallClockLimit = clone.wallClockLimit;

        this.value = clone.value;
    }

    @Override
    public MethodResourceDescription copy() {
        return new MethodResourceDescription(this);
    }

    /* *******************************************
     * GETTERS AND SETTERS
     * *******************************************/
    public List<Processor> getProcessors() {
        return processors;
    }

    public void setProcessors(List<Processor> processors) {
        this.processors = processors;

        this.totalComputingUnits = 0;
        for (Processor p : this.processors) {
            this.totalComputingUnits += p.getComputingUnits();
        }
    }

    public void resetProcessors() {
        this.processors = new LinkedList<Processor>();
        this.totalComputingUnits = 0;
    }

    public void addProcessor(Processor p) {
        this.processors.add(p);
        this.totalComputingUnits += p.getComputingUnits();
    }

    public void addProcessor(String procName, int computingUnits, String architecture, float speed, String propName, String propValue) {
        // This method is called from XML: empty and null values must be checked
        Processor p = new Processor();
        if (procName != null && !procName.isEmpty()) {
            p.setName(procName);
        }
        if (computingUnits > 0) {
            p.setComputingUnits(computingUnits);
        }
        if (architecture != null && !architecture.isEmpty()) {
            p.setArchitecture(architecture);
        }
        if (speed > (float) 0.0) {
            p.setSpeed(speed);
        }
        if (propName != null && !propName.isEmpty()) {
            p.setPropName(propName);
        }
        if (propValue != null && !propValue.isEmpty()) {
            p.setPropValue(propValue);
        }

        this.addProcessor(p);
    }

    public List<String> getArchitectures() {
        LinkedList<String> architectures = new LinkedList<String>();

        for (Processor p : this.processors) {
            String arch = p.getArchitecture();
            if (!arch.equals(UNASSIGNED_STR)) {
                architectures.add(arch);
            } else if (!architectures.contains(UNASSIGNED_STR)) {
                // Only add once
                architectures.add(UNASSIGNED_STR);
            }
        }

        return architectures;
    }

    public int getTotalComputingUnits() {
        return this.totalComputingUnits;
    }

    public float getMemorySize() {
        return memorySize;
    }

    public void setMemorySize(float memorySize) {
        if (memorySize > (float) 0.0) {
            this.memorySize = memorySize;
        }
    }

    public String getMemoryType() {
        return memoryType;
    }

    public void setMemoryType(String memoryType) {
        if (memoryType != null && !memoryType.isEmpty()) {
            this.memoryType = memoryType;
        }
    }

    public float getStorageSize() {
        return storageSize;
    }

    public void setStorageSize(float storageSize) {
        if (storageSize > 0) {
            this.storageSize = storageSize;
        }
    }

    public String getStorageType() {
        return storageType;
    }

    public void setStorageType(String storageType) {
        if (storageType != null && !storageType.isEmpty()) {
            this.storageType = storageType;
        }
    }

    public String getOperatingSystemType() {
        return operatingSystemType;
    }

    public void setOperatingSystemType(String operatingSystemType) {
        if (operatingSystemType != null && !operatingSystemType.isEmpty()) {
            this.operatingSystemType = operatingSystemType;
        }
    }

    public String getOperatingSystemDistribution() {
        return operatingSystemDistribution;
    }

    public void setOperatingSystemDistribution(String operatingSystemDistribution) {
        if (operatingSystemDistribution != null && !operatingSystemDistribution.isEmpty()) {
            this.operatingSystemDistribution = operatingSystemDistribution;
        }
    }

    public String getOperatingSystemVersion() {
        return operatingSystemVersion;
    }

    public void setOperatingSystemVersion(String operatingSystemVersion) {
        if (operatingSystemVersion != null && !operatingSystemVersion.isEmpty()) {
            this.operatingSystemVersion = operatingSystemVersion;
        }
    }

    public List<String> getAppSoftware() {
        return appSoftware;
    }

    public void setAppSoftware(List<String> appSoftware) {
        if (appSoftware != null) {
            this.appSoftware = appSoftware;
        }
    }

    public void resetAppSoftware() {
        appSoftware = new LinkedList<String>();
    }

    public void addApplication(String application) {
        if (application != null && !application.isEmpty()) {
            appSoftware.add(application.toUpperCase());
        }
    }

    public List<String> getHostQueues() {
        return hostQueues;
    }

    public void setHostQueues(List<String> hostQueues) {
        if (hostQueues != null) {
            this.hostQueues = hostQueues;
        }
    }

    public void resetHostQueues() {
        hostQueues = new LinkedList<String>();
    }

    public void addHostQueue(String queue) {
        if (queue != null && !queue.isEmpty()) {
            hostQueues.add(queue.toUpperCase());
        }
    }

    public int getPriceTimeUnit() {
        return priceTimeUnit;
    }

    public void setPriceTimeUnit(int priceTimeUnit) {
        if (priceTimeUnit > 0) {
            this.priceTimeUnit = priceTimeUnit;
        }
    }

    public float getPricePerUnit() {
        return pricePerUnit;
    }

    public void setPricePerUnit(float pricePerUnit) {
        if (priceTimeUnit > (float) 0.0) {
            this.pricePerUnit = pricePerUnit;
        }
    }

    public int getWallClockLimit() {
        return wallClockLimit;
    }

    public void setWallClockLimit(int wallClockLimit) {
        if (wallClockLimit > 0) {
            this.wallClockLimit = wallClockLimit;
        }
    }

    public Float getValue() {
        return value;
    }

    public void setValue(Float value) {
        this.value = value;
    }


    /* *******************************************
     * METHODRESOURCE OPERATIONS
     * *******************************************/
	// This method tries to substitute the implicit default values by defined mr2 values.
    // Keeps the already defined values (do NOT overwrite)
    // ONLY CALLED FROM CONSTRAINTS (1 processor always)
    public void merge(MethodResourceDescription mr2) {
        // Processor constraints
        Processor pthis = this.processors.get(0);
        Processor pmr2 = mr2.processors.get(0);
        if (pthis.getComputingUnits() <= ONE_INT) {
            int newCus = pmr2.getComputingUnits();
            pthis.setComputingUnits(newCus);
            this.totalComputingUnits = newCus;
        }
        if (pthis.getSpeed() == UNASSIGNED_FLOAT) {
            pthis.setSpeed(pmr2.getSpeed());
        }
        if (pthis.getName().equals(UNASSIGNED_STR)) {
            pthis.setName(pmr2.getName());
        }
        if (pthis.getArchitecture().equals(UNASSIGNED_STR)) {
            pthis.setArchitecture(pmr2.getArchitecture());
        }
        if (pthis.getPropName().equals(UNASSIGNED_STR)) {
            pthis.setPropName(pmr2.getPropName());
        }
        if (pthis.getPropValue().equals(UNASSIGNED_STR)) {
            pthis.setPropValue(pmr2.getPropValue());
        }

        // Memory
        if (this.memorySize == UNASSIGNED_FLOAT) {
            this.setMemorySize(mr2.getMemorySize());
        }
        if (this.memoryType.equals(UNASSIGNED_STR)) {
            this.setMemoryType(mr2.getMemoryType());
        }

        // Storage
        if (this.storageSize == UNASSIGNED_FLOAT) {
            this.setStorageSize(mr2.getStorageSize());
        }
        if (this.storageType.equals(UNASSIGNED_STR)) {
            this.setStorageType(mr2.getStorageType());
        }

        // Operating System
        if (this.operatingSystemType.equals(UNASSIGNED_STR)) {
            this.setOperatingSystemType(mr2.getOperatingSystemType());
        }
        if (this.operatingSystemDistribution.equals(UNASSIGNED_STR)) {
            this.setOperatingSystemDistribution(mr2.getOperatingSystemDistribution());
        }
        if (this.operatingSystemVersion.equals(UNASSIGNED_STR)) {
            this.setOperatingSystemVersion(mr2.getOperatingSystemVersion());
        }

        // Applications
        for (String app : mr2.appSoftware) {
            if (!this.appSoftware.contains(app)) {
                this.appSoftware.add(app);
            }
        }

        // Host queues
        for (String queue : mr2.hostQueues) {
            if (!this.hostQueues.contains(queue)) {
                this.hostQueues.add(queue);
            }
        }

        // Price
        if (this.pricePerUnit == UNASSIGNED_FLOAT) {
            if (mr2.pricePerUnit > (float) 0.0) {
                this.pricePerUnit = mr2.pricePerUnit;
            }
        }
        if (this.priceTimeUnit == UNASSIGNED_INT) {
            if (mr2.priceTimeUnit > 0) {
                this.priceTimeUnit = mr2.priceTimeUnit;
            }
        }

        // WallClock limit
        if (this.wallClockLimit == UNASSIGNED_INT) {
            this.setWallClockLimit(mr2.getWallClockLimit());
        }

        // Internal fields
        this.value = mr2.value;
    }

    public float difference(MethodResourceDescription mr2) {
        float processorDif = this.getTotalComputingUnits() - mr2.getTotalComputingUnits();
        float memoryDif = this.memorySize - mr2.memorySize;

        return processorDif * DIFFERENCE_WEIGHT + memoryDif;
    }

    public boolean contains(MethodResourceDescription rc2) {
        boolean contained = checkCompatibility(this.operatingSystemType, rc2.operatingSystemType);
        contained = contained && checkCompatibility(this.operatingSystemDistribution, rc2.operatingSystemDistribution);
        contained = contained && checkCompatibility(this.operatingSystemVersion, rc2.operatingSystemVersion);
        contained = contained && this.hostQueues.containsAll(rc2.hostQueues);
        contained = contained && this.appSoftware.containsAll(rc2.appSoftware);
        contained = contained && checkProcessors(rc2);
        contained = contained && checkMemory(rc2);
        contained = contained && checkStorage(rc2);

        return contained;
    }

    public boolean containsDynamic(MethodResourceDescription rc2) {
        boolean contained = checkProcessors(rc2);
        contained = contained && checkMemory(rc2);
        return contained;
    }

    private boolean checkProcessors(MethodResourceDescription rc2) {
        for (Processor p : rc2.processors) {
            if (!checkProcessor(p)) {
                return false;
            }
        }

        return true;
    }

    private boolean checkProcessor(Processor p) {
        for (Processor pThis : this.processors) {
            if (checkProcessorContains(pThis, p)) {
                // Satisfies all
                return true;
            }
        }

        return false;
    }

    private boolean checkProcessorContains(Processor pThis, Processor pRc2) {
        boolean contains = checkProcessorCompatibility(pThis, pRc2)
                && checkInclusion(pThis.getComputingUnits(), pRc2.getComputingUnits());

        return contains;
    }

    private boolean checkProcessorCompatibility(Processor pThis, Processor pRc2) {
        boolean compatible = checkCompatibility(pThis.getName(), pRc2.getName());
        compatible = compatible && checkCompatibility(pThis.getArchitecture(), pRc2.getArchitecture());
        compatible = compatible && checkInclusion(pThis.getSpeed(), pRc2.getSpeed());
        compatible = compatible && checkCompatibility(pThis.getPropName(), pRc2.getPropName());
        compatible = compatible && checkCompatibility(pThis.getPropValue(), pRc2.getPropValue());

        return compatible;
    }

    private boolean checkMemory(MethodResourceDescription rc2) {
        return checkInclusion(this.memorySize, rc2.memorySize)
                && checkCompatibility(this.memoryType, rc2.memoryType);
    }

    private boolean checkStorage(MethodResourceDescription rc2) {
        return checkInclusion(this.storageSize, rc2.storageSize)
                && checkCompatibility(this.storageType, rc2.storageType);
    }

    private boolean checkCompatibility(String value1, String value2) {
        return value1.equals(value2) || value1.equals(UNASSIGNED_STR) || value2.equals(UNASSIGNED_STR);
    }

    private boolean checkInclusion(int value1, int value2) {
        // If the value1 (implicit) is unassigned (in terms of CUs) it cannot run anything
        return value1 >= value2 || value2 == UNASSIGNED_INT;
    }

    private boolean checkInclusion(float value1, float value2) {
        return value1 >= value2 || value1 == UNASSIGNED_FLOAT || value2 == UNASSIGNED_FLOAT;
    }

    public Integer canHostSimultaneously(MethodResourceDescription rc2) {
        int min = Integer.MAX_VALUE;

        // Global CUs restriction
        if (rc2.getTotalComputingUnits() >= 1) {
            float ratio = this.getTotalComputingUnits() / (float) rc2.getTotalComputingUnits();
            min = (int) ratio;
        }

        // Per processor CUs restriction
        for (Processor p : rc2.processors) {
            for (Processor pThis : this.processors) {
                if (checkProcessorCompatibility(pThis, p)) {
                    float ratio = pThis.getComputingUnits() / p.getComputingUnits();
                    min = Math.min(min, (int) ratio);
                }
            }
        }

        // Memory restriction
        if (rc2.memorySize > 0.0f) {
            float ratio = this.memorySize / rc2.memorySize;
            min = Math.min(min, (int) ratio);
        }

        return min;
    }

    public MethodResourceDescription multiply(int amount) {
        MethodResourceDescription rd = new MethodResourceDescription();

        // Processors
        for (Processor p : this.processors) {
            Processor new_p = new Processor(p);
            new_p.multiply(amount);
            rd.addProcessor(new_p);
        }

        // Memory
        rd.memorySize = this.memorySize * amount;

        // Storage
        rd.storageSize = this.storageSize * amount;

        return rd;
    }

    // This method expands the implicit value with the ones defined by mr2
    // Keeps the unique values and gets the maximum of the common values
    @Override
    public void increase(ResourceDescription rd2) {
        MethodResourceDescription mrd2 = (MethodResourceDescription) rd2;

        // Increase Processors and Memory
        increaseDynamic(rd2);

        // Storage
        this.storageSize = Math.max(this.storageSize, mrd2.storageSize);
        if (this.storageType.equals(UNASSIGNED_STR)) {
            if (mrd2.storageType != null && !mrd2.storageType.isEmpty()) {
                this.storageType = mrd2.storageType;
            }
        }

        // OperatingSystem
        if (this.operatingSystemType.equals(UNASSIGNED_STR)) {
            if (mrd2.operatingSystemType != null && !mrd2.operatingSystemType.isEmpty()) {
                this.operatingSystemType = mrd2.operatingSystemType;
            }
        }
        if (this.operatingSystemDistribution.equals(UNASSIGNED_STR)) {
            if (mrd2.operatingSystemDistribution != null && !mrd2.operatingSystemDistribution.isEmpty()) {
                this.operatingSystemDistribution = mrd2.operatingSystemDistribution;
            }
        }
        if (this.operatingSystemVersion.equals(UNASSIGNED_STR)) {
            if (mrd2.memoryType != null && !mrd2.memoryType.isEmpty()) {
                this.operatingSystemVersion = mrd2.operatingSystemVersion;
            }
        }

        // Applications
        for (String app : mrd2.appSoftware) {
            if (!this.appSoftware.contains(app)) {
                this.appSoftware.add(app);
            }
        }

        // Host queues
        for (String queue : mrd2.hostQueues) {
            if (!this.hostQueues.contains(queue)) {
                this.hostQueues.add(queue);
            }
        }

        // Price
        if (this.pricePerUnit == UNASSIGNED_FLOAT) {
            if (mrd2.pricePerUnit > (float) 0.0) {
                this.pricePerUnit = mrd2.pricePerUnit;
            }
        }
        if (this.priceTimeUnit == UNASSIGNED_INT) {
            if (mrd2.priceTimeUnit > 0) {
                this.priceTimeUnit = mrd2.priceTimeUnit;
            }
        }

        // WallClock limit
        this.wallClockLimit = Math.max(this.wallClockLimit, mrd2.wallClockLimit);

        // Internal fields
        this.value = Math.max(this.value, mrd2.value);
    }

    @Override
    public void reduce(ResourceDescription rd2) {
        MethodResourceDescription mrd2 = (MethodResourceDescription) rd2;

        // Increase Processors and Memory
        reduceDynamic(rd2);

        // Storage
        this.storageSize = Math.min(this.storageSize, mrd2.storageSize);
        if (this.storageType.equals(UNASSIGNED_STR)) {
            if (mrd2.storageType != null && !mrd2.storageType.isEmpty()) {
                this.storageType = mrd2.storageType;
            }
        }

        // OperatingSystem
        if (this.operatingSystemType.equals(UNASSIGNED_STR)) {
            if (mrd2.operatingSystemType != null && !mrd2.operatingSystemType.isEmpty()) {
                this.operatingSystemType = mrd2.operatingSystemType;
            }
        }
        if (this.operatingSystemDistribution.equals(UNASSIGNED_STR)) {
            if (mrd2.operatingSystemDistribution != null && !mrd2.operatingSystemDistribution.isEmpty()) {
                this.operatingSystemDistribution = mrd2.operatingSystemDistribution;
            }
        }
        if (this.operatingSystemVersion.equals(UNASSIGNED_STR)) {
            if (mrd2.memoryType != null && !mrd2.memoryType.isEmpty()) {
                this.operatingSystemVersion = mrd2.operatingSystemVersion;
            }
        }

        // Applications
        for (String app : mrd2.appSoftware) {
            if (this.appSoftware.contains(app)) {
                this.appSoftware.remove(app);
            }
        }

        // Host queues
        for (String queue : mrd2.hostQueues) {
            if (this.hostQueues.contains(queue)) {
                this.hostQueues.remove(queue);
            }
        }

        // Price
        if (this.pricePerUnit == UNASSIGNED_FLOAT) {
            if (mrd2.pricePerUnit > (float) 0.0) {
                this.pricePerUnit = mrd2.pricePerUnit;
            }
        } else {
            if ((mrd2.pricePerUnit > (float) 0.0) && (mrd2.pricePerUnit < this.pricePerUnit)) {
                this.pricePerUnit = mrd2.pricePerUnit;
            }
        }
        if (this.priceTimeUnit == UNASSIGNED_INT) {
            if (mrd2.priceTimeUnit > 0) {
                this.priceTimeUnit = mrd2.priceTimeUnit;
            }
        } else {
            if ((mrd2.priceTimeUnit > 0) && (mrd2.priceTimeUnit < this.priceTimeUnit)) {
                this.priceTimeUnit = mrd2.priceTimeUnit;
            }
        }

        // WallClock limit
        this.wallClockLimit = Math.min(this.wallClockLimit, mrd2.wallClockLimit);

        // Internal fields
        this.value = Math.min(this.value, mrd2.value);
    }

    @Override
    public void increaseDynamic(ResourceDescription rd2) {
        MethodResourceDescription mrd2 = (MethodResourceDescription) rd2;

        // Processor
        for (Processor p : mrd2.processors) {
            // Looks for a mergeable processor
            boolean processorMerged = false;
            for (Processor pThis : this.processors) {
                if (checkProcessorCompatibility(pThis, p)) {
                    processorMerged = true;

                    // Increase current
                    int cus = p.getComputingUnits();
                    pThis.addComputingUnits(cus);
                    this.totalComputingUnits += cus;

                    // Go for next processor
                    break;
                }
            }
            if (!processorMerged) {
                Processor newProc = new Processor(p);
                this.addProcessor(newProc); // Increases totalCUs
            }
        }

        // Memory
        if ((this.memorySize != UNASSIGNED_FLOAT) && (mrd2.memorySize != UNASSIGNED_FLOAT)) {
            this.memorySize += mrd2.memorySize;
        }
    }

    @Override
    public ResourceDescription reduceDynamic(ResourceDescription rd2) {
        MethodResourceDescription mrd2 = (MethodResourceDescription) rd2;
        MethodResourceDescription reduced = new MethodResourceDescription();

        // Processor
        for (Processor p : mrd2.processors) {
            // Looks for a mergeable processor
            boolean processorMerged = false;
            for (Processor pThis : this.processors) {
                if (checkProcessorCompatibility(pThis, p)) {
                    processorMerged = true;
                    int cus = p.getComputingUnits();

                    // Copy the real decreased capabilities
                    Processor p_reduced = new Processor(pThis);
                    p_reduced.setComputingUnits(cus);
                    reduced.addProcessor(p_reduced);

                    // Decrease current
                    pThis.removeComputingUnits(cus);
                    this.totalComputingUnits -= cus;

                    // Go for next processor
                    break;
                }
            }
            if (!processorMerged) {
                // The reduce is invalid
                return null;
            }
        }

        // Memory
        if (checkMemory(mrd2)) {
            // Copy the real decreased capabilities
            reduced.setMemoryType(this.memoryType);
            if ((mrd2.memorySize == UNASSIGNED_FLOAT) || (this.memorySize == UNASSIGNED_FLOAT)) {
                reduced.setMemorySize(UNASSIGNED_FLOAT);
            } else {
                this.memorySize -= mrd2.memorySize;
                reduced.setMemorySize(mrd2.memorySize);
            }
        } else {
            // The reduce is invalid
            return null;
        }

        // Return the real decreased capabilities
        return reduced;
    }

    @Override
    public ResourceDescription getDynamicCommons(ResourceDescription other) {
        MethodResourceDescription otherMRD = (MethodResourceDescription) other;
        MethodResourceDescription common = new MethodResourceDescription();
        
        // Processor
        for (Processor p : otherMRD.getProcessors()) {
        	boolean isProcessorCompatible = false;
        	int i = 0;
        	while (i < this.processors.size() && !isProcessorCompatible) {
        		Processor pThis = this.processors.get(i);
        		
            	// Only checks compatibility, not inclusion
        		if (checkProcessorCompatibility(pThis, p)) {
    	    		// Satisfies compatibility
        			isProcessorCompatible = true;
        			// Include commons
        			common.addProcessor(getDynamicCommonsProcessor(pThis, p));
        		}
        		i = i + 1;
        	}
        }
        
        // Memory
        // Only checks compatibility, not inclusion
        if (checkCompatibility(this.memoryType, otherMRD.memoryType)) {
        	// Copy the assignable memory type (no the requested)
        	common.setMemoryType(this.getMemoryType());
        	common.setMemorySize(Math.min(this.memorySize, otherMRD.getMemorySize()));
        }
        
        return common;
    }
    
    private Processor getDynamicCommonsProcessor(Processor pThis, Processor p) {
    	// Copy the assignable processor (no the requested)
    	Processor common = new Processor(pThis);
    	
    	// Compute the number of CUs that can be given
    	int cus = Math.min(pThis.getComputingUnits(), p.getComputingUnits());
    	common.setComputingUnits(cus);
    	
    	return common;
    }

    @Override
    public boolean isDynamicUseless() {
        return (this.getMemorySize() <= 0.0 && this.totalComputingUnits < 1);
    }

    @Override
    public boolean canHost(Implementation<?> impl) {
        if (impl.getType() == Type.METHOD) {
            MethodResourceDescription wd = (MethodResourceDescription) impl.getRequirements();
            return this.contains(wd);
        }
        return false;

    }

    @Override
    public boolean canHostDynamic(Implementation<?> impl) {
        if (impl.getType() == Type.METHOD) {
            MethodResourceDescription wd = (MethodResourceDescription) impl.getRequirements();
            return this.containsDynamic(wd);
        }
        return false;
    }

    /* *******************************************
     * LOGGERS
     * *******************************************/
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[DESCRIPTION");

        for (Processor p : this.processors) {
            sb.append(" [PROCESSOR ").append(p.getName());
            sb.append(" COMPUTING_UNITS=").append(p.getComputingUnits());
            sb.append(" SPEED=").append(p.getSpeed());
            sb.append(" ARCHITECTURE=").append(p.getArchitecture());
            sb.append(" PROP_NAME=").append(p.getPropName());
            sb.append(" PROP_VALUE=").append(p.getPropValue());
            sb.append("]");
        }

        sb.append("[GENERAL_COUNTS");
        sb.append(" TOTAL_CU=").append(this.totalComputingUnits);
        sb.append("]");

        sb.append(" [MEMORY");
        sb.append(" SIZE=").append(this.memorySize);
        sb.append(" TYPE=").append(this.memoryType);
        sb.append("]");

        sb.append(" [STORAGE");
        sb.append(" SIZE=").append(this.storageSize);
        sb.append(" TYPE=").append(this.storageType);
        sb.append("]");

        sb.append(" [OPERATING_SYSTEM");
        sb.append(" TYPE=").append(this.operatingSystemType);
        sb.append(" DISTRIBUTION=").append(this.operatingSystemDistribution);
        sb.append(" VERSION=").append(this.operatingSystemVersion);
        sb.append("]");

        sb.append(" [SOFTWARE ");
        for (String app : this.appSoftware) {
            sb.append(app).append(", ");
        }
        sb.append("]");

        sb.append(" [HOST_QUEUES ");
        for (String queue : this.hostQueues) {
            sb.append(queue).append(", ");
        }
        sb.append("]");

        sb.append(" [PRICE");
        sb.append(" TIME_UNIT=").append(this.priceTimeUnit);
        sb.append(" PRICE_PER_TIME=").append(this.pricePerUnit);
        sb.append("]");

        sb.append(" [WALLCLOCK=").append(this.wallClockLimit).append("]");

        // End DESCRIPTION
        sb.append("]");

        return sb.toString();
    }

}
