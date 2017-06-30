package integratedtoolkit.types.resources;

import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.Implementation.TaskType;
import integratedtoolkit.types.resources.components.Processor;
import integratedtoolkit.util.EnvironmentLoader;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class MethodResourceDescription extends WorkerResourceDescription {

    // Constant for weight difference (dynamic increase/decrease)
    private static final int DIFFERENCE_WEIGHT = 10_000;
    private static final int OTHER_PROC_DIFFERENCE_WEIGHT = 1_000;

    // Empty Resource Description
    public static final MethodResourceDescription EMPTY_FOR_RESOURCE = new MethodResourceDescription();
    public static final MethodResourceDescription EMPTY_FOR_CONSTRAINTS = new MethodResourceDescription(ONE_INT);

    /* Tags for key-value string constraints description **************** */
    // !!!!!!!!!! WARNING: Coherent with constraints class
    public static final String PROC_NAME = "ProcessorName";
    public static final String COMPUTING_UNITS = "ComputingUnits";
    public static final String PROC_SPEED = "ProcessorSpeed";
    public static final String PROC_ARCH = "ProcessorArchitecture";
    public static final String PROC_TYPE = "ProcessorType";
    public static final String PROC_MEM_SIZE = "ProcessorInternalMemorySize";
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

    /* Resource Description properties ********************************** */
    // Processor
    protected List<Processor> processors = new LinkedList<Processor>();
    protected int totalCPUComputingUnits = ZERO_INT;
    protected int totalCPUs = ZERO_INT;
    protected int totalGPUComputingUnits = ZERO_INT;
    protected int totalGPUs = ZERO_INT;
    protected int totalFPGAComputingUnits = ZERO_INT;
    protected int totalFPGAs = ZERO_INT;
    protected int totalOTHERComputingUnits = ZERO_INT;
    protected int totalOTHERs = ZERO_INT;

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
    protected List<String> appSoftware = new LinkedList<>();
    // Host queues
    protected List<String> hostQueues = new LinkedList<>();
    // Price
    protected int priceTimeUnit = UNASSIGNED_INT;
    protected float pricePerUnit = UNASSIGNED_FLOAT;
    // WallClock Limit (from constraints, not from XML files)
    protected int wallClockLimit = UNASSIGNED_INT;

    /* Internal fields ************************************************** */
    protected Float value = 0.0f;


    /*
     * ******************************************* 
     * CONSTRUCTORS
     *******************************************/
    public MethodResourceDescription() {
        super();
    }

    private MethodResourceDescription(int initialCUs) {
        super();

        // Add processor structures (with an empty non assigned processor)
        Processor p = new Processor();
        p.setComputingUnits(initialCUs);
        this.addProcessor(p); // Increases the total CUs
    }

    /**
     * Creates a MethodResourceDescription representing a set of constraints The
     * constraints are validated and loaded through this process. If any error
     * occurs an exception is raised to the user through the ErrorManager
     *
     * @param constraints
     */
    public MethodResourceDescription(Constraints constraints) {
        super();

        /*
         * No constraints are defined
         */
        if (constraints == null) {
            // We leave the default values and add a single CU
            Processor p = new Processor();
            p.setComputingUnits(ONE_INT);
            this.addProcessor(p);

            return;
        }

        /*
         * Otherwise we parse each possible constraint
         */
        // Parse processors - When coming from Constraints only one processor is available
        integratedtoolkit.types.annotations.Processor[] processorsConstraints = constraints.processors();
        if (processorsConstraints != null && processorsConstraints.length > 0) {
            for (integratedtoolkit.types.annotations.Processor pC : processorsConstraints) {
                this.addProcessor(getProcessorFromProcessorsConstraint(pC));
            }
        }

        if (containsProcessorsProperties(constraints)) {
            Processor p = new Processor();
            String procName = constraints.processorName();
            procName = EnvironmentLoader.loadFromEnvironment(procName);
            if (procName != null && !procName.equals(UNASSIGNED_STR)) {
                p.setName(procName);
            }

            String cuSTR = constraints.computingUnits();
            cuSTR = EnvironmentLoader.loadFromEnvironment(cuSTR);
            int cu = (cuSTR != null && !cuSTR.isEmpty() && !cuSTR.equals(UNASSIGNED_STR)) ? Integer.valueOf(cuSTR) : ONE_INT;
            if (cu > ONE_INT) {
                p.setComputingUnits(cu);
            } else {
                // When loading from constraints, always use at least one computing unit
                p.setComputingUnits(ONE_INT);
            }

            String speedSTR = constraints.processorSpeed();
            speedSTR = EnvironmentLoader.loadFromEnvironment(speedSTR);
            float speed = (speedSTR != null && !speedSTR.isEmpty() && !speedSTR.equals(UNASSIGNED_STR)) ? Float.valueOf(speedSTR)
                    : UNASSIGNED_FLOAT;
            if (speed != UNASSIGNED_FLOAT) {
                p.setSpeed(speed);
            }

            String arch = constraints.processorArchitecture();
            arch = EnvironmentLoader.loadFromEnvironment(arch);
            if (arch != null && !arch.equals(UNASSIGNED_STR)) {
                p.setArchitecture(arch);
            }

            String type = constraints.processorType();
            type = EnvironmentLoader.loadFromEnvironment(type);
            if (type != null) {
                p.setType(type);
            }

            String internalMemorySTR = constraints.processorInternalMemorySize();
            internalMemorySTR = EnvironmentLoader.loadFromEnvironment(internalMemorySTR);
            float internalMemory = (internalMemorySTR != null && !internalMemorySTR.isEmpty() && !internalMemorySTR.equals(UNASSIGNED_STR))
                    ? Float.valueOf(internalMemorySTR) : UNASSIGNED_FLOAT;
            if (internalMemory != UNASSIGNED_FLOAT) {
                p.setInternalMemory(internalMemory);
            }

            String propName = constraints.processorPropertyName();
            propName = EnvironmentLoader.loadFromEnvironment(propName);
            if (propName != null && !propName.equals(UNASSIGNED_STR)) {
                p.setPropName(propName);
            }

            String propvalue = constraints.processorPropertyValue();
            propvalue = EnvironmentLoader.loadFromEnvironment(propvalue);
            if (propvalue != null && !propvalue.equals(UNASSIGNED_STR)) {
                p.setPropValue(propvalue);
            }
            this.addProcessor(p);
        }

        if (this.totalCPUs == 0) {
            Processor p = new Processor();
            p.setComputingUnits(ONE_INT);
            this.addProcessor(p);
        }

        // Parse software
        String software = constraints.appSoftware();
        software = EnvironmentLoader.loadFromEnvironment(software);
        if (software != null && !software.equals(UNASSIGNED_STR)) {
            for (String value : software.split(",")) {
                this.appSoftware.add(value.trim().toUpperCase());
            }
        }

        // Parse queues
        String queues = constraints.hostQueues();
        queues = EnvironmentLoader.loadFromEnvironment(queues);
        if (queues != null && !queues.equals(UNASSIGNED_STR)) {
            for (String value : queues.split(",")) {
                this.hostQueues.add(value.trim().toUpperCase());
            }
        }

        // Parse memory, storage and OS constraints
        String memorySizeSTR = constraints.memorySize();
        memorySizeSTR = EnvironmentLoader.loadFromEnvironment(memorySizeSTR);
        float memorySize = (memorySizeSTR != null && !memorySizeSTR.isEmpty() && !memorySizeSTR.equals(UNASSIGNED_STR))
                ? Float.valueOf(memorySizeSTR) : UNASSIGNED_FLOAT;
        if (memorySize != UNASSIGNED_FLOAT) {
            this.memorySize = memorySize;
        }
        String memoryType = constraints.memoryType();
        memoryType = EnvironmentLoader.loadFromEnvironment(memoryType);
        if (memoryType != null && !memoryType.equals(UNASSIGNED_STR)) {
            this.memoryType = memoryType;
        }

        String storageSizeSTR = constraints.storageSize();
        storageSizeSTR = EnvironmentLoader.loadFromEnvironment(storageSizeSTR);
        float storageSize = (storageSizeSTR != null && !storageSizeSTR.isEmpty() && !storageSizeSTR.equals(UNASSIGNED_STR))
                ? Float.valueOf(storageSizeSTR) : UNASSIGNED_FLOAT;
        if (storageSize != UNASSIGNED_FLOAT) {
            this.storageSize = storageSize;
        }
        String storageType = constraints.storageType();
        storageType = EnvironmentLoader.loadFromEnvironment(storageType);
        if (storageType != null && !storageType.equals(UNASSIGNED_STR)) {
            this.storageType = storageType;
        }

        String operatingSystemType = constraints.operatingSystemType();
        operatingSystemType = EnvironmentLoader.loadFromEnvironment(operatingSystemType);
        if (operatingSystemType != null && !operatingSystemType.equals(UNASSIGNED_STR)) {
            this.operatingSystemType = operatingSystemType;
        }
        String operatingSystemDistribution = constraints.operatingSystemDistribution();
        operatingSystemDistribution = EnvironmentLoader.loadFromEnvironment(operatingSystemDistribution);
        if (operatingSystemDistribution != null && !operatingSystemDistribution.equals(UNASSIGNED_STR)) {
            this.operatingSystemDistribution = operatingSystemDistribution;
        }
        String operatingSystemVersion = constraints.operatingSystemVersion();
        operatingSystemVersion = EnvironmentLoader.loadFromEnvironment(operatingSystemVersion);
        if (operatingSystemVersion != null && !operatingSystemVersion.equals(UNASSIGNED_STR)) {
            this.operatingSystemVersion = operatingSystemVersion;
        }

        String wallClockLimitSTR = constraints.wallClockLimit();
        wallClockLimitSTR = EnvironmentLoader.loadFromEnvironment(wallClockLimitSTR);
        int wallClockLimit = (wallClockLimitSTR != null && !wallClockLimitSTR.isEmpty() && !wallClockLimitSTR.equals(UNASSIGNED_STR))
                ? Integer.valueOf(wallClockLimitSTR) : UNASSIGNED_INT;
        if (wallClockLimit != UNASSIGNED_INT) {
            this.wallClockLimit = wallClockLimit;
        }

        // Prices don't come from constraints
    }

    private boolean containsProcessorsProperties(Constraints constraints) {
        return (!constraints.processorName().equals(Constants.UNASSIGNED)
                || !constraints.processorArchitecture().equals(Constants.UNASSIGNED)
                || !constraints.processorType().equals(Constants.UNASSIGNED_PROCESSOR_TYPE)
                || !constraints.processorSpeed().equals(Constants.UNASSIGNED)
                || !constraints.processorInternalMemorySize().equals(Constants.UNASSIGNED)
                || !constraints.processorPropertyName().equals(Constants.UNASSIGNED_PROCESSOR_TYPE)
                || !constraints.processorPropertyValue().equals(Constants.UNASSIGNED_PROCESSOR_TYPE));

    }

    private Processor getProcessorFromProcessorsConstraint(integratedtoolkit.types.annotations.Processor processorConstraints) {
        Processor p = new Processor();
        String procName = processorConstraints.name();
        if (procName != null && !procName.equals(UNASSIGNED_STR)) {
            procName = EnvironmentLoader.loadFromEnvironment(procName);
            p.setName(procName);
        }

        String cuSTR = processorConstraints.computingUnits();
        cuSTR = EnvironmentLoader.loadFromEnvironment(cuSTR);
        int cu = (cuSTR != null && !cuSTR.isEmpty() && !cuSTR.equals(UNASSIGNED_STR)) ? Integer.valueOf(cuSTR) : ONE_INT;
        if (cu > ONE_INT) {
            p.setComputingUnits(cu);
        } else {
            // When loading from constraints, always use at least one computing unit
            p.setComputingUnits(ONE_INT);
        }

        String speedSTR = processorConstraints.speed();
        speedSTR = EnvironmentLoader.loadFromEnvironment(speedSTR);
        float speed = (speedSTR != null && !speedSTR.isEmpty() && !speedSTR.equals(UNASSIGNED_STR)) ? Float.valueOf(speedSTR)
                : UNASSIGNED_FLOAT;
        if (speed != UNASSIGNED_FLOAT) {
            p.setSpeed(speed);
        }

        String arch = processorConstraints.architecture();
        arch = EnvironmentLoader.loadFromEnvironment(arch);
        if (arch != null && !arch.equals(UNASSIGNED_STR)) {
            p.setArchitecture(arch);
        }

        String type = processorConstraints.type();
        type = EnvironmentLoader.loadFromEnvironment(type);
        if (type != null) {
            p.setType(type);
        }

        String internalMemorySTR = processorConstraints.internalMemorySize();
        internalMemorySTR = EnvironmentLoader.loadFromEnvironment(internalMemorySTR);
        float internalMemory = (internalMemorySTR != null && !internalMemorySTR.isEmpty() && !internalMemorySTR.equals(UNASSIGNED_STR))
                ? Float.valueOf(internalMemorySTR) : UNASSIGNED_FLOAT;
        if (internalMemory != UNASSIGNED_FLOAT) {
            p.setInternalMemory(internalMemory);
        }

        String propName = processorConstraints.propertyName();
        propName = EnvironmentLoader.loadFromEnvironment(propName);
        if (propName != null && !propName.equals(UNASSIGNED_STR)) {
            p.setPropName(propName);
        }

        String propvalue = processorConstraints.propertyValue();
        propvalue = EnvironmentLoader.loadFromEnvironment(propvalue);
        if (propvalue != null && !propvalue.equals(UNASSIGNED_STR)) {
            p.setPropValue(propvalue);
        }

        return p;
    }

    /**
     * For python constraints
     *
     * @param description
     */
    public MethodResourceDescription(String description) {
        super();

        // TODO: Change to support multi-processors in constraints
        // Warning: When coming from constrains, only 1 PROCESSOR is available with at least 1 CU
        Processor proc = new Processor();
        proc.setComputingUnits(ONE_INT);

        if (description != null && !description.isEmpty()) {
            String[] constraints = description.split(";");
            for (String c : constraints) {
                String key = c.split(":")[0].trim();
                String val = c.split(":")[1].trim();
                addConstraints(key, val, proc);
            }
        }

        // Add the information retrieved from the processor constraints
        this.addProcessor(proc); // Increases the totalCUs
    }

    /**
     * For C constraints
     *
     * @param constraints
     */
    public MethodResourceDescription(String[] constraints, String processorString) {
        super();
        Processor proc = new Processor();

        if (processorString != null && !processorString.isEmpty()) {
            String[] processors = StringUtils.split(processorString, "@");
            for (int i = 0; i < processors.length; i++) {
                processors[i] = processors[i].replace("Processor(", "");
                processors[i] = processors[i].replaceAll("[,()]", "");

                String[] processorConstraints = processors[i].split(" ");
                proc = new Processor();
                for (int j = 0; j < processorConstraints.length; ++j) {
                    String key = processorConstraints[j].split("=")[0].trim();
                    String val = processorConstraints[j].split("=")[1].trim();
                    addConstraints(key, val, proc);
                }
                this.addProcessor(proc);
            }
        } else {
            // If no specific processor is requested, a single processor will be used with at least 1 CU
            proc.setComputingUnits(ONE_INT);
        }

        // Don't add constraints if there only was processor info
        if (constraints.length != 1 || !"".equals(constraints[0])) {
            for (String c : constraints) {
                String key = c.split("=")[0].trim();
                String val = c.split("=")[1].trim();
                addConstraints(key, val, proc);
            }
        }

        // Add the information retrieved from the processor constraints
        if (processorString == null || processorString.isEmpty()) {
            this.addProcessor(proc); // Increases the totalCUs
        }

        if (this.totalCPUs == 0) {
            Processor p = new Processor();
            p.setComputingUnits(ONE_INT);
            this.addProcessor(p);
        }

    }

    private void addConstraints(String key, String val, Processor proc) {
        val = EnvironmentLoader.loadFromEnvironment(val);
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
                case PROC_TYPE:
                    proc.setType(val);
                    break;
                case PROC_PROP_NAME:
                    proc.setPropName(val);
                    break;
                case PROC_PROP_VALUE:
                    proc.setPropValue(val);
                    break;
                case PROC_MEM_SIZE:
                    proc.setInternalMemory(Float.valueOf(val));
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

        initProcessorCounters();
        for (Processor p : clone.processors) {
            Processor newP = new Processor(p);
            this.addProcessor(newP); // Increases totalCUs
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

    private void initProcessorCounters() {
        this.totalCPUComputingUnits = 0;
        this.totalCPUs = 0;
        this.totalGPUComputingUnits = 0;
        this.totalGPUs = 0;
        this.totalFPGAComputingUnits = 0;
        this.totalFPGAs = 0;
        this.totalOTHERComputingUnits = 0;
        this.totalOTHERs = 0;
    }

    @Override
    public MethodResourceDescription copy() {
        return new MethodResourceDescription(this);
    }

    /*
     * ******************************************* 
     * GETTERS AND SETTERS
     *******************************************/
    public List<Processor> getProcessors() {
        return processors;
    }

    public void setProcessors(List<Processor> processors) {
        this.processors = processors;

        initProcessorCounters();
        for (Processor p : this.processors) {
            updateCounters(p);
        }
    }

    public void resetProcessors() {
        this.processors = new LinkedList<Processor>();
        initProcessorCounters();
    }

    public void addProcessor(Processor p) {
        this.processors.add(p);
        updateCounters(p);
    }

    private void updateCounters(Processor p) {
        String type = p.getType();
        int cu = p.getComputingUnits();
        if (type.equals(Constants.CPU_TYPE)) {
            if (cu > 0) {
                totalCPUComputingUnits += cu;

            } else {
                totalCPUComputingUnits++;
            }
            totalCPUs++;
        } else if (type.equals(Constants.GPU_TYPE)) {
            if (cu > 0) {
                totalGPUComputingUnits += cu;

            } else {
                totalGPUComputingUnits++;
            }
            totalGPUs++;
        } else if (type.equals(Constants.FPGA_TYPE)) {
            if (cu > 0) {
                totalFPGAComputingUnits += cu;
            } else {
                totalFPGAComputingUnits++;
            }
            totalFPGAs++;
        } else {
            if (cu > 0) {
                totalOTHERComputingUnits += cu;
            } else {
                totalOTHERComputingUnits++;
            }
            totalOTHERs++;
        }

    }

    public void addProcessor(String procName, int computingUnits, String architecture, float speed, String type, float internalMemory,
            String propName, String propValue) {

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
        if (type != null && !type.isEmpty()) {
            p.setType(type);
        }
        if (internalMemory > (float) 0.0) {
            p.setInternalMemory(internalMemory);
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

    public int getTotalCPUComputingUnits() {
        return this.totalCPUComputingUnits;
    }

    public int getTotalGPUComputingUnits() {
        return this.totalGPUComputingUnits;
    }

    public int getTotalFPGAComputingUnits() {
        return this.totalFPGAComputingUnits;
    }

    public int getTotalOTHERComputingUnits() {
        return this.totalOTHERComputingUnits;
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

    public boolean containsCPU() {
        return getTotalCPUComputingUnits() > 0;
    }

    public boolean containsGPU() {
        return getTotalGPUComputingUnits() > 0;
    }

    public boolean containsFPGA() {
        return getTotalFPGAComputingUnits() > 0;
    }

    public boolean containsOthers() {
        return getTotalOTHERComputingUnits() > 0;
    }

    /*
     * ******************************************* 
     * METHOD-RESOURCE OPERATIONS
     *******************************************/
    // This method tries to substitute the implicit default values by defined mr2 values.
    // Keeps the already defined values (do NOT overwrite)
    // ONLY CALLED FROM CONSTRAINTS (1 processor always)
    public void mergeMultiConstraints(MethodResourceDescription mr2) {
        // Processor constraints
        for (Processor pmr2 : mr2.processors) {
            String type = pmr2.getType();
            Processor pthis = lookForProcessorType(type);
            if (pthis != null) {
                if (pthis.getComputingUnits() <= ONE_INT) {
                    int newCus = pmr2.getComputingUnits();
                    int currentCUs = pthis.getComputingUnits();
                    pthis.setComputingUnits(newCus);
                    if (type.equals(Constants.CPU_TYPE)) {
                        this.totalCPUComputingUnits += (newCus - currentCUs);
                    } else if (type.equals(Constants.GPU_TYPE)) {
                        this.totalGPUComputingUnits += (newCus - currentCUs);
                    } else if (type.equals(Constants.FPGA_TYPE)) {
                        this.totalFPGAComputingUnits += (newCus - currentCUs);
                    } else {
                        this.totalOTHERComputingUnits += (newCus - currentCUs);
                    }
                }
                if (pthis.getSpeed() == UNASSIGNED_FLOAT) {
                    pthis.setSpeed(pmr2.getSpeed());
                }
                if (pthis.getInternalMemory() == UNASSIGNED_FLOAT) {
                    pthis.setInternalMemory(pmr2.getInternalMemory());
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
            } else {
                this.addProcessor(pmr2);
            }
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

    private Processor lookForProcessorType(String type) {
        for (Processor p : this.processors) {
            if (p.getType().equals(type)) {
                return p;
            }
        }
        return null;
    }

    public float difference(MethodResourceDescription mr2) {
        float processorDif = this.getTotalCPUComputingUnits() - mr2.getTotalCPUComputingUnits();
        float otherProcessorsDif = (this.getTotalGPUComputingUnits() + this.getTotalFPGAComputingUnits()
                + this.getTotalOTHERComputingUnits())
                - (mr2.getTotalGPUComputingUnits() + mr2.getTotalFPGAComputingUnits() + mr2.getTotalOTHERComputingUnits());
        float memoryDif = this.memorySize - mr2.memorySize;

        return (processorDif * DIFFERENCE_WEIGHT) + (otherProcessorsDif * OTHER_PROC_DIFFERENCE_WEIGHT) + memoryDif;
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
        boolean contains = checkProcessorCompatibility(pThis, pRc2) && checkInclusion(pThis.getComputingUnits(), pRc2.getComputingUnits());

        return contains;
    }

    private boolean checkProcessorCompatibility(Processor pThis, Processor pRc2) {
        boolean compatible = checkCompatibility(pThis.getName(), pRc2.getName());
        compatible = compatible && checkCompatibility(pThis.getArchitecture(), pRc2.getArchitecture());
        compatible = compatible && checkInclusion(pThis.getSpeed(), pRc2.getSpeed());
        compatible = compatible && checkCompatibility(pThis.getType(), pRc2.getType());
        compatible = compatible && checkInclusion(pThis.getInternalMemory(), pRc2.getInternalMemory());
        compatible = compatible && checkCompatibility(pThis.getPropName(), pRc2.getPropName());
        compatible = compatible && checkCompatibility(pThis.getPropValue(), pRc2.getPropValue());

        return compatible;
    }

    private boolean checkMemory(MethodResourceDescription rc2) {
        return checkInclusion(this.memorySize, rc2.memorySize) && checkCompatibility(this.memoryType, rc2.memoryType);
    }

    private boolean checkStorage(MethodResourceDescription rc2) {
        return checkInclusion(this.storageSize, rc2.storageSize) && checkCompatibility(this.storageType, rc2.storageType);
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
        if (rc2.getTotalCPUComputingUnits() >= 1) {
            float ratio = this.getTotalCPUComputingUnits() / (float) rc2.getTotalCPUComputingUnits();
            min = (int) ratio;
        }
        if (rc2.getTotalGPUComputingUnits() >= 1) {
            float ratio = this.getTotalGPUComputingUnits() / (float) rc2.getTotalGPUComputingUnits();
            min = (int) ratio;
        }
        if (rc2.getTotalFPGAComputingUnits() >= 1) {
            float ratio = this.getTotalFPGAComputingUnits() / (float) rc2.getTotalFPGAComputingUnits();
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
        if (this.memorySize != UNASSIGNED_FLOAT && rc2.memorySize != UNASSIGNED_FLOAT && rc2.memorySize > 0.0f) {
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
        } else if ((mrd2.pricePerUnit > (float) 0.0) && (mrd2.pricePerUnit < this.pricePerUnit)) {
            this.pricePerUnit = mrd2.pricePerUnit;
        }
        if (this.priceTimeUnit == UNASSIGNED_INT) {
            if (mrd2.priceTimeUnit > 0) {
                this.priceTimeUnit = mrd2.priceTimeUnit;
            }
        } else if ((mrd2.priceTimeUnit > 0) && (mrd2.priceTimeUnit < this.priceTimeUnit)) {
            this.priceTimeUnit = mrd2.priceTimeUnit;
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
                    this.increaseComputingUnits(pThis.getType(), cus);

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
                    this.decreaseComputingUnits(pThis.getType(), cus);

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

    private void increaseComputingUnits(String type, int cus) {

        if (type.equals(Constants.CPU_TYPE)) {
            this.totalCPUComputingUnits += cus;
        } else if (type.equals(Constants.GPU_TYPE)) {
            this.totalGPUComputingUnits += cus;
        } else if (type.equals(Constants.FPGA_TYPE)) {
            this.totalFPGAComputingUnits += cus;
        } else {
            this.totalOTHERComputingUnits += cus;
        }
    }

    private void decreaseComputingUnits(String type, int cus) {
        if (type.equals(Constants.CPU_TYPE)) {
            this.totalCPUComputingUnits -= cus;
        } else if (type.equals(Constants.GPU_TYPE)) {
            this.totalGPUComputingUnits -= cus;
        } else if (type.equals(Constants.FPGA_TYPE)) {
            this.totalFPGAComputingUnits -= cus;
        } else {
            this.totalOTHERComputingUnits -= cus;
        }
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

    /*
     * ******************************************* 
     * EXTERNALIZATION
     *******************************************/
    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        processors = (List<Processor>) in.readObject();
        totalCPUComputingUnits = in.readInt();
        totalCPUs = in.readInt();
        totalGPUComputingUnits = in.readInt();
        totalGPUs = in.readInt();
        totalFPGAComputingUnits = in.readInt();
        totalFPGAs = in.readInt();
        totalOTHERComputingUnits = in.readInt();
        totalOTHERs = in.readInt();
        memorySize = in.readFloat();
        memoryType = (String) in.readObject();
        storageSize = in.readFloat();
        storageType = (String) in.readObject();
        operatingSystemType = (String) in.readObject();
        operatingSystemDistribution = (String) in.readObject();
        operatingSystemVersion = (String) in.readObject();
        appSoftware = (List<String>) in.readObject();
        hostQueues = (List<String>) in.readObject();
        priceTimeUnit = in.readInt();
        pricePerUnit = in.readFloat();
        wallClockLimit = in.readInt();
        value = in.readFloat();

    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(processors);
        out.writeInt(totalCPUComputingUnits);
        out.writeInt(totalCPUs);
        out.writeInt(totalGPUComputingUnits);
        out.writeInt(totalGPUs);
        out.writeInt(totalFPGAComputingUnits);
        out.writeInt(totalFPGAs);
        out.writeInt(totalOTHERComputingUnits);
        out.writeInt(totalOTHERs);
        out.writeFloat(memorySize);
        out.writeObject(memoryType);
        out.writeFloat(storageSize);
        out.writeObject(storageType);
        out.writeObject(operatingSystemType);
        out.writeObject(operatingSystemDistribution);
        out.writeObject(operatingSystemVersion);
        out.writeObject(appSoftware);
        out.writeObject(hostQueues);
        out.writeInt(priceTimeUnit);
        out.writeFloat(pricePerUnit);
        out.writeInt(wallClockLimit);
        out.writeFloat(value);
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
        return (this.getMemorySize() <= 0.0 && this.totalCPUComputingUnits < 1);
    }

    @Override
    public boolean canHost(Implementation impl) {
        if (impl.getTaskType() == TaskType.METHOD) {
            MethodResourceDescription wd = (MethodResourceDescription) impl.getRequirements();
            return this.contains(wd);
        }
        return false;

    }

    @Override
    public boolean canHostDynamic(Implementation impl) {
        if (impl.getTaskType() == TaskType.METHOD) {
            MethodResourceDescription wd = (MethodResourceDescription) impl.getRequirements();
            return this.containsDynamic(wd);
        }
        return false;
    }

    /*
     * ******************************************* LOGGERS ******************************************
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[DESCRIPTION");

        for (Processor p : this.processors) {
            sb.append(" [PROCESSOR ").append(p.getName());
            sb.append(" TYPE=").append(p.getType());
            sb.append(" COMPUTING_UNITS=").append(p.getComputingUnits());
            sb.append(" SPEED=").append(p.getSpeed());
            sb.append(" INTERNAL_MEMORY=").append(p.getInternalMemory());
            sb.append(" ARCHITECTURE=").append(p.getArchitecture());
            sb.append(" PROP_NAME=").append(p.getPropName());
            sb.append(" PROP_VALUE=").append(p.getPropValue());
            sb.append("]");
        }

        sb.append("[GENERAL_COUNTS");
        sb.append(" TOTAL_CPUs=").append(this.totalCPUs);
        sb.append(" TOTAL_CPU_CU=").append(this.totalCPUComputingUnits);
        sb.append(" TOTAL_GPUs=").append(this.totalGPUs);
        sb.append(" TOTAL_GPU_CU=").append(this.totalGPUComputingUnits);
        sb.append(" TOTAL_FPGAs=").append(this.totalFPGAs);
        sb.append(" TOTAL_FPGA_CU=").append(this.totalFPGAComputingUnits);
        sb.append(" TOTAL_OTHERs=").append(this.totalOTHERs);
        sb.append(" TOTAL_OTHER_CU=").append(this.totalOTHERComputingUnits);
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

    @Override
    public String getDynamicDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append(" Processor: ");
        // Processor

        for (Processor pThis : this.processors) {
            sb.append(pThis.getComputingUnits() + " " + pThis.getArchitecture() + " cores");

        }

        // Memory
        sb.append(" Memory: ");
        if (this.memorySize != UNASSIGNED_FLOAT) {
            sb.append(this.memorySize);
        } else {
            sb.append("Unassigned");
        }
        return sb.toString();
    }
}
