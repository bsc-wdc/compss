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
package es.bsc.compss.types.resources;

import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.resources.components.Processor;
import es.bsc.compss.types.resources.components.Processor.ProcessorType;
import es.bsc.compss.util.EnvironmentLoader;
import es.bsc.compss.util.ErrorManager;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;


public class MethodResourceDescription extends WorkerResourceDescription {

    // Constant for weight difference (dynamic increase/decrease)
    private static final int DIFFERENCE_WEIGHT = 10_000;
    private static final int OTHER_PROC_DIFFERENCE_WEIGHT = 1_000;

    // Empty Resource Description
    public static final MethodResourceDescription EMPTY_FOR_RESOURCE = new MethodResourceDescription();
    public static final MethodResourceDescription EMPTY_FOR_CONSTRAINTS = new MethodResourceDescription(ONE_INT);

    /* Tags for key-value string constraints description **************** */
    // !!!!!!!!!! WARNING: Coherent with constraints class
    private static final String PROCESSORS = "processors";
    private static final String PROC_NAME = "processorname";
    private static final String COMPUTING_UNITS = "computingunits";
    private static final String PROC_SPEED = "processorspeed";
    private static final String PROC_ARCH = "processorarchitecture";
    private static final String PROC_TYPE = "processortype";
    private static final String PROC_MEM_SIZE = "processorinternalmemorysize";
    private static final String PROC_PROP_NAME = "processorpropertyname";
    private static final String PROC_PROP_VALUE = "processorpropertyvalue";
    private static final String MEM_SIZE = "memorysize";
    private static final String MEM_TYPE = "memorytype";
    private static final String STORAGE_SIZE = "storagesize";
    private static final String STORAGE_TYPE = "storagetype";
    private static final String STORAGE_BW = "storagebw";
    private static final String OS_TYPE = "operatingsystemtype";
    private static final String OS_DISTRIBUTION = "operatingsystemdistribution";
    private static final String OS_VERSION = "operatingsystemversion";
    private static final String APP_SOFTWARE = "appsoftware";
    private static final String HOST_QUEUES = "hostqueues";
    private static final String WALL_CLOCK_LIMIT = "wallclocklimit";

    /* Resource Description properties ********************************** */
    // Processor
    protected List<Processor> processors = new LinkedList<>();
    protected int totalCPUComputingUnits = ZERO_INT;
    protected int totalMPIioComputingUnits = ZERO_INT;
    protected int totalCPUs = ZERO_INT;
    protected int totalGPUComputingUnits = ZERO_INT;
    protected int totalGPUs = ZERO_INT;
    protected int totalFPGAComputingUnits = ZERO_INT;
    protected int totalFPGAs = ZERO_INT;
    protected int totalOtherComputingUnits = ZERO_INT;
    protected int totalOthers = ZERO_INT;

    // Memory
    protected float memorySize = UNASSIGNED_FLOAT;
    protected String memoryType = UNASSIGNED_STR;
    // Storage
    protected float storageSize = UNASSIGNED_FLOAT;
    protected String storageType = UNASSIGNED_STR;
    protected int storageBW = UNASSIGNED_INT;
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
     * ********************************************************************************************************
     * CONSTRUCTORS
     *********************************************************************************************************/
    /**
     * Creates a new empty MethodResourceDescription instance.
     */
    public MethodResourceDescription() {
        super();
    }

    /**
     * Internal creation of a MethodResourceDescription with an initial set of computing units {@code initialCUs}.
     *
     * @param initialCUs Initial number of computing units.
     */
    private MethodResourceDescription(int initialCUs) {
        super();

        // Add processor structures (with an empty non assigned processor)
        Processor p = new Processor();
        p.setComputingUnits(initialCUs);
        this.addProcessor(p); // Increases the total CUs
    }

    /**
     * Creates a MethodResourceDescription representing a set of constraints. The constraints are validated and loaded
     * through this process. If any error occurs an exception is raised to the user through the ErrorManager.
     *
     * @param constraints Java constraints.
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
        es.bsc.compss.types.annotations.Processor[] processorsConstraints = constraints.processors();
        if (processorsConstraints != null && processorsConstraints.length > 0) {
            for (es.bsc.compss.types.annotations.Processor pC : processorsConstraints) {
                Processor p = getProcessorFromProcessorsConstraint(pC);
                if (p != null) {
                    if (p.hasUnassignedCUs()) {
                        p.setComputingUnits(ONE_INT);

                    }
                    if (p.getComputingUnits() > 0) {
                        this.addProcessor(p);
                    }
                }
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

            // When loading from constraints, always use at least one computing unit
            int cu =
                (cuSTR != null && !cuSTR.isEmpty() && !cuSTR.equals(UNASSIGNED_STR)) ? Integer.valueOf(cuSTR) : ONE_INT;
            p.setComputingUnits(cu);

            String speedSTR = constraints.processorSpeed();
            speedSTR = EnvironmentLoader.loadFromEnvironment(speedSTR);
            float speed =
                (speedSTR != null && !speedSTR.isEmpty() && !speedSTR.equals(UNASSIGNED_STR)) ? Float.valueOf(speedSTR)
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
            float internalMemory =
                (internalMemorySTR != null && !internalMemorySTR.isEmpty() && !internalMemorySTR.equals(UNASSIGNED_STR))
                    ? Float.valueOf(internalMemorySTR)
                    : UNASSIGNED_FLOAT;
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
            if (p.hasUnassignedCUs()) {
                p.setComputingUnits(ONE_INT);
            }
            if (cu > 0) {
                this.addProcessor(p);
            }
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
            ? Float.valueOf(memorySizeSTR)
            : UNASSIGNED_FLOAT;
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
        float storageSize =
            (storageSizeSTR != null && !storageSizeSTR.isEmpty() && !storageSizeSTR.equals(UNASSIGNED_STR))
                ? Float.valueOf(storageSizeSTR)
                : UNASSIGNED_FLOAT;
        if (storageSize != UNASSIGNED_FLOAT) {
            this.storageSize = storageSize;
        }
        String storageType = constraints.storageType();
        storageType = EnvironmentLoader.loadFromEnvironment(storageType);
        if (storageType != null && !storageType.equals(UNASSIGNED_STR)) {
            this.storageType = storageType;
        }
        String storageBWSTR = constraints.storageBW();
        storageBWSTR = EnvironmentLoader.loadFromEnvironment(storageBWSTR);
        int storageBW = (storageBWSTR != null && !storageBWSTR.isEmpty() && !storageBWSTR.equals(UNASSIGNED_STR))
            ? Integer.valueOf(storageBWSTR)
            : UNASSIGNED_INT;
        if (storageBW != UNASSIGNED_INT) {
            this.storageBW = storageBW;
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
        int wallClockLimit =
            (wallClockLimitSTR != null && !wallClockLimitSTR.isEmpty() && !wallClockLimitSTR.equals(UNASSIGNED_STR))
                ? Integer.valueOf(wallClockLimitSTR)
                : UNASSIGNED_INT;
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

    private Processor
        getProcessorFromProcessorsConstraint(es.bsc.compss.types.annotations.Processor processorConstraints) {
        Processor p = new Processor();
        String procName = processorConstraints.name();
        if (procName != null && !procName.equals(UNASSIGNED_STR)) {
            procName = EnvironmentLoader.loadFromEnvironment(procName);
            p.setName(procName);
        }

        String cuSTR = processorConstraints.computingUnits();
        cuSTR = EnvironmentLoader.loadFromEnvironment(cuSTR);

        // When loading from constraints, always use at least one computing unit
        int cu =
            (cuSTR != null && !cuSTR.isEmpty() && !cuSTR.equals(UNASSIGNED_STR)) ? Integer.valueOf(cuSTR) : ONE_INT;
        p.setComputingUnits(cu);

        String speedSTR = processorConstraints.speed();
        speedSTR = EnvironmentLoader.loadFromEnvironment(speedSTR);
        float speed =
            (speedSTR != null && !speedSTR.isEmpty() && !speedSTR.equals(UNASSIGNED_STR)) ? Float.valueOf(speedSTR)
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
        float internalMemory =
            (internalMemorySTR != null && !internalMemorySTR.isEmpty() && !internalMemorySTR.equals(UNASSIGNED_STR))
                ? Float.valueOf(internalMemorySTR)
                : UNASSIGNED_FLOAT;
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
     * Creates a new MethodResourceDescription from the given Python description.
     *
     * @param description Python description.
     */
    public MethodResourceDescription(String description) {
        super();

        // Warning: When coming from constrains, only 1 PROCESSOR is available with at least 1 CU
        Processor proc = new Processor();
        if (description != null && !description.isEmpty()) {
            String[] constraints = description.split(";");
            for (String c : constraints) {
                int sepIndex = c.indexOf(":");
                if (sepIndex < 0) {
                    ErrorManager.error("ERROR: Unrecognised constraint " + c);
                    return;
                }
                String key = c.substring(0, sepIndex).trim().replaceAll("_", "");
                String val = c.substring(sepIndex + 1, c.length()).trim();
                if (key.equals(PROCESSORS)) {
                    treatProcessorsList(val);
                } else {
                    addConstraints(key, val, proc);
                }
            }
        }

        if (proc.isCPU()) {
            if (proc.isModified()) {
                if (proc.hasUnassignedCUs()) {
                    proc.setComputingUnits(ONE_INT);
                }
                if (proc.getComputingUnits() > 0) {
                    this.addProcessor(proc);
                }
            } else {
                if (this.totalCPUs == 0) {
                    // if processor has not been modified it must be added only if there are no CPU constraints already
                    // defined
                    proc.setComputingUnits(ONE_INT);
                    this.addProcessor(proc);
                }
            }
        } else {
            // If it is not CPU it must be added
            if (proc.hasUnassignedCUs()) {
                proc.setComputingUnits(ONE_INT);
            }
            if (proc.getComputingUnits() > 0) {
                this.addProcessor(proc);
            }
            if (this.totalCPUs == 0) {
                // Task require to use at least a CPU
                Processor p = new Processor();
                p.setComputingUnits(ONE_INT);
                this.addProcessor(p);
            } // else nothing to do
        }

    }

    private void treatProcessorsList(String processors) {
        // Format [{processor constraints}, {processor constraints}]
        if (processors.startsWith("[") && processors.endsWith("]")) {
            int procStartIndex = processors.indexOf("{");
            int procEndIndex = processors.indexOf("}");
            while (procStartIndex > 0) {
                if (procEndIndex > 0 && procEndIndex > procStartIndex) {
                    treatProcessorInList(processors.substring(procStartIndex + 1, procEndIndex));
                    procStartIndex = processors.indexOf("{", procEndIndex);
                    procEndIndex = processors.indexOf("}", procStartIndex);
                } else {
                    ErrorManager.error("ERROR: Unrecognised processor definition (processors)");
                    return;
                }
            }
        } else {
            ErrorManager.error("ERROR: Unrecognised processors list definition (" + processors + ")");
            return;
        }
    }

    private void treatProcessorInList(String processor) {
        String[] processorConstraints = processor.split(",");
        Processor proc = new Processor();
        for (int j = 0; j < processorConstraints.length; ++j) {
            String key = processorConstraints[j].split(":")[0].trim();
            String val = processorConstraints[j].split(":")[1].trim();
            addConstraints(key, val, proc);
        }
        // CU must be 1 if not defined
        if (proc.hasUnassignedCUs()) {
            proc.setComputingUnits(ONE_INT);
        }
        if (proc.getComputingUnits() > 0) {
            this.addProcessor(proc);
        }
    }

    /**
     * Creates a new MethodResourceDescription from the given C constraints.
     *
     * @param constraints C constraints.
     * @param processorString C processor definition.
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
                Processor currentProc = new Processor();
                for (int j = 0; j < processorConstraints.length; ++j) {
                    String key = processorConstraints[j].split("=")[0].trim();
                    String val = processorConstraints[j].split("=")[1].trim();
                    addConstraints(key, val, currentProc);
                }
                if (currentProc.hasUnassignedCUs()) {
                    currentProc.addComputingUnits(ONE_INT);
                }
                if (currentProc.getComputingUnits() > 0) {
                    this.addProcessor(currentProc);
                }
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

    /**
     * Adds a processor constraint from bindings (ignores letter case).
     *
     * @param key Constraint key.
     * @param val Constraint value.
     * @param proc Processor.
     */
    private void addConstraints(String key, String val, Processor proc) {
        val = EnvironmentLoader.loadFromEnvironment(val);

        if (val != null && !val.isEmpty()) {
            switch (key.toLowerCase()) {
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
                case STORAGE_BW:
                    this.storageBW = Integer.valueOf(val);
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
                default:
                    ErrorManager.error("ERROR: Unrecognised constraint " + key);
                    return;
            }
        } else {
            // Set the default values
            switch (key.toLowerCase()) {
                case COMPUTING_UNITS:
                    proc.setComputingUnits(ONE_INT);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Copies the given MethodResourceDescription.
     *
     * @param clone MethodResourceDescription to clone.
     */
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
        this.storageBW = clone.storageBW;

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
        this.totalOtherComputingUnits = 0;
        this.totalOthers = 0;
    }

    @Override
    public MethodResourceDescription copy() {
        return new MethodResourceDescription(this);
    }

    /*
     * ************************************************************************************************************
     * GETTERS AND SETTERS
     *************************************************************************************************************/
    /**
     * Sets computingUnits and totalCPUs to zero for IO tasks.
     */
    public void setIOResources() {
        this.totalCPUComputingUnits = 0;
        this.totalCPUs = 0;

        this.processors.clear();
    }

    /**
     * Returns whether a method uses CPUs or not.
     *
     * @return true if the method uses CPUs, false otherwise.
     */
    public boolean usesCPUs() {
        return (!this.getProcessors().isEmpty() && this.getTotalCPUComputingUnits() != 0);
    }

    /**
     * Returns the registered processors.
     *
     * @return A list containing the registered processors.
     */
    @XmlElementWrapper(name = "processors")
    @XmlElement(name = "processor")
    public List<Processor> getProcessors() {
        return this.processors;
    }

    /**
     * Registers a new list of processors.
     *
     * @param processors New list of processors.
     */
    public void setProcessors(List<Processor> processors) {
        this.processors = processors;

        initProcessorCounters();
        for (Processor p : this.processors) {
            updateCounters(p);
        }
    }

    /**
     * Resets the registered processors.
     */
    public void resetProcessors() {
        this.processors = new LinkedList<>();
        initProcessorCounters();
    }

    /**
     * Adds a new processor {@code p}.
     *
     * @param p New processor.
     */
    public void addProcessor(Processor p) {
        this.processors.add(p);
        updateCounters(p);
    }

    /**
     * Adds a new Processor to the current resource with the given information.
     *
     * @param procName Processor name.
     * @param computingUnits Processor computing units.
     * @param architecture Processor architecture.
     * @param speed Processor speed.
     * @param type Processor type.
     * @param internalMemory Processor internal memory.
     * @param propName Processor custom property name.
     * @param propValue Processor custom property value.
     */
    public void addProcessor(String procName, int computingUnits, String architecture, float speed, String type,
        float internalMemory, String propName, String propValue) {

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

    /**
     * Updates the resource global counters.
     *
     * @param p Processor capabilities to be added in the global counters.
     */
    private void updateCounters(Processor p) {
        int cu = p.getComputingUnits();
        switch (p.getType()) {
            case CPU:
                if (cu > 0) {
                    totalCPUComputingUnits += cu;
                    totalMPIioComputingUnits += cu;

                } else {
                    totalCPUComputingUnits++;
                    totalMPIioComputingUnits++;
                }
                totalCPUs++;
                break;
            case GPU:
                if (cu > 0) {
                    totalGPUComputingUnits += cu;

                } else {
                    totalGPUComputingUnits++;
                }
                totalGPUs++;
                break;
            case FPGA:
                if (cu > 0) {
                    totalFPGAComputingUnits += cu;
                } else {
                    totalFPGAComputingUnits++;
                }
                totalFPGAs++;
                break;
            default:
                if (cu > 0) {
                    totalOtherComputingUnits += cu;
                } else {
                    totalOtherComputingUnits++;
                }
                totalOthers++;
        }
    }

    /**
     * Returns the registered architectures.
     *
     * @return A list containing the registered architectures.
     */
    public List<String> getArchitectures() {
        LinkedList<String> architectures = new LinkedList<>();

        for (Processor p : this.processors) {
            String arch = p.getArchitecture();
            if (!arch.equals(UNASSIGNED_STR)) {
                architectures.add(arch);
            } else {
                if (!architectures.contains(UNASSIGNED_STR)) {
                    // Only add once
                    architectures.add(UNASSIGNED_STR);
                }
            }
        }

        return architectures;
    }

    /**
     * Returns the total number of CPU computing units.
     *
     * @return The total number of CPU computing units.
     */
    public int getTotalCPUComputingUnits() {
        return this.totalCPUComputingUnits;
    }

    /**
     * Returns the total number of MPI computing units.
     *
     * @return The total number of MPI computing units.
     */
    public int getTotalMPIComputingUnits() {
        return this.totalMPIioComputingUnits;
    }

    /**
     * Returns the total number of GPU computing units.
     *
     * @return The total number of GPU computing units.
     */
    public int getTotalGPUComputingUnits() {
        return this.totalGPUComputingUnits;
    }

    /**
     * Returns the total number of FPGA computing units.
     *
     * @return The total number of FPGA computing units.
     */
    public int getTotalFPGAComputingUnits() {
        return this.totalFPGAComputingUnits;
    }

    /**
     * Returns the total number of OTHER computing units.
     *
     * @return The total number of OTHER computing units.
     */
    public int getTotalOTHERComputingUnits() {
        return this.totalOtherComputingUnits;
    }

    /**
     * Returns the memory size.
     *
     * @return The memory size.
     */
    public float getMemorySize() {
        return memorySize;
    }

    /**
     * Sets a new memory size.
     *
     * @param memorySize New memory size.
     */
    public void setMemorySize(float memorySize) {
        if (memorySize > (float) 0.0) {
            this.memorySize = memorySize;
        }
    }

    /**
     * Returns the memory type.
     *
     * @return The memory type.
     */
    public String getMemoryType() {
        return memoryType;
    }

    /**
     * Sets a new memory type.
     *
     * @param memoryType New memory type.
     */
    public void setMemoryType(String memoryType) {
        if (memoryType != null && !memoryType.isEmpty()) {
            this.memoryType = memoryType;
        }
    }

    /**
     * Returns the storage size.
     *
     * @return The storage size.
     */
    public float getStorageSize() {
        return storageSize;
    }

    /**
     * Sets a new storage size.
     *
     * @param storageSize New storage size.
     */
    public void setStorageSize(float storageSize) {
        if (storageSize > 0) {
            this.storageSize = storageSize;
        }
    }

    /**
     * Returns the storage type.
     *
     * @return The storage type.
     */
    public String getStorageType() {
        return storageType;
    }

    /**
     * Sets a new storage type.
     *
     * @param storageType New storage type.
     */
    public void setStorageType(String storageType) {
        if (storageType != null && !storageType.isEmpty()) {
            this.storageType = storageType;
        }
    }

    /**
     * Returns the storage bandwidth.
     *
     * @return The storage bandwidth.
     */
    public int getStorageBW() {
        return storageBW;
    }

    /**
     * Sets a new storage bandwidth.
     *
     * @param storageBW New storage bandwidth.
     */
    public void setStorageBW(int storageBW) {
        if (storageBW > 0) {
            this.storageBW = storageBW;
        }
    }

    /**
     * Returns the operating system type.
     *
     * @return The operating system type.
     */
    public String getOperatingSystemType() {
        return operatingSystemType;
    }

    /**
     * Sets a new operating system type.
     *
     * @param operatingSystemType New operating system type.
     */
    public void setOperatingSystemType(String operatingSystemType) {
        if (operatingSystemType != null && !operatingSystemType.isEmpty()) {
            this.operatingSystemType = operatingSystemType;
        }
    }

    /**
     * Returns the operating system distribution.
     *
     * @return The operating system distribution.
     */
    public String getOperatingSystemDistribution() {
        return operatingSystemDistribution;
    }

    /**
     * Sets a new operating system distribution.
     *
     * @param operatingSystemDistribution New operating system distribution.
     */
    public void setOperatingSystemDistribution(String operatingSystemDistribution) {
        if (operatingSystemDistribution != null && !operatingSystemDistribution.isEmpty()) {
            this.operatingSystemDistribution = operatingSystemDistribution;
        }
    }

    /**
     * Returns the operating system version.
     *
     * @return The operating system version.
     */
    public String getOperatingSystemVersion() {
        return operatingSystemVersion;
    }

    /**
     * Sets a new operating system version.
     *
     * @param operatingSystemVersion New operating system version.
     */
    public void setOperatingSystemVersion(String operatingSystemVersion) {
        if (operatingSystemVersion != null && !operatingSystemVersion.isEmpty()) {
            this.operatingSystemVersion = operatingSystemVersion;
        }
    }

    /**
     * Returns the registered application software.
     *
     * @return List containing the registered application software.
     */
    public List<String> getAppSoftware() {
        return appSoftware;
    }

    /**
     * Sets a new list of supported application software.
     *
     * @param appSoftware New list of supported application software.
     */
    public void setAppSoftware(List<String> appSoftware) {
        if (appSoftware != null) {
            this.appSoftware = appSoftware;
        }
    }

    /**
     * Resets the list of application software.
     */
    public void resetAppSoftware() {
        appSoftware = new LinkedList<>();
    }

    /**
     * Adds a new application to the supported application software list.
     *
     * @param application New application name.
     */
    public void addApplication(String application) {
        if (application != null && !application.isEmpty()) {
            appSoftware.add(application.toUpperCase());
        }
    }

    /**
     * Returns the registered host queues.
     *
     * @return A list containing the registered host queues.
     */
    public List<String> getHostQueues() {
        return hostQueues;
    }

    /**
     * Sets a new list of registered host queues.
     *
     * @param hostQueues New list of registered host queues.
     */
    public void setHostQueues(List<String> hostQueues) {
        if (hostQueues != null) {
            this.hostQueues = hostQueues;
        }
    }

    /**
     * Resets the list of registered host queues.
     */
    public void resetHostQueues() {
        hostQueues = new LinkedList<>();
    }

    /**
     * Adds a new host queue to the registered host queues.
     *
     * @param queue New host queue.
     */
    public void addHostQueue(String queue) {
        if (queue != null && !queue.isEmpty()) {
            hostQueues.add(queue.toUpperCase());
        }
    }

    /**
     * Returns the time unit of the price slot.
     *
     * @return The time unit of the price slot.
     */
    public int getPriceTimeUnit() {
        return priceTimeUnit;
    }

    /**
     * Sets a new time unit for the price slot.
     *
     * @param priceTimeUnit New time unit for the price slot (in seconds).
     */
    public void setPriceTimeUnit(int priceTimeUnit) {
        if (priceTimeUnit > 0) {
            this.priceTimeUnit = priceTimeUnit;
        }
    }

    /**
     * Returns the price per time unit.
     *
     * @return The price per time unit.
     */
    public float getPricePerUnit() {
        return pricePerUnit;
    }

    /**
     * Sets a new price per time unit.
     *
     * @param pricePerUnit New price per time unit.
     */
    public void setPricePerUnit(float pricePerUnit) {
        if (priceTimeUnit > (float) 0.0) {
            this.pricePerUnit = pricePerUnit;
        }
    }

    /**
     * Returns the wallclock time.
     *
     * @return The wallclock time.
     */
    public int getWallClockLimit() {
        return wallClockLimit;
    }

    /**
     * Sets a new wallclock time.
     *
     * @param wallClockLimit The new wallclock time.
     */
    public void setWallClockLimit(int wallClockLimit) {
        if (wallClockLimit > 0) {
            this.wallClockLimit = wallClockLimit;
        }
    }

    /**
     * Returns the internal value.
     *
     * @return The internal value.
     */
    public Float getValue() {
        return value;
    }

    /**
     * Sets the internal value.
     *
     * @param value The new internal value.
     */
    public void setValue(Float value) {
        this.value = value;
    }

    /**
     * Returns whether the resource contains CPU processors or not.
     *
     * @return {@code true} if there are CPU computing units, {@code false} otherwise.
     */
    public boolean containsCPU() {
        return getTotalCPUComputingUnits() > 0;
    }

    /**
     * Returns whether the resource contains GPU processors or not.
     *
     * @return {@code true} if there are GPU computing units, {@code false} otherwise.
     */
    public boolean containsGPU() {
        return getTotalGPUComputingUnits() > 0;
    }

    /**
     * Returns whether the resource contains FPGA processors or not.
     *
     * @return {@code true} if there are FPGA computing units, {@code false} otherwise.
     */
    public boolean containsFPGA() {
        return getTotalFPGAComputingUnits() > 0;
    }

    /**
     * Returns whether the resource contains OTHER processors or not.
     *
     * @return {@code true} if there are OTHER computing units, {@code false} otherwise.
     */
    public boolean containsOthers() {
        return getTotalOTHERComputingUnits() > 0;
    }

    /*
     * ************************************************************************************************************
     * METHOD-RESOURCE OPERATIONS
     *************************************************************************************************************/
    /**
     * This method tries to substitute the implicit default values by defined mr2 values. Keeps the already defined
     * values (does NOT overwrite). IT IS ONLY CALLED FROM CONSTRAINTS (1 processor always).
     *
     * @param mr2 MethodResourceDescription to merge.
     */
    public void mergeMultiConstraints(MethodResourceDescription mr2) {
        // Processor constraints
        for (Processor pmr2 : mr2.processors) {
            ProcessorType type = pmr2.getType();
            Processor pthis = lookForProcessorType(type);
            if (pthis != null) {
                if (pthis.getComputingUnits() <= ONE_INT) {
                    int newCus = pmr2.getComputingUnits();
                    int currentCUs = pthis.getComputingUnits();
                    pthis.setComputingUnits(newCus);
                    switch (type) {
                        case CPU:
                            this.totalCPUComputingUnits += (newCus - currentCUs);
                            break;
                        case GPU:
                            this.totalGPUComputingUnits += (newCus - currentCUs);
                            break;
                        case FPGA:
                            this.totalFPGAComputingUnits += (newCus - currentCUs);
                            break;
                        default:
                            this.totalOtherComputingUnits += (newCus - currentCUs);
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
        if (this.storageBW == UNASSIGNED_INT) {
            this.setStorageBW(mr2.getStorageBW());
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

    private Processor lookForProcessorType(ProcessorType type) {
        for (Processor p : this.processors) {
            if (p.getType() == type) {
                return p;
            }
        }
        return null;
    }

    /**
     * Returns the difference in terms of computingUnits and memory with the given MethodResourceDescription
     * {@code mr2}.
     *
     * @param mr2 MethodResourceDescription to compare with.
     * @return Difference value.
     */
    public float difference(MethodResourceDescription mr2) {
        float processorDif = this.getTotalCPUComputingUnits() - mr2.getTotalCPUComputingUnits();
        float otherProcessorsDif = (this.getTotalGPUComputingUnits() + this.getTotalFPGAComputingUnits()
            + this.getTotalOTHERComputingUnits())
            - (mr2.getTotalGPUComputingUnits() + mr2.getTotalFPGAComputingUnits() + mr2.getTotalOTHERComputingUnits());
        float memoryDif = this.memorySize - mr2.memorySize;

        return (processorDif * DIFFERENCE_WEIGHT) + (otherProcessorsDif * OTHER_PROC_DIFFERENCE_WEIGHT) + memoryDif;
    }

    /**
     * Returns whether the current MethodResourceDescription contains the given MethodResourceDescription {@code rc2}.
     *
     * @param rc2 MethodResourceDescription to compare with.
     * @return {@code true} if the current MethodResourceDescription contains the given MethodResourceDescription
     *         {@code rc2}, {@code false} otherwise.
     */
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

    /**
     * Returns whether the current MethodResourceDescription contains the DYNAMIC constraints of the given
     * MethodResourceDescription {@code rc2}.
     *
     * @param rc2 MethodResourceDescription to compare with.
     * @return {@code true} if the current MethodResourceDescription contains the DYNAMIC constraints of the given
     *         MethodResourceDescription {@code rc2}, {@code false} otherwise.
     */
    public boolean containsDynamic(MethodResourceDescription rc2) {
        boolean contained = checkProcessors(rc2);
        contained = contained && checkMemory(rc2);
        contained = contained && checkStorage(rc2);
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
        return checkInclusion(this.storageSize, rc2.storageSize)
            && checkCompatibility(this.storageType, rc2.storageType) && checkInclusion(this.storageBW, rc2.storageBW);
    }

    private boolean checkCompatibility(ProcessorType type1, ProcessorType type2) {
        return type1 == type2;
    }

    private boolean checkCompatibility(String value1, String value2) {
        return value1.equals(value2) || value1.equals(UNASSIGNED_STR) || value2.equals(UNASSIGNED_STR);
    }

    private boolean checkInclusion(int value1, int value2) {
        return value1 >= value2 || value1 == UNASSIGNED_INT || value2 == UNASSIGNED_INT;
    }

    private boolean checkInclusion(float value1, float value2) {
        return value1 >= value2 || value1 == UNASSIGNED_FLOAT || value2 == UNASSIGNED_FLOAT;
    }

    /**
     * Returns the number of simultaneous executions of {@code rc2} that the current MethodResourceDescription can host.
     *
     * @param rc2 MethodResourceDescription to host simultaneously.
     * @return Maximum number of simultaneous executions.
     */
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
                    float ratio = 0;
                    if (p.getComputingUnits() == 0) {
                        ratio = pThis.getComputingUnits();
                    } else {
                        ratio = pThis.getComputingUnits() / p.getComputingUnits();
                    }
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

    /**
     * Scales the current MethodResourceDescription by the {@code amount} factor.
     *
     * @param amount Scaling factor.
     * @return New scaled MethodResourceDescription.
     */
    public MethodResourceDescription multiply(int amount) {
        MethodResourceDescription rd = new MethodResourceDescription();

        // Processors
        for (Processor p : this.processors) {
            Processor newP = new Processor(p);
            newP.multiply(amount);
            rd.addProcessor(newP);
        }

        // Memory
        rd.memorySize = this.memorySize * amount;

        // Storage
        rd.storageSize = this.storageSize * amount;
        rd.storageBW = this.storageBW * amount;

        return rd;
    }

    @Override
    public void mimic(ResourceDescription rd) {
        this.processors.clear();
        this.totalCPUComputingUnits = 0;
        this.totalGPUComputingUnits = 0;
        this.totalFPGAComputingUnits = 0;
        this.totalOtherComputingUnits = 0;

        MethodResourceDescription mrd2 = (MethodResourceDescription) rd;
        for (Processor p2 : mrd2.getProcessors()) {
            this.addProcessor(new Processor(p2));
        }

        // Memory
        this.memorySize = mrd2.memorySize;
        this.memoryType = mrd2.memoryType;

        // Storage
        this.storageSize = mrd2.storageSize;
        this.storageType = mrd2.storageType;
        this.storageBW = mrd2.storageBW;

        // OperatingSystem
        this.operatingSystemType = mrd2.operatingSystemType;
        this.operatingSystemDistribution = mrd2.operatingSystemDistribution;
        this.operatingSystemVersion = mrd2.operatingSystemVersion;

        // Applications
        this.appSoftware.clear();
        for (String app : mrd2.appSoftware) {
            this.appSoftware.add(app);
        }

        // Host queues
        this.hostQueues.clear();
        for (String queue : mrd2.hostQueues) {
            this.hostQueues.add(queue);
        }

        // Price
        this.pricePerUnit = mrd2.pricePerUnit;
        this.priceTimeUnit = mrd2.priceTimeUnit;

        // WallClock limit
        this.wallClockLimit = mrd2.wallClockLimit;

        // Internal fields
        this.value = mrd2.value;
    }

    // This method expands the implicit value with the ones defined by mr2
    // Keeps the unique values and gets the maximum of the common values
    @Override
    public void increase(ResourceDescription rd2) {
        MethodResourceDescription mrd2 = (MethodResourceDescription) rd2;

        // Increase Processors and Memory and Storage Bandwidth
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

        // Increase Processors and Memory and Storage Bandwidth
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
        if (mrd2.appSoftware == this.appSoftware) {
            // this is the case when it is reducing itself (avoid concurrent modification)
            this.appSoftware.clear();
        } else {
            for (String app : mrd2.appSoftware) {
                if (this.appSoftware.contains(app)) {
                    this.appSoftware.remove(app);
                }
            }
        }
        // Host queues
        if (mrd2.hostQueues == this.hostQueues) {
            // this is the case when it is reducing itself (avoid concurrent modification)
            this.hostQueues.clear();
        } else {
            for (String queue : mrd2.hostQueues) {
                if (this.hostQueues.contains(queue)) {
                    this.hostQueues.remove(queue);
                }
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

        // Storage Bandwidth
        if ((this.storageBW != UNASSIGNED_INT) && (mrd2.storageBW != UNASSIGNED_INT)) {
            this.storageBW += mrd2.storageBW;
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
            Iterator<Processor> processors = this.processors.iterator();
            while (processors.hasNext() && !processorMerged) {
                Processor pThis = processors.next();
                if (checkProcessorCompatibility(pThis, p)) {
                    processorMerged = true;
                    int cus = p.getComputingUnits();

                    // Copy the real decreased capabilities
                    Processor pReduced = new Processor(pThis);
                    pReduced.setComputingUnits(cus);
                    reduced.addProcessor(pReduced);

                    // Decrease current
                    pThis.removeComputingUnits(cus);
                    this.decreaseComputingUnits(pThis.getType(), cus);

                    if (pThis.getComputingUnits() == 0) {
                        processors.remove();
                    }

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

        // Storage
        if (checkStorage(mrd2)) {
            // Copy the real decreased capabilities
            reduced.setStorageType(this.storageType);
            if ((mrd2.storageBW == UNASSIGNED_INT) || (this.storageBW == UNASSIGNED_INT)) {
                reduced.setStorageBW(UNASSIGNED_INT);
            } else {
                this.storageBW -= mrd2.storageBW;
                reduced.setStorageBW(mrd2.storageBW);
            }
        } else {
            // The reduce is invalid
            return null;
        }

        // Return the real decreased capabilities
        return reduced;
    }

    private void increaseComputingUnits(ProcessorType type, int cus) {
        switch (type) {
            case CPU:
                this.totalCPUComputingUnits += cus;
                break;
            case GPU:
                this.totalGPUComputingUnits += cus;
                break;
            case FPGA:
                this.totalFPGAComputingUnits += cus;
                break;
            default:
                this.totalOtherComputingUnits += cus;
        }
    }

    private void decreaseComputingUnits(ProcessorType type, int cus) {
        switch (type) {
            case CPU:
                this.totalCPUComputingUnits -= cus;
                break;
            case GPU:
                this.totalGPUComputingUnits -= cus;
                break;
            case FPGA:
                this.totalFPGAComputingUnits -= cus;
                break;
            default:
                this.totalOtherComputingUnits -= cus;
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
                    Processor commonProcessor = getDynamicCommonsProcessor(pThis, p);
                    if (commonProcessor.getComputingUnits() > 0) {
                        common.addProcessor(commonProcessor);
                    }
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

        // Storage
        // Only checks compatibility, not inclusion
        if (checkCompatibility(this.storageType, otherMRD.storageType)) {
            // Copy the assignable storage type (no the requested)
            common.setStorageType(this.getStorageType());
            common.setStorageSize(Math.min(this.storageSize, otherMRD.getStorageSize()));
            common.setStorageBW(Math.min(this.storageBW, otherMRD.getStorageBW()));
        }

        return common;
    }

    /*
     * ************************************************************************************************************
     * EXTERNALIZATION
     *************************************************************************************************************/
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.processors = (List<Processor>) in.readObject();
        this.totalCPUComputingUnits = in.readInt();
        this.totalMPIioComputingUnits = in.readInt();
        this.totalCPUs = in.readInt();
        this.totalGPUComputingUnits = in.readInt();
        this.totalGPUs = in.readInt();
        this.totalFPGAComputingUnits = in.readInt();
        this.totalFPGAs = in.readInt();
        this.totalOtherComputingUnits = in.readInt();
        this.totalOthers = in.readInt();
        this.memorySize = in.readFloat();
        this.memoryType = (String) in.readObject();
        this.storageSize = in.readFloat();
        this.storageType = (String) in.readObject();
        this.storageBW = in.readInt();
        this.operatingSystemType = (String) in.readObject();
        this.operatingSystemDistribution = (String) in.readObject();
        this.operatingSystemVersion = (String) in.readObject();
        this.appSoftware = (List<String>) in.readObject();
        this.hostQueues = (List<String>) in.readObject();
        this.priceTimeUnit = in.readInt();
        this.pricePerUnit = in.readFloat();
        this.wallClockLimit = in.readInt();
        this.value = in.readFloat();

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.processors);
        out.writeInt(this.totalCPUComputingUnits);
        out.writeInt(this.totalMPIioComputingUnits);
        out.writeInt(this.totalCPUs);
        out.writeInt(this.totalGPUComputingUnits);
        out.writeInt(this.totalGPUs);
        out.writeInt(this.totalFPGAComputingUnits);
        out.writeInt(this.totalFPGAs);
        out.writeInt(this.totalOtherComputingUnits);
        out.writeInt(this.totalOthers);
        out.writeFloat(this.memorySize);
        out.writeObject(this.memoryType);
        out.writeFloat(this.storageSize);
        out.writeObject(this.storageType);
        out.writeInt(this.storageBW);
        out.writeObject(this.operatingSystemType);
        out.writeObject(this.operatingSystemDistribution);
        out.writeObject(this.operatingSystemVersion);
        out.writeObject(this.appSoftware);
        out.writeObject(this.hostQueues);
        out.writeInt(this.priceTimeUnit);
        out.writeFloat(this.pricePerUnit);
        out.writeInt(this.wallClockLimit);
        out.writeFloat(this.value);
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
        return (this.getMemorySize() <= 0.0 && this.totalCPUComputingUnits < 1 && this.totalGPUComputingUnits < 1
            && this.getStorageBW() <= 0);
    }

    @Override
    public boolean isDynamicConsuming() {
        return (this.getMemorySize() > 0.0 || this.totalCPUComputingUnits > 0 || this.totalGPUComputingUnits > 0
            || this.getStorageBW() > 0);
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
     * ************************************************************************************************************
     * LOGGERS
     ************************************************************************************************************/

    protected void dumpContent(StringBuilder sb) {
        sb.append("\"processors\":");
        dumpProcessors(sb);
        sb.append(",");

        sb.append("\"memory\":{");
        sb.append("\"size\":").append(this.memorySize).append(",");
        sb.append("\"type\":\"").append(this.memoryType).append("\"");
        sb.append("},");

        sb.append("\"storage\":{");
        sb.append("\"size\":").append(this.storageSize).append(",");
        sb.append("\"type\":\"").append(this.storageType).append("\",");
        sb.append("\"bandwidth\":").append(this.storageBW);
        sb.append("},");

        sb.append("\"operating_system\":{");
        sb.append("\"type\":\"").append(this.operatingSystemType).append("\",");
        sb.append("\"distribution\":\"").append(this.operatingSystemDistribution).append("\",");
        sb.append("\"version\":\"").append(this.operatingSystemVersion).append("\"");
        sb.append("},");

        sb.append("\"software\":[");
        Iterator<String> swItr = this.appSoftware.iterator();
        if (swItr.hasNext()) {
            sb.append("\"").append(swItr.next()).append("\"");
        }
        while (swItr.hasNext()) {
            sb.append(",\"").append(swItr.next()).append("\"");
        }
        sb.append("],");

        sb.append("\"host_queues\":[");
        Iterator<String> queuesItr = this.hostQueues.iterator();
        if (queuesItr.hasNext()) {
            sb.append("\"").append(queuesItr.next()).append("\"");
        }
        while (queuesItr.hasNext()) {
            sb.append(",\"").append(queuesItr.next()).append("\"");
        }
        sb.append("],");

        sb.append("\"price\":{");
        sb.append("\"time_unit\":").append(this.priceTimeUnit).append(",");
        sb.append("\"price_per_unit\":").append(this.pricePerUnit);
        sb.append("},");

        sb.append("\"wallclock\":").append(this.wallClockLimit);

    }

    private void dumpProcessors(StringBuilder sb) {
        sb.append("[");
        Iterator<Processor> procItr = this.processors.iterator();
        Processor p;
        if (procItr.hasNext()) {
            p = procItr.next();
            sb.append(p.toString());
        }
        while (procItr.hasNext()) {
            sb.append(",");
            p = procItr.next();
            sb.append(p.toString());
        }
        sb.append("],");
        sb.append("\"general_counts\":{");
        sb.append("\"total_cpus\":").append(this.totalCPUs).append(",");
        sb.append("\"total_cpu_cu\":").append(this.totalCPUComputingUnits).append(",");
        sb.append("\"total_gpus\":").append(this.totalGPUs).append(",");
        sb.append("\"total_gpu_cu\":").append(this.totalGPUComputingUnits).append(",");
        sb.append("\"total_fpgas\":").append(this.totalFPGAs).append(",");
        sb.append("\"total_fpga_cu\":").append(this.totalFPGAComputingUnits).append(",");
        sb.append("\"total_others\":").append(this.totalOthers).append(",");
        sb.append("\"total_other_cu\":").append(this.totalOtherComputingUnits);
        sb.append("}");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        this.dumpContent(sb);
        sb.append("}");

        return sb.toString();
    }

    @Override
    public String getDynamicDescription() {
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"processors\":[");
        // Processors
        Iterator<Processor> procItr = this.processors.iterator();
        Processor p;
        if (procItr.hasNext()) {
            p = procItr.next();
            sb.append("{\"computing_units\":").append(p.getComputingUnits()).append(",");
            sb.append("\"architecture\":\"").append(p.getArchitecture()).append("\"}");
        }
        while (procItr.hasNext()) {
            p = procItr.next();
            sb.append(",{\"computing_units\":").append(p.getComputingUnits()).append(",");
            sb.append("\"architecture\":\"").append(p.getArchitecture()).append("\"}");
        }
        sb.append("]");

        // Memory
        sb.append(",\"memory\":");
        if (this.memorySize != UNASSIGNED_FLOAT) {
            sb.append(this.memorySize);
        } else {
            sb.append("\"Unassigned\"");
        }

        // Storage Bandwidth
        sb.append(",\"storage_bandwidth\":");
        if (this.storageBW != UNASSIGNED_INT) {
            sb.append(this.storageBW);
        } else {
            sb.append("\"Unassigned\"");
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public void scaleUpBy(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("ERROR: Trying to scale by 0 or negative");
        } else if (n > 1) {
            for (Processor pThis : this.processors) {
                int oldCUs = pThis.getComputingUnits();
                int newCUs = oldCUs * n;
                pThis.setComputingUnits(newCUs);
                this.increaseComputingUnits(pThis.getType(), newCUs - oldCUs);
            }

            if (this.storageBW != UNASSIGNED_INT) {
                this.storageBW = this.storageBW * n;
            }
            if (this.memorySize != UNASSIGNED_FLOAT) {
                this.memorySize = this.memorySize * n;
            }
        }

    }

    @Override
    public void scaleDownBy(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("ERROR: Trying to scale by 0 or negative");
        } else if (n > 1) {
            for (Processor pThis : this.processors) {
                int oldCUs = pThis.getComputingUnits();
                int newCUs = oldCUs / n;
                pThis.setComputingUnits(newCUs);
                this.decreaseComputingUnits(pThis.getType(), oldCUs - newCUs);
            }
            if (this.storageBW != UNASSIGNED_INT) {
                this.storageBW = this.storageBW / n;
            }
            if (this.memorySize != UNASSIGNED_FLOAT) {
                this.memorySize = this.memorySize / n;
            }
        }

    }


    /// Enum to quickly distinguish superclass MethodResourceDescription and
    // subclass ClusterMethodResourceDescription
    protected enum MethodResourceType {
        METHOD, CLUSTER
    }


    /**
     * Returns type of method Resource description.
     *
     * @return the method type
     */
    public MethodResourceType getMethodType() {
        return MethodResourceType.METHOD;
    }

}
