package integratedtoolkit.types.resources;

import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Implementation.Type;
import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.resources.components.Processor;

import java.util.LinkedList;
import java.util.List;


public class MethodResourceDescription extends WorkerResourceDescription {

	// Empty Resource Description
    public static final MethodResourceDescription EMPTY = new MethodResourceDescription();
    
    // Unassigned values 
    // !!!!!!!!!! WARNING: Coherent with constraints class
    public static final String UNASSIGNED_STR 	= "[unassigned]";
    public static final int UNASSIGNED_INT 		= -1;
    public static final float UNASSIGNED_FLOAT 	= (float) -1.0;
    
    /* Tags for key-value string constraints description *****************/
    // !!!!!!!!!! WARNING: Coherent with constraints class
    public static final String PROC_NAME 			= "ProcessorName";
    public static final String COMPUTING_UNITS		= "ComputingUnits";
    public static final String PROC_SPEED 			= "ProcessorSpeed";
    public static final String PROC_ARCH	 		= "ProcessorArchitecture";
    public static final String PROC_PROP_NAME		= "ProcessorPropertyName";
    public static final String PROC_PROP_VALUE 		= "ProcessorPropertyValue";
    public static final String MEM_SIZE 			= "MemorySize";
    public static final String MEM_TYPE 			= "MemoryType";
    public static final String STORAGE_SIZE 		= "StorageSize";
    public static final String STORAGE_TYPE 		= "StorageType";
    public static final String OS_TYPE 				= "OperatingSystemType";
    public static final String OS_DISTRIBUTION		= "OperatingSystemDistribution";
    public static final String OS_VERSION			= "OperatingSystemVersion";
    public static final String APP_SOFTWARE 		= "AppSoftware";
    public static final String HOST_QUEUES			= "HostQueues";
    public static final String WALL_CLOCK_LIMIT 	= "WallClockLimit";
    
    /* Resource Description properties ***********************************/
    // Processor
    protected List<Processor> processors;
    protected int totalComputingUnits = 0;
    
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
    protected List<String> appSoftware;
    // Host queues
    protected List<String> hostQueues;
    // Price
    protected int priceTimeUnit = UNASSIGNED_INT;
    protected float pricePerUnit = UNASSIGNED_FLOAT;
    // WallClock Limit (from constraints, not from XML files)
    protected int wallClockLimit = UNASSIGNED_INT;
    
    /* Internal fields ***************************************************/
    protected int slots = 0;
    protected Float value = 0.0f;
    
    
    /* *******************************************
     * CONSTRUCTORS
     * *******************************************/
    public MethodResourceDescription() {
        super();
        this.processors = new LinkedList<Processor>();    
        this.appSoftware = new LinkedList<String>();
        this.hostQueues = new LinkedList<String>();
    }

    public MethodResourceDescription(Constraints constraints) {
    	super();
    	this.processors = new LinkedList<Processor>();    
        this.appSoftware = new LinkedList<String>();
        this.hostQueues = new LinkedList<String>();
        
        // Parse processors - When comming from Constraints only one processor is available
        Processor p = new Processor();
        String procName = constraints.processorName();
        if (procName != null && !procName.equals(UNASSIGNED_STR)) {
        	p.setName(procName);
        }
        int cu = constraints.computingUnits();
        if (cu != UNASSIGNED_INT) {
        	p.setComputingUnits(cu);
        	totalComputingUnits = cu;
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
            for (String value : software.split(",")){
                this.appSoftware.add(value.trim());
            } 
        }
        
        // Parse queues
        String queues = constraints.hostQueues();
        if (queues != null && !queues.equals(UNASSIGNED_STR)) {
        	for (String value : queues.split(",")){
                this.hostQueues.add(value.trim());
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
    
    public MethodResourceDescription(String description){
    	super();
    	this.processors = new LinkedList<Processor>();    
        this.appSoftware = new LinkedList<String>();
        this.hostQueues = new LinkedList<String>();
        
        // Warning: When comming from contrains, only 1 PROCESSOR is available
        Processor proc = new Processor();
        String[] constraints = description.split(";");
        for(String c : constraints){
            String key = c.split(":")[0].trim();
            String val = c.split(":")[1].trim();
            if (val != null && !val.isEmpty()) {
	            switch (key) {
		            case PROC_NAME:
		            	proc.setName(val);
		                break;
		            case PROC_SPEED:
		                proc.setSpeed( Float.valueOf(val) );
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
		            	int cu = Integer.valueOf(val);
		                proc.setComputingUnits(cu);
		                totalComputingUnits = cu;
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
		                    for (String app : val.split(",")){
		                        this.appSoftware.add(app.trim());
		                    } 
		                }
		                break;
		            case HOST_QUEUES:
		            	if (val.compareTo(UNASSIGNED_STR) != 0) {
		                    for (String app : val.split(",")){
		                        this.hostQueues.add(app.trim());
		                    } 
		                }
		                break;
		            case WALL_CLOCK_LIMIT:
		                this.wallClockLimit = Integer.valueOf(val);
		                break;
	            }
            }
        }
        // Add the information retrieved from the processor constraints
        this.processors.add(proc);
    }

    public MethodResourceDescription(MethodResourceDescription clone) {
        super(clone);
        
        this.processors = new LinkedList<Processor>();
        this.totalComputingUnits = 0;
        for (Processor p : clone.processors) {
            this.processors.add(p);
            this.totalComputingUnits += p.getComputingUnits();
        }
        
        this.memorySize = clone.memorySize;
        this.memoryType = clone.memoryType;
        
        this.storageSize = clone.storageSize;
        this.storageType = clone.storageType;
        
        this.operatingSystemType = clone.operatingSystemType;
        this.operatingSystemDistribution = clone.operatingSystemDistribution;
        this.operatingSystemVersion = clone.operatingSystemVersion;
        
        this.appSoftware = new LinkedList<String>();
        for (String app : clone.appSoftware) {
            this.appSoftware.add(app);
        }
        
        this.hostQueues = new LinkedList<String>();
        for (String queue : clone.hostQueues) {
            this.hostQueues.add(queue);
        }
        
        this.priceTimeUnit = clone.priceTimeUnit;
        this.pricePerUnit = clone.pricePerUnit;
        
        this.wallClockLimit = clone.wallClockLimit;
        
        this.slots = clone.slots;
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
		if (speed > (float)0.0) {
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
		
		for (Processor p: this.processors) {
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
		if (appSoftware != null && !appSoftware.isEmpty()) {
			this.appSoftware = appSoftware;
		}
	}
	
	public void resetAppSoftware() {
        appSoftware = new LinkedList<String>();
    }

    public void addApplication(String application) {
    	if (application != null && !application.isEmpty()) {
    		appSoftware.add(application);
    	}
    }
    
    public List<String> getHostQueues() {
		return hostQueues;
	}

	public void setHostQueues(List<String> hostQueues) {
		if (hostQueues != null && !hostQueues.isEmpty()) {
			this.hostQueues = hostQueues;
		}
	}
	
	public void resethostQueues() {
		hostQueues = new LinkedList<String>();
    }

    public void addQueue(String queue) {
    	if (queue != null && !queue.isEmpty()) {
    		hostQueues.add(queue);
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
		if (priceTimeUnit > (float)0.0) {
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

	public int getSlots() {
		return slots;
	}

	public void setSlots(int slots) {
		this.slots = slots;
	}
	
	public void addSlot() {
	    this.slots++;
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
    public void join(MethodResourceDescription mr2) {    	
        // Processor
    	for (Processor p : mr2.processors) {
    		boolean processorMerged = false;
    		for (Processor pthis : this.processors) {
    			if (p.getArchitecture().equals(pthis.getArchitecture())
    					&& p.getSpeed() == pthis.getSpeed()) {
    				processorMerged = true;
    				pthis.addComputingUnits(p.getComputingUnits());
    				this.totalComputingUnits += p.getComputingUnits();
    				break;
    			}
    		}
    		if (!processorMerged) {
    			this.processors.add(p);
    			this.totalComputingUnits += p.getComputingUnits();
    		}
    	}
    	
    	// Memory
    	this.memorySize = Math.max(this.memorySize, mr2.memorySize);
    	if (this.memoryType.equals(UNASSIGNED_STR)) {
    		if (mr2.memoryType != null && !mr2.memoryType.isEmpty()) {
    			this.memoryType = mr2.memoryType;
    		}
    	}
    	
    	// Storage
    	this.storageSize = Math.max(this.storageSize, mr2.storageSize);
    	if (this.storageType.equals(UNASSIGNED_STR)) {
    		if (mr2.storageType != null && !mr2.storageType.isEmpty()) {
    			this.storageType = mr2.storageType;
    		}
    	}
    	
    	// OperatingSystem
    	if (this.operatingSystemType.equals(UNASSIGNED_STR)) {
    		if (mr2.operatingSystemType != null && !mr2.operatingSystemType.isEmpty()) {
    			this.operatingSystemType = mr2.operatingSystemType;
    		}
    	}
    	if (this.operatingSystemDistribution.equals(UNASSIGNED_STR)) {
    		if (mr2.operatingSystemDistribution != null && !mr2.operatingSystemDistribution.isEmpty()) {
    			this.operatingSystemDistribution = mr2.operatingSystemDistribution;
    		}
    	}
    	if (this.operatingSystemVersion.equals(UNASSIGNED_STR)) {
    		if (mr2.memoryType != null && !mr2.memoryType.isEmpty()) {
    			this.operatingSystemVersion = mr2.operatingSystemVersion;
    		}
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
    		if (mr2.pricePerUnit > (float)0.0) {
    			this.pricePerUnit = mr2.pricePerUnit;
    		}
    	}
    	if (this.priceTimeUnit == UNASSIGNED_INT) {
    		if (mr2.priceTimeUnit > 0) {
    			this.priceTimeUnit = mr2.priceTimeUnit;
    		}
    	}
    	
    	// WallClock limit
    	this.wallClockLimit = java.lang.Math.max(this.wallClockLimit, mr2.wallClockLimit);
    	
    	// Internal values
        this.slots += mr2.slots;
    }

    public float difference(MethodResourceDescription mr2) {
        int weight = 10000;
        float processorDif = this.getTotalComputingUnits() - mr2.getTotalComputingUnits();
        float memoryDif = this.memorySize - mr2.memorySize;
        
        return processorDif*weight + memoryDif;
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
    
    public boolean dynamicContains(MethodResourceDescription rc2) {
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
    	for (Processor p_this : this.processors) {
    		if (checkCompatibility(p_this.getArchitecture(),p.getArchitecture())
    			 && checkInclusion(p_this.getSpeed(), p.getSpeed())
    			 && checkInclusion(p_this.getComputingUnits(), p.getComputingUnits())) {
    			
    			return true;
    		}
    	}
    	
    	return false;
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
        return value1 >= value2 || value1 == UNASSIGNED_INT || value2 == UNASSIGNED_INT;
    }

    private boolean checkInclusion(float value1, float value2) {
        return value1 >= value2 || value1 == UNASSIGNED_FLOAT || value2 == UNASSIGNED_FLOAT;
    }

    public Integer canHostSimultaneously(MethodResourceDescription rc2) {
        float min = Float.MAX_VALUE;
        
        if (rc2.getTotalComputingUnits() != 0) {
            float ratio = this.getTotalComputingUnits() / (float) rc2.getTotalComputingUnits();
            min = ratio;
        }
        
        if (rc2.memorySize != 0.0f) {
            float ratio = this.memorySize / rc2.memorySize;
            min = Math.min(min, ratio);
        }

        return (int) min;
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
    
    @Override
	public void increase(ResourceDescription rd) {
    	MethodResourceDescription mrd = (MethodResourceDescription) rd;
    	for (Processor p : mrd.processors) {
    		increaseProcessor(p);
    	}
        
        this.memorySize += mrd.memorySize;
        this.storageSize += mrd.storageSize;
    }
    
    private void increaseProcessor(Processor p) {
    	for (Processor p_this : this.processors) {
    		if (checkCompatibility(p_this.getArchitecture(),p.getArchitecture())
       			 && checkInclusion(p_this.getSpeed(), p.getSpeed())) {
    			
    			p_this.addComputingUnits(p.getComputingUnits());
    			this.totalComputingUnits += p.getComputingUnits();
    			return;
    		}
    	}
    }

    @Override
	public void reduce(ResourceDescription rd) {
    	MethodResourceDescription mrd = (MethodResourceDescription) rd;
    	for (Processor p : mrd.processors) {
    		decreaseProcessor(p);
    	}
        
        this.memorySize -= mrd.memorySize;
        this.storageSize -= mrd.storageSize;
    }
    
    private void decreaseProcessor(Processor p) {
    	for (Processor p_this : this.processors) {
    		if (checkCompatibility(p_this.getArchitecture(),p.getArchitecture())
       			 && checkInclusion(p_this.getSpeed(), p.getSpeed())) {
    			
    			p_this.removeComputingUnits(p.getComputingUnits());
    			this.totalComputingUnits -= p.getComputingUnits();
    			return;
    		}
    	}
    }

    @Override
    public void increaseDynamic(ResourceDescription resources) {
    	increase(resources);
    }

    @Override
    public void reduceDynamic(ResourceDescription resources) {
        reduce(resources);
    }

    @Override
    public ResourceDescription getDynamicCommons(ResourceDescription other) {
        MethodResourceDescription otherMRD = (MethodResourceDescription) other;
        MethodResourceDescription common = new MethodResourceDescription();
        
        // Processor
        for (Processor p : otherMRD.getProcessors()) {
        	if (checkProcessor(p)) {
        		common.addProcessor(p);
        	}
        }
        // Memory
        common.setMemorySize(Math.min(this.memorySize, otherMRD.getMemorySize()));
        
        return common;
    }

    @Override
    public boolean isDynamicUseless() {
        return (this.getMemorySize() == 0.0 && this.totalComputingUnits == 0);
    }

    @Override
    public boolean canHost(Implementation<?> impl) {
        if (impl.getType() == Type.METHOD) {
            MethodResourceDescription wd = (MethodResourceDescription) impl.getRequirements();
            return this.contains(wd);
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
