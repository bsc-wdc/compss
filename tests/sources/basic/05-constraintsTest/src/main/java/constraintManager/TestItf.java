package constraintManager;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.task.Method;


public interface TestItf {
	
	/* ********************************************
	 * EMPTY CORE-ELEMENTS
	 * *******************************************/
	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints()
	void emptyCoreElement0();

	@Method(declaringClass = "constraintManager.Implementation1")
	void emptyCoreElement1();

	
	/* ********************************************
	 * SIMPLE CONSTRAINTS CORE-ELEMENTS
	 * *******************************************/
	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(computingUnits = "1")
	void simpleCoreElement0();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(processorName = "Main")
	void simpleCoreElement1();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(processorSpeed = "2.4")
	void simpleCoreElement2();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(processorArchitecture = "amd64")
	void simpleCoreElement3();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(processorPropertyName = "ThreadAffinity", processorPropertyValue = "Big")
	void simpleCoreElement4();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(memorySize = "8.0")
	void simpleCoreElement5();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(memoryType = "Volatile")
	void simpleCoreElement6();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(storageSize = "240.0")
	void simpleCoreElement7();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(storageType = "SSD")
	void simpleCoreElement8();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(operatingSystemType = "Linux")
	void simpleCoreElement9();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(operatingSystemDistribution = "OpenSUSE")
	void simpleCoreElement10();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(operatingSystemVersion = "13.2")
	void simpleCoreElement11();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(appSoftware = "JAVA")
	void simpleCoreElement12();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(appSoftware = "JAVA, PYTHON, COMPSS")
	void simpleCoreElement13();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(hostQueues = "sequential")
	void simpleCoreElement14();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(hostQueues = "sequential, debug, bsc")
	void simpleCoreElement15();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(wallClockLimit = "10")
	void simpleCoreElement16();
	
	
	/* ********************************************
	 * COMPLEX CONSTRAINTS CORE-ELEMENTS
	 * *******************************************/
	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(computingUnits = "2", processorArchitecture="amd64", 
					memorySize="8.0", storageSize="120.0", 
					operatingSystemType="Windows")
	void complexCoreElement0();
	
	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(   computingUnits = "2", 
				  	processorName="Main", processorArchitecture="amd64", processorSpeed="2.4", 
				  	processorPropertyName="ThreadAffinity", processorPropertyValue="Big",
					memorySize="8.0", memoryType="Volatile",
					storageSize="120.0", storageType="SSD", 
					operatingSystemType="Windows", operatingSystemDistribution="XP", operatingSystemVersion="SP2",
					appSoftware="JAVA, PYTHON",
					hostQueues="sequential, debug",
					wallClockLimit="10"
				)
	void complexCoreElement1();
	
	
	   /* ********************************************
     * MULTI-CONSTRAINTS CORE-ELEMENTS
     * *******************************************/
    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints(computingUnits = "4"))
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints())
    @Constraints(computingUnits = "2")
    void multiCoreElement0();
    
    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints())
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints(processorName = "Slave"))
    @Constraints(processorName = "Main")
    void multiCoreElement1();

    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints(processorSpeed = "3.0"))
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints())
    @Constraints(processorSpeed = "2.4")
    void multiCoreElement2();

    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints(processorArchitecture = "x86"))
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints())
    @Constraints(processorArchitecture = "amd64")
    void multiCoreElement3();

    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints(processorPropertyName = "ThreadAffinity", processorPropertyValue = "Little"))
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints())
    @Constraints(processorPropertyName = "ThreadAffinity", processorPropertyValue = "Big")
    void multiCoreElement4();

    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints())
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints(memorySize = "4.0"))
    @Constraints(memorySize = "8.0")
    void multiCoreElement5();

    
    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints(memoryType = "Non-Volatile"))
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints())
    @Constraints(memoryType = "Volatile")
    void multiCoreElement6();

    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints(storageSize = "300.0"))
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints())
    @Constraints(storageSize = "240.0")
    void multiCoreElement7();

    
    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints(storageType = "HDD"))
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints())
    @Constraints(storageType = "SSD")
    void multiCoreElement8();

    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints())
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints(operatingSystemType = "Windows"))
    @Constraints(operatingSystemType = "Linux")
    void multiCoreElement9();

    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints(operatingSystemDistribution = "XP"))
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints())
    @Constraints(operatingSystemDistribution = "OpenSUSE")
    void multiCoreElement10();

    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints(operatingSystemVersion = "SP2"))
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints())
    @Constraints(operatingSystemVersion = "13.2")
    void multiCoreElement11();

    
    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints())
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints(appSoftware = "PYTHON"))
    @Constraints(appSoftware = "JAVA")
    void multiCoreElement12();

    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints(appSoftware = "PYTHON, COMPSS"))
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints(appSoftware = "COMPSS"))
    @Constraints(appSoftware = "JAVA, PYTHON")
    void multiCoreElement13();

    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints(hostQueues = "debug"))
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints())
    @Constraints(hostQueues = "sequential")
    void multiCoreElement14();

    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints(hostQueues = "debug, bsc"))
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints(hostQueues = "bsc"))
    @Constraints(hostQueues = "sequential, debug")
    void multiCoreElement15();

    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints(wallClockLimit = "5"))
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints())
    @Constraints(wallClockLimit = "10")
    void multiCoreElement16();
    
    @Method(declaringClass = "constraintManager.Implementation1", 
            constraints = @Constraints(computingUnits = "4", memorySize = "4.0" ))
    @Method(declaringClass = "constraintManager.Implementation2", 
            constraints = @Constraints(computingUnits = "8", storageSize = "300.0", operatingSystemType="Windows"))
                // modified both        not modified                        modified 1              modified 2
    @Constraints(computingUnits = "2", processorArchitecture = "amd64", memorySize = "8.0", storageSize = "240.0")
    void multiCoreElement17();
	
}
