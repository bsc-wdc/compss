package cloudManager;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.MultiConstraints;


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
	@Constraints(computingUnits = 1)
	void simpleCoreElement0();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(processorName = "Main")
	void simpleCoreElement1();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(processorSpeed = (float)2.4)
	void simpleCoreElement2();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(processorArchitecture = "amd64")
	void simpleCoreElement3();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(processorPropertyName = "ThreadAffinity", processorPropertyValue = "Big")
	void simpleCoreElement4();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(memorySize = (float)8.0)
	void simpleCoreElement5();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(memoryType = "Volatile")
	void simpleCoreElement6();

	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(storageSize = (float)240.0)
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
	@Constraints(wallClockLimit = (int)10)
	void simpleCoreElement16();
	
	
	/* ********************************************
	 * COMPLEX CONSTRAINTS CORE-ELEMENTS
	 * *******************************************/
	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(computingUnits = 2, processorArchitecture="amd64", 
					memorySize=(float)8.0, storageSize=(float)120.0, 
					operatingSystemType="Windows")
	void complexCoreElement0();
	
	@Method(declaringClass = "constraintManager.Implementation1")
	@Constraints(   computingUnits = 2, 
				  	processorName="Main", processorArchitecture="amd64", processorSpeed=(float)2.4, 
				  	processorPropertyName="ThreadAffinity", processorPropertyValue="Big",
					memorySize=(float)8.0, memoryType="Volatile",
					storageSize=(float)120.0, storageType="SSD", 
					operatingSystemType="Windows", operatingSystemDistribution="XP", operatingSystemVersion="SP2",
					appSoftware="JAVA, PYTHON",
					hostQueues="sequential, debug",
					wallClockLimit=(int)10
				)
	void complexCoreElement1();
	
	
	/* ********************************************
	 * MULTI-CONSTRAINTS CORE-ELEMENTS
	 * *******************************************/
	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)
	@Constraints(computingUnits = 2)
	@MultiConstraints({
		@Constraints(computingUnits = 4), 
		@Constraints()
	})
	void multiCoreElement0();

	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)
	@Constraints(processorName = "Main")
	@MultiConstraints({
		@Constraints(), 
		@Constraints(processorName = "Slave")
	})
	void multiCoreElement1();

	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)
	@Constraints(processorSpeed = (float)2.4)
	@MultiConstraints({
		@Constraints(processorSpeed = (float)3.0), 
		@Constraints()
	})
	void multiCoreElement2();

	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)
	@Constraints(processorArchitecture = "amd64")
	@MultiConstraints({
		@Constraints(processorArchitecture = "x86"), 
		@Constraints()
	})
	void multiCoreElement3();

	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)
	@Constraints(processorPropertyName = "ThreadAffinity", processorPropertyValue = "Big")
	@MultiConstraints({
		@Constraints(processorPropertyName = "ThreadAffinity", processorPropertyValue = "Little"), 
		@Constraints()
	})
	void multiCoreElement4();

	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)
	@Constraints(memorySize = (float)8.0)
	@MultiConstraints({
		@Constraints(), 
		@Constraints(memorySize = (float)4.0)
	})
	void multiCoreElement5();

	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)
	@Constraints(memoryType = "Volatile")
	@MultiConstraints({
		@Constraints(memoryType = "Non-Volatile"), 
		@Constraints()
	})
	void multiCoreElement6();

	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)
	@Constraints(storageSize = (float)240.0)
	@MultiConstraints({
		@Constraints(storageSize = (float)300.0), 
		@Constraints()
	})
	void multiCoreElement7();

	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)
	@Constraints(storageType = "SSD")
	@MultiConstraints({
		@Constraints(storageType = "HDD"), 
		@Constraints()
	})
	void multiCoreElement8();

	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)
	@Constraints(operatingSystemType = "Linux")
	@MultiConstraints({
		@Constraints(), 
		@Constraints(operatingSystemType = "Windows")
	})
	void multiCoreElement9();

	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)
	@Constraints(operatingSystemDistribution = "OpenSUSE")
	@MultiConstraints({
		@Constraints(operatingSystemDistribution = "XP"), 
		@Constraints()
	})
	void multiCoreElement10();

	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)
	@Constraints(operatingSystemVersion = "13.2")
	@MultiConstraints({
		@Constraints(operatingSystemVersion = "SP2"), 
		@Constraints()
	})
	void multiCoreElement11();

	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)
	@Constraints(appSoftware = "JAVA")
	@MultiConstraints({
		@Constraints(), 
		@Constraints(appSoftware = "PYTHON")
	})
	void multiCoreElement12();

	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)
	@Constraints(appSoftware = "JAVA, PYTHON")
	@MultiConstraints({
		@Constraints(appSoftware = "PYTHON, COMPSS"), 
		@Constraints(appSoftware = "COMPSS")
	})
	void multiCoreElement13();

	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)
	@Constraints(hostQueues = "sequential")
	@MultiConstraints({
		@Constraints(hostQueues = "debug"), 
		@Constraints()
	})
	void multiCoreElement14();

	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)
	@Constraints(hostQueues = "sequential, debug")
	@MultiConstraints({
		@Constraints(hostQueues = "debug, bsc"), 
		@Constraints(hostQueues = "bsc")
	})
	void multiCoreElement15();

	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)
	@Constraints(wallClockLimit = (int)10)
	@MultiConstraints({
		@Constraints(wallClockLimit = (int)5), 
		@Constraints()
	})
	void multiCoreElement16();
	
	@Method(declaringClass = {
			"constraintManager.Implementation1",
			"constraintManager.Implementation2"}
	)			// modified both		not modified						modified 1				modified 2
	@Constraints(computingUnits = 2, processorArchitecture = "amd64", memorySize = (float)8.0, storageSize = (float) 240.0)
	@MultiConstraints({
		@Constraints(computingUnits = 4, memorySize = (float)4.0 ), 
		@Constraints(computingUnits = 8, storageSize = (float)300.0)
	})
	void multiCoreElement17();
	
}
