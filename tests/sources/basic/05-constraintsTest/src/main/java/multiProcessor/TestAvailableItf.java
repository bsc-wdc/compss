package multiProcessor;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;
import integratedtoolkit.types.annotations.Processor;
import integratedtoolkit.types.annotations.task.Method;


public interface TestAvailableItf {
	
	@Method(declaringClass = "resourceManager.Implementation1")
	@Constraints(processors={@Processor(type = "GPU", computingUnits = "2", internalMemorySize = "1"), 
	                         @Processor(type = "FPGA", computingUnits = "1")}, computingUnits = "2")
	void coreElement1(
		@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName
	);
	
	@Method(declaringClass = "resourceManager.Implementation1")
	@Constraints(processors={@Processor(type = "other", computingUnits = "2"), 
	                         @Processor(type = "FPGA", computingUnits = "1")}, memorySize = "2.0")
	void coreElement2(
		@Parameter(type = Type.FILE, direction = Direction.IN) String fileName
	);
	
	@Method(declaringClass = "resourceManager.Implementation1")
	@Constraints(processors={@Processor(type = "GPU", computingUnits = "2", internalMemorySize = "3")})
	void coreElement3(
		@Parameter(type = Type.FILE, direction = Direction.IN) String fileName
	);
	
	@Method(declaringClass = "resourceManager.Implementation1")
	@Constraints(processors={@Processor(type = "other", computingUnits = "2"), 
	                         @Processor(type = "FPGA", computingUnits = "1")}, processorInternalMemorySize = "3")
	void coreElement4(
		@Parameter(type = Type.FILE, direction = Direction.IN) String fileName
	);
	
}
