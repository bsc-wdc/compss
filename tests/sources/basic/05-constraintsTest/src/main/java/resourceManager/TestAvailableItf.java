package resourceManager;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;
import integratedtoolkit.types.annotations.task.Method;


public interface TestAvailableItf {
	
	@Method(declaringClass = "resourceManager.Implementation1")
	@Constraints(computingUnits = "2")
	void coreElement1(
		@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName
	);
	
	@Method(declaringClass = "resourceManager.Implementation1")
	@Constraints(memorySize = "2.0")
	void coreElement2(
		@Parameter(type = Type.FILE, direction = Direction.IN) String fileName
	);
	
}
