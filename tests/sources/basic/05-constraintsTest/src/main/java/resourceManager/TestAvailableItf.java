package resourceManager;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;


public interface TestAvailableItf {
	
	@Method(declaringClass = "resourceManager.Implementation1")
	@Constraints(computingUnits = 2, memorySize = (float)1.0)
	void coreElement1(
		@Parameter(type = Type.FILE, direction = Direction.OUT) String fileName
	);
	
	@Method(declaringClass = "resourceManager.Implementation1")
	@Constraints(computingUnits = 1, memorySize = (float)2.0)
	void coreElement2(
		@Parameter(type = Type.FILE, direction = Direction.IN) String fileName
	);
	
}
