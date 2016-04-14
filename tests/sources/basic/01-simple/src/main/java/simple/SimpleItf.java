package simple;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;


public interface SimpleItf {
	@Constraints(processorCPUCount = 1, memoryPhysicalSize = 0.3f)
	@Method(declaringClass = "simple.SimpleImpl")
	void increment(
		@Parameter(type = Type.FILE, direction = Direction.INOUT)
		String file
	);

}
