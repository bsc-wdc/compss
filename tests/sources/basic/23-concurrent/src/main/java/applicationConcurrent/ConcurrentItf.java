package applicationConcurrent;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.task.Method;


public interface ConcurrentItf {
	
	@Method(declaringClass = "applicationConcurrent.ConcurrentImpl")
	void increment(
		@Parameter(type = Type.FILE, direction = Direction.INOUT)
		String file
	);

}
