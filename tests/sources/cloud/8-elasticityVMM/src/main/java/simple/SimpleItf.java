package simple;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;
import integratedtoolkit.types.annotations.task.Method;


public interface SimpleItf {

	@Method(declaringClass = "simple.SimpleImpl")
	void increment(
		@Parameter(type = Type.FILE, direction = Direction.INOUT)
		String file
	);
	
}
