package java_api_calls;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


public interface MainItf {
	
	@Method(declaringClass = "java_api_calls.MainImpl")
	void increment(
		@Parameter(type = Type.FILE, direction = Direction.INOUT)
		String file
	);

}
