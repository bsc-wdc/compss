package testReschedulingFail;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


public interface MainItf {
	
	@Method(declaringClass = "testReschedulingFail.Main")
	Dummy errorTask(
		@Parameter(type = Type.INT, direction = Direction.IN) int x,
		@Parameter(type = Type.OBJECT, direction = Direction.IN) Dummy din
	);
	
}
