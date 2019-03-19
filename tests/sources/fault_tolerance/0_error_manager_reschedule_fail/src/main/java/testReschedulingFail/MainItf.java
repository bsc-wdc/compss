package testReschedulingFail;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface MainItf {
	
	@Method(declaringClass = "testReschedulingFail.Main")
	Dummy errorTask(
		@Parameter(type = Type.INT, direction = Direction.IN) int x,
		@Parameter(type = Type.OBJECT, direction = Direction.IN) Dummy din
	);
	
}
