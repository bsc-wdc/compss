package testAllBlocked;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;


public interface MainItf {
	
	// No worker will satisfy this constraint, thus no workers available
	@Constraints(computingUnits = "999")
	@Method(declaringClass = "testAllBlocked.Main")
	Dummy normalTask(
		@Parameter(type = Type.INT, direction = Direction.IN) int x,
		@Parameter(type = Type.OBJECT, direction = Direction.IN) Dummy din
	);
}
