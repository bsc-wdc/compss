package testNonExistingInFile;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.task.Method;


public interface MainItf {
	@Method(declaringClass = "testNonExistingInFile.Main")
	Dummy errorTask(
		@Parameter(type = Type.FILE, direction = Direction.IN) String file
	);
}
