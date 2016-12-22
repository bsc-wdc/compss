package testNonExistingInFile;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.task.Method;


public interface MainItf {
	@Method(declaringClass = "testNonExistingInFile.Main")
	Dummy errorTask(
		@Parameter(type = Type.FILE, direction = Direction.IN) String file
	);
}
