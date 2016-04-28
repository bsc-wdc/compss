package testNonExistingInFile;

import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;


public interface MainItf {
	@Method(declaringClass = "testNonExistingInFile.Main")
	Dummy errorTask(
		@Parameter(type = Type.FILE, direction = Direction.IN) String file
	);
}
