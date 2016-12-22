package cbm2.files;

import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.task.Method;


public interface Cbm2Itf {
	@Method(declaringClass = "cbm2.files.Cbm2Impl")
	void runTaskIn //B = f(A,B)
	(
		@Parameter(type = Type.INT, direction = Direction.IN)   int sleepTime,
		@Parameter(type = Type.FILE, direction = Direction.IN)  String dummyFilePath,
		@Parameter(type = Type.FILE, direction = Direction.OUT) String dummyFilePathOut
	);

	@Method(declaringClass = "cbm2.files.Cbm2Impl")
	void runTaskInOut //f(A,B), where B is inout
	(
		@Parameter(type = Type.INT, direction = Direction.IN)     int sleepTime,
		@Parameter(type = Type.FILE, direction = Direction.INOUT) String dummyFilePath
	);
	
}