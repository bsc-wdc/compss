package testMPI;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Method;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;


public interface MainItf {
	
	@Method(declaringClass = "testMPI.MainImpl", computingNodes = "1")
	@Constraints(computingUnits = 2)
	public int taskSingleMPI(
		@Parameter(type = Type.STRING, direction = Direction.IN) String binary,
		@Parameter(type = Type.OBJECT, direction = Direction.IN) int[] data
	);
	
	@Method(declaringClass = "testMPI.MainImpl", computingNodes = "2")
	@Constraints(computingUnits = 2)
	public int taskMultipleMPI(
		@Parameter(type = Type.STRING, direction = Direction.IN) String binary,
		@Parameter(type = Type.OBJECT, direction = Direction.IN) int[] data
	);

}
