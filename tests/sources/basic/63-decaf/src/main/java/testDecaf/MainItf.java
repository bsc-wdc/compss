package testDecaf;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Stream;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.task.Decaf;


public interface MainItf {
	
	@Decaf( dfScript= "test.py", mpiRunner = "mpirun", computingNodes = "1")
	@Constraints(computingUnits = "2")
	int taskSingleDecaf(
		@Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String fileOut
	);
	
	@Decaf(dfScript = "test.py", mpiRunner = "mpirun", computingNodes = "2")
    	@Constraints(computingUnits = "2")
    	Integer taskMultipleDecaf(
        	@Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String fileOut
    	);
	
	@Decaf(dfScript = "test.py", mpiRunner = "mpirun", computingNodes = "2")
	@Constraints(computingUnits = "1")
	Integer taskConcurrentMultipleDecaf(
	    @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String fileOut
	);
	
}
