package testDecaf;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Decaf;


public interface MainItf {
	
	@Decaf( dfScript= "$PWD/decaf/test.py", dfExecutor="test.sh", mpiRunner = "mpirun", computingNodes = "1")
	@Constraints(computingUnits = "2")
	int taskSingleDecaf(
		@Parameter()String arg1,
		@Parameter()int arg2,
		@Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String fileOut
	);
	
	@Decaf(dfScript = "$PWD/decaf/test.py", dfExecutor="test.sh", mpiRunner = "mpirun", computingNodes = "2")
    	@Constraints(computingUnits = "2")
    	Integer taskMultipleDecaf(
    		@Parameter()String arg1,
    		@Parameter()int arg2,
        	@Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String fileOut
    	);
	
	@Decaf(dfScript = "$PWD/decaf/test.py", dfExecutor="test.sh", mpiRunner = "mpirun", computingNodes = "2")
	@Constraints(computingUnits = "1")
	Integer taskConcurrentMultipleDecaf(
		@Parameter()String arg1,
		@Parameter()int arg2,
	    @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String fileOut
	);
	
}
