package testMPI;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.MPI;


public interface MainItf {
	
	@MPI(binary = "${VEC_SUM_MPI_BINARY}", mpiRunner = "mpirun", computingNodes = "1")
	@Constraints(computingUnits = "2")
	int taskSingleMPI(
		@Parameter(type = Type.OBJECT, direction = Direction.IN) int[] data,
		@Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String fileOut
	);
	
	@MPI(binary = "${VEC_SUM_MPI_BINARY}", mpiRunner = "mpirun", computingNodes = "2")
    @Constraints(computingUnits = "2")
    Integer taskMultipleMPI(
        @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] data,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String fileOut
    );
	
	@MPI(binary = "${VEC_SUM_MPI_BINARY}", mpiRunner = "mpirun", computingNodes = "2")
	@Constraints(computingUnits = "1")
	Integer taskConcurrentMultipleMPI(
	    @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] data,
	    @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String fileOut
	);
	
}
